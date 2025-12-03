from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from database import get_db

router = APIRouter(
    prefix="/complaints",
    tags=["complaints"],
    responses={404: {"description": "Not found"}},
)

ALLOWED_TABLES = ['against', 'by']

@router.get("/list")
def list_complaints(
    state: str = None,
    start_year: int = None,
    end_year: int = None,
    table: str = Query(..., description="Table name: 'against' or 'by'"),
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    query_str = f"SELECT * FROM {table} WHERE 1=1"
    params = {}
    
    if state:
        query_str += " AND State = :state"
        params["state"] = state
    
    if start_year:
        query_str += " AND CAST(substr(ReportName, -4) AS INTEGER) >= :syear"
        params["syear"] = start_year

    if end_year:
        query_str += " AND CAST(substr(ReportName, -4) AS INTEGER) <= :eyear"
        params["eyear"] = end_year
    
    result = db.execute(text(query_str), params).mappings().all()
    return {"data": [dict(row) for row in result]}

@router.get("/stats")
def complaint_stats(
    start_year: int = None,
    end_year: int = None,
    table: str = Query(..., description="Table name: 'against' or 'by'"),
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    # Total count
    count_query = f"SELECT COUNT(*) as total FROM {table} WHERE 1=1"
    params = {}
    
    if start_year:
        count_query += " AND CAST(substr(ReportName, -4) AS INTEGER) >= :syear"
        params["syear"] = start_year
    if end_year:
        count_query += " AND CAST(substr(ReportName, -4) AS INTEGER) <= :eyear"
        params["eyear"] = end_year
        
    total = db.execute(text(count_query), params).scalar()
    
    # Yearly distribution
    year_query = f"""
        SELECT substr(ReportName, -4) as year, COUNT(*) as count 
        FROM {table} 
        WHERE ReportName IS NOT NULL
    """
    if start_year:
        year_query += " AND CAST(substr(ReportName, -4) AS INTEGER) >= :syear"
    if end_year:
        year_query += " AND CAST(substr(ReportName, -4) AS INTEGER) <= :eyear"
        
    year_query += " GROUP BY year ORDER BY year"
    
    yearly_data = db.execute(text(year_query), params).mappings().all()
    
    return {
        "total_complaints": total,
        "yearly_distribution": [dict(row) for row in yearly_data]
    }

@router.get("/filters")
def get_filters(db: Session = Depends(get_db)):
    """
    Get distinct values for all filters from both tables.
    """
    filters = {
        "years": set(),
        "states": set(),
        "complaint_types": set(),
        "affiliations": set(),
        "decisions": set()
    }

    for table in ALLOWED_TABLES:
        # Years (extracted from ReportName)
        years_query = f"SELECT DISTINCT substr(ReportName, -4) as year FROM {table} WHERE ReportName IS NOT NULL"
        years_res = db.execute(text(years_query)).scalars().all()
        for y in years_res:
            if y and y.isdigit():
                filters["years"].add(int(y))

        # States
        states_query = f"SELECT DISTINCT State FROM {table} WHERE State IS NOT NULL"
        states_res = db.execute(text(states_query)).scalars().all()
        filters["states"].update([s for s in states_res if s])

        # Complaint Types
        types_query = f"SELECT DISTINCT ComplaintType FROM {table} WHERE ComplaintType IS NOT NULL"
        types_res = db.execute(text(types_query)).scalars().all()
        filters["complaint_types"].update([t for t in types_res if t])

        # Decisions
        decisions_query = f"SELECT DISTINCT Decision FROM {table} WHERE Decision IS NOT NULL"
        decisions_res = db.execute(text(decisions_query)).scalars().all()
        filters["decisions"].update([d for d in decisions_res if d])

        # Affiliations (combine complainant and accused affiliations)
        c_aff_query = f"SELECT DISTINCT c_aff_resolved FROM {table} WHERE c_aff_resolved IS NOT NULL"
        a_aff_query = f"SELECT DISTINCT a_aff_resolved FROM {table} WHERE a_aff_resolved IS NOT NULL"
        
        c_aff_res = db.execute(text(c_aff_query)).scalars().all()
        a_aff_res = db.execute(text(a_aff_query)).scalars().all()
        
        filters["affiliations"].update([a for a in c_aff_res if a])
        filters["affiliations"].update([a for a in a_aff_res if a])

    return {
        "years": sorted(list(filters["years"]), reverse=True),
        "states": sorted(list(filters["states"])),
        "complaint_types": sorted(list(filters["complaint_types"])),
        "affiliations": sorted(list(filters["affiliations"])),
        "decisions": sorted(list(filters["decisions"]))
    }
