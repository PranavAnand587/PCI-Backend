from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from database import get_db

router = APIRouter(
    prefix="/locations",
    tags=["locations"],
    responses={404: {"description": "Not found"}},
)

ALLOWED_TABLES = ['against', 'by']

@router.get("/states")
def cases_per_state(
    start_year: int = None,
    end_year: int = None,
    table: str = Query(..., description="Table name: 'against' or 'by'"),
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    query_str = f"""
        SELECT State, COUNT(*) as case_count
        FROM {table}
        WHERE State IS NOT NULL
    """
    params = {}
    
    if start_year:
        query_str += " AND CAST(substr(ReportName, -4) AS INTEGER) >= :syear"
        params["syear"] = start_year
    if end_year:
        query_str += " AND CAST(substr(ReportName, -4) AS INTEGER) <= :eyear"
        params["eyear"] = end_year
        
    query_str += " GROUP BY State ORDER BY case_count DESC"
    
    rows = db.execute(text(query_str), params).mappings().all()
    return [{"state": row["State"], "count": row["case_count"]} for row in rows]
