from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from collections import Counter
from database import get_db

router = APIRouter(
    prefix="/visualizations",
    tags=["visualizations"],
    responses={404: {"description": "Not found"}},
)

ALLOWED_TABLES = ['against', 'by']

@router.get("/wordcloud")
def wordcloud_data(
    table: str = Query(..., description="Table name"),
    column: str = Query(..., description="Column to analyze"),
    start_year: int = None,
    end_year: int = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    query_str = f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
    params = {}
    
    if start_year and end_year:
        query_str += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year
        
    rows = db.execute(text(query_str), params).fetchall()
    
    all_text = []
    for row in rows:
        if row[0]:
            # Split by common delimiters if needed, or just take the text
            # main.py logic: [phrase.strip().lower() for phrase in row[0].split(';') if phrase.strip()]
            all_text.extend([t.strip().lower() for t in row[0].split(';') if t.strip()])
            
    counter = Counter(all_text)
    most_common = counter.most_common(limit)
    
    return [{"text": word, "value": count} for word, count in most_common]

@router.get("/network")
def network_data(
    table: str = Query(..., description="Table name"),
    limit: int = 100,
    db: Session = Depends(get_db)
):
    # Simplified network data: Complainant -> Against
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    query_str = f"""
        SELECT Complainant, Against, ComplaintType
        FROM {table}
        WHERE Complainant IS NOT NULL AND Against IS NOT NULL
        LIMIT :limit
    """
    rows = db.execute(text(query_str), {"limit": limit}).mappings().all()
    
    nodes = set()
    links = []
    
    for row in rows:
        source = row["Complainant"]
        target = row["Against"]
        nodes.add(source)
        nodes.add(target)
        links.append({
            "source": source,
            "target": target,
            "type": row["ComplaintType"]
        })
        
    return {
        "nodes": [{"id": n} for n in nodes],
        "links": links
    }
