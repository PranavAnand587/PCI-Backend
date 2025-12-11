from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd
from database import get_db

router = APIRouter(
    prefix="/media",
    tags=["media"],
    responses={404: {"description": "Not found"}},
)

ALLOWED_TABLES = ['against', 'by']
ALLOWED_GROUP_COLS = ["res_ComplaintType", "State", "level"] # Updated to match schema if needed, checking main.py it was res_ComplaintType but let's stick to what's likely in DB or alias it.
# main.py uses "ComplaintType" as default in visualize_press but checks against ["res_ComplaintType", "State", "level"]
# Let's assume the column name in DB is what we pass, or we map it.
# In main.py: query = f"SELECT Press, {group_col}, ReportName FROM {table} WHERE Press IS NOT NULL"
# So group_col must be a valid column.

@router.get("/top")
def top_media_houses(
    table: str = Query(..., description="Table name"),
    top_k: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    query_str = f"""
        SELECT Press, COUNT(*) as count
        FROM {table}
        WHERE Press IS NOT NULL
        GROUP BY Press
        ORDER BY count DESC
        LIMIT :limit
    """
    rows = db.execute(text(query_str), {"limit": top_k}).mappings().all()
    return [{"press": row["Press"], "count": row["count"]} for row in rows]

@router.get("/trends")
def media_trends(
    table: str = Query(..., description="Table name"),
    press_name: str = Query(..., description="Name of the media house"),
    db: Session = Depends(get_db)
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    query_str = f"""
        SELECT substr(ReportName, -4) as year, COUNT(*) as count
        FROM {table}
        WHERE Press = :press AND ReportName IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    rows = db.execute(text(query_str), {"press": press_name}).mappings().all()
    return [{"year": row["year"], "count": row["count"]} for row in rows]
