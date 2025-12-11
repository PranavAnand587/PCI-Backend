from fastapi import APIRouter, HTTPException, Query, Response
from sqlalchemy import text
from database import engine
import pandas as pd
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from rapidfuzz import process, fuzz
import io
from collections import Counter
import matplotlib.cm as cm
from pathlib import Path

router = APIRouter(
    prefix="/research",
    tags=["Research Findings"],
    responses={404: {"description": "Not found"}},
)

ALLOWED_TABLES = ['against', 'by']
ALLOWED_GROUP_COLS = ["res_res_ComplaintType", "State", "Decision", "c_aff_resolved", "a_aff_resolved"]

# Load India GeoJSON once
# Assuming india_states.geojson is in the parent directory of routers (i.e., api_dev)
GEOJSON_PATH = Path(__file__).resolve().parent.parent / "india_states.geojson"
try:
    india = gpd.read_file(GEOJSON_PATH)
    india_states = india['NAME_1'].tolist()
except Exception as e:
    print(f"Error loading GeoJSON: {e}")
    india = None
    india_states = []

# === Fuzzy Match Function ===
def match_state(state_name, choices, threshold=90):
    match, score, _ = process.extractOne(state_name, choices, scorer=fuzz.token_sort_ratio)
    return match if score >= threshold else None

@router.get("/cases_per_state_year")
def query_data(state: str = None, start_year: int = None, end_year: int = None, table: str = Query(..., description="Table name: 'against' or 'by'")):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    # Build SQL query dynamically
    query = f"SELECT * FROM {table} WHERE 1=1"
    params = {}
    if state:
        query += " AND State = :state"
        params["state"] = state
    
    if start_year:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) >= :syear"
        params["syear"] = start_year

    if end_year:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) <= :eyear"
        params["eyear"] = end_year
    
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    return {"data": [dict(row) for row in rows]}

@router.get("/cases_per_state")
def cases_per_state(start_year: int, end_year: int, table: str = Query(..., description="Table name: 'against' or 'by'")):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
    query = f"""
        SELECT State, COUNT(*) as case_count
        FROM {table}
        WHERE CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear
        GROUP BY State
        ORDER BY case_count DESC
    """
    params = {"syear": start_year, "eyear": end_year}

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return [{"state": row["State"], "count": row["case_count"]} for row in rows]

@router.get("/wordcloud")
def get_wordcloud(start_year: int = None, end_year: int = None, table: str = Query(..., description="Table name: 'against' or 'by'"), column: str = "Complaint"):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    # Sanitize column name to prevent SQL injection (basic check)
    if not column.isidentifier():
         raise HTTPException(status_code=400, detail="Invalid column name")

    query = f"""
        SELECT {column}
        FROM {table}
        WHERE {column} IS NOT NULL
    """
    params = {}

    if start_year is not None and end_year is not None:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    # Flatten the phrases
    all_phrases = []
    for row in rows:
        if row[0]:
            all_phrases.extend(
                [phrase.strip().lower() for phrase in str(row[0]).split(';') if phrase.strip()]
            )

    if not all_phrases:
        # Return a blank image or error
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, "No data available", ha='center', va='center')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        plt.close()
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    # Count phrase frequencies
    phrase_freq = Counter(all_phrases)

    # Generate word cloud
    wordcloud = WordCloud(width=1200, height=600, background_color='white') \
        .generate_from_frequencies(phrase_freq)

    # Save to BytesIO
    img_bytes = io.BytesIO()
    plt.figure(figsize=(15, 7))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout(pad=0)
    plt.savefig(img_bytes, format='png')
    plt.close()

    img_bytes.seek(0)
    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/india_map")
def india_map(start_year: int = None, end_year: int = None, table: str = Query(..., description="Table name: 'against' or 'by'")):
    if india is None:
        raise HTTPException(status_code=500, detail="GeoJSON not loaded")
        
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    query = f"""
        SELECT State, COUNT(*) as count
        FROM {table}
        WHERE State IS NOT NULL
    """
    params = {}

    if start_year is not None and end_year is not None:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year

    query += " GROUP BY State"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    # Convert to dict: {state: count}
    a = {row[0]: row[1] for row in rows}

    # 2. Convert dict to DataFrame for merging
    if not a:
        # Return empty map
        fig, ax = plt.subplots(1, 1, figsize=(15, 15))
        india.plot(ax=ax, color='white', edgecolor='black')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png', bbox_inches='tight')
        plt.close(fig)
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    a_df = pd.DataFrame(list(a.items()), columns=["State", "count"])
    a_df.set_index("State", inplace=True)

    # 3. Fuzzy match with India states
    a_df['MatchedState'] = a_df.index.to_series().apply(lambda x: match_state(x, india_states))
    a_matched = a_df.dropna(subset=['MatchedState'])

    # 4. Merge with GeoJSON
    merged = india.merge(a_matched, left_on='NAME_1', right_on='MatchedState', how='left')
    merged['count'] = merged['count'].fillna(0)

    # 6. Plot map
    fig, ax = plt.subplots(1, 1, figsize=(15, 15))
    merged.plot(
        column='count',
        ax=ax,
        legend=True,
        cmap='YlOrRd',
        edgecolor='black',
        linewidth=0.5,
        missing_kwds={'color': 'lightgrey'}
    )

    # Add counts at centroids
    for idx, row in merged.iterrows():
        if row['count'] > 0:
            plt.annotate(
                text=f"{int(row['count'])}",
                xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                ha='center',
                fontsize=8,
                color='black'
            )

    plt.title(f"State-wise Heatmap ({table})", fontsize=18)
    plt.axis('off')
    plt.tight_layout()

    # 7. Save to BytesIO for FastAPI Response
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format='png', bbox_inches='tight')
    plt.close(fig)
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/stacked_histogram")
def stacked_histogram(
    table: str = Query(..., description="Table name: 'against' or 'by'"),
    start_year: int = None,
    end_year: int = None,
    column: str = "res_ComplaintType"
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    if not column.isidentifier():
         raise HTTPException(status_code=400, detail="Invalid column name")

    # Fetch data
    query = f"SELECT ReportName, {column} FROM {table} WHERE ReportName IS NOT NULL AND {column} IS NOT NULL"
    params = {}
    if start_year and end_year:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year

    df = pd.read_sql_query(text(query), engine, params=params)

    if df.empty:
        # Return empty image
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, "No data available", ha='center', va='center')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        plt.close()
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    # Create a pivot table: index=ReportName, columns=column, values=count
    pivot_df = df.groupby(['ReportName', column]).size().unstack(fill_value=0)

    # Plot stacked bar chart
    plt.figure(figsize=(14, 12))
    bottom = pd.Series([0]*len(pivot_df), index=pivot_df.index)

    for col in pivot_df.columns:
        plt.bar(pivot_df.index, pivot_df[col], bottom=bottom, label=col)
        bottom += pivot_df[col]

    plt.title(f'Number of Complaints by Report for Each {column}', fontsize=16)
    plt.xlabel('ReportName')
    plt.ylabel('Number of Complaints')
    plt.xticks(rotation=90)
    plt.legend(title=column, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save to BytesIO
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format='png')
    plt.close()
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/cdf_lineplot")
def cdf_lineplot(table: str = Query(...), start_year: int = None, end_year: int = None, column: str = "res_ComplaintType"):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    if not column.isidentifier():
         raise HTTPException(status_code=400, detail="Invalid column name")

    # Fetch ReportName (year) and res_ComplaintType
    query = f"SELECT ReportName, {column} FROM {table} WHERE ReportName IS NOT NULL AND {column} IS NOT NULL"
    params = {}
    if start_year and end_year:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year

    df = pd.read_sql_query(text(query), engine, params=params)
    if df.empty:
        # Return empty image
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, "No data available", ha='center', va='center')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        plt.close()
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    # Extract year from ReportName
    df['Year'] = df['ReportName'].str[-4:].astype(int)

    # Create a CDF per complaint type
    types = df[column].unique()
    plt.figure(figsize=(14, 8))
    for t in types:
        type_df = df[df[column] == t]
        yearly_counts = type_df.groupby('Year').size().sort_index()
        cdf = yearly_counts.cumsum() / yearly_counts.sum()
        plt.plot(cdf.index, cdf.values, marker='o', label=t)

    plt.title(f'Year-wise CDF of Complaints per {column}', fontsize=16)
    plt.xlabel('Year')
    plt.ylabel('Cumulative Fraction')
    plt.xticks(sorted(df['Year'].unique()))
    plt.ylim(0, 1.05)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title=column, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save to BytesIO
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format='png')
    plt.close()
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/freq_line_plot")
def freq_lineplot(
    table: str = Query(...),
    start_year: int = None,
    end_year: int = None,
    column: str = "res_ComplaintType"
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")
        
    if not column.isidentifier():
         raise HTTPException(status_code=400, detail="Invalid column name")

    # Fetch ReportName (year) and res_ComplaintType (or other column)
    query = f"SELECT ReportName, {column} FROM {table} WHERE ReportName IS NOT NULL AND {column} IS NOT NULL"
    params = {}
    if start_year and end_year:
        query += " AND CAST(substr(ReportName, -4) AS INTEGER) BETWEEN :syear AND :eyear"
        params["syear"] = start_year
        params["eyear"] = end_year
    df = pd.read_sql_query(text(query), engine, params=params)
    if df.empty:
        # Return empty image
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, "No data available", ha='center', va='center')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        plt.close()
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    # Extract year from ReportName
    df['Year'] = df['ReportName'].str[-4:].astype(int)

    # Unique categories
    types = df[column].unique()

    plt.figure(figsize=(14, 8))

    # Frequency plot
    for t in types:
        type_df = df[df[column] == t]
        yearly_counts = type_df.groupby('Year').size().sort_index()
        plt.plot(yearly_counts.index, yearly_counts.values, marker='o', label=t)

    plt.title(f'Year-wise Frequency of Complaints per {column}', fontsize=16)
    plt.xlabel('Year')
    plt.ylabel('Number of Complaints')
    plt.xticks(sorted(df['Year'].unique()))
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title=column, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save to BytesIO
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format='png')
    plt.close()
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/visualize_press")
def visualize_press(
    table: str = Query(...),
    chart_type: str = Query(..., regex="^(bar|bubble|wordcloud|line)$"),
    group_col: str = Query("res_ComplaintType"),
    top_k: int = Query(10, ge=1, le=50)
):
    # if group_col not in ALLOWED_GROUP_COLS:
    #     raise HTTPException(status_code=400, detail="Invalid group column")
    
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    # Fetch data
    # Note: Press column might not exist or might be named differently. 
    # In 'against' table, it's 'Against'. In 'by' table, it's 'Complainant' (who is Press).
    # The provided code used "Press". I need to adapt.
    
    press_col = "Against" if table == "against" else "Complainant"
    
    query = f"SELECT {press_col} as Press, {group_col}, ReportName FROM {table} WHERE {press_col} IS NOT NULL"
    try:
        df = pd.read_sql_query(text(query), engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found")

    # Top K Press Houses
    topk = df["Press"].value_counts().nlargest(top_k).index
    filtered = df[df["Press"].isin(topk)]

    # --- Plot ---
    plt.figure(figsize=(12,7))

    if chart_type == "bar":
        pivot = pd.crosstab(filtered["Press"], filtered[group_col])
        pivot.plot(kind="bar", stacked=True, figsize=(12,7))
        plt.title(f"Top {top_k} Media Houses grouped by {group_col}")
        plt.ylabel("Count")
        plt.xlabel("Media House")
        plt.xticks(rotation=45, ha="right")

    elif chart_type == "bubble":
        counts = filtered.groupby(["Press", group_col]).size().reset_index(name="Count")
        plt.scatter(counts[group_col], counts["Press"], s=counts["Count"]*10, alpha=0.6)
        plt.title(f"Top {top_k} Media Houses Bubble Plot by {group_col}")
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Press House")

    elif chart_type == "wordcloud":
        counts = filtered["Press"].value_counts().nlargest(top_k)
        wc = WordCloud(width=800, height=400, background_color="white").generate_from_frequencies(counts)
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.title(f"Top {top_k} Media Houses Word Cloud")

    elif chart_type == "line":
        if "Year" not in df.columns:
            df["Year"] = df["ReportName"].str[-4:].astype(int)  # Example extraction
            filtered["Year"] = filtered["ReportName"].str[-4:].astype(int)

        trend = filtered.groupby(["Year", "Press"]).size().reset_index(name="Count")
        for house in topk:
            sub = trend[trend["Press"] == house]
            plt.plot(sub["Year"], sub["Count"], marker="o", label=house)
        plt.title(f"Trend of Complaints for Top {top_k} Media Houses")
        plt.xlabel("Year")
        plt.ylabel("Complaints")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout()

    # Save to BytesIO
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png")
    plt.close()
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")

@router.get("/bubble_topk_press")
def bubble_topk_press(
    table: str = Query(...),
    state: str = Query(...),
    topk: int = Query(5, ge=1, le=20)
):
    # Validate
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name")

    press_col = "Against" if table == "against" else "Complainant"

    # Fetch data (assuming table has ReportName, Press, State columns)
    query = f"""
        SELECT ReportName, {press_col} as Press, State
        FROM {table}
        WHERE ReportName IS NOT NULL AND {press_col} IS NOT NULL AND State = :state
    """
    df = pd.read_sql_query(text(query), engine, params={"state": state})

    if df.empty:
        # Return empty image
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, f"No data for {state}", ha='center', va='center')
        plt.axis('off')
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        plt.close()
        img_bytes.seek(0)
        return Response(content=img_bytes.getvalue(), media_type="image/png")

    # Extract Year
    df["Year"] = df["ReportName"].str[-4:].astype(int)

    # Group by Year, Press
    grouped = df.groupby(["Year", "Press"]).size().reset_index(name="Complaints")

    # Keep only top-k per year
    topk_df = grouped.groupby("Year", group_keys=False).apply(
        lambda x: x.nlargest(topk, "Complaints")
    )

    # Assign unique colors to each press
    presses = topk_df["Press"].unique()
    # cmap = cm.get_cmap("tab20", len(presses)) # Deprecated
    cmap = cm.get_cmap("tab20")
    color_map = {ph: cmap(i % 20) for i, ph in enumerate(presses)}

    # Plot
    plt.figure(figsize=(16, 8))
    for ph in presses:
        subdf = topk_df[topk_df["Press"] == ph]
        plt.scatter(
            subdf["Year"],
            subdf["Complaints"],
            s=subdf["Complaints"] * 50,  # bubble size
            color=color_map[ph],
            alpha=0.7,
            label=ph
        )

    plt.legend(title="Press", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.title(f"Top {topk} Media Houses in {state} by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Complaints")
    plt.tight_layout()

    # Save image
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    plt.close()
    img_bytes.seek(0)

    return Response(content=img_bytes.getvalue(), media_type="image/png")
