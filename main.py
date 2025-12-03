from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import complaints, locations, media, visualizations, research

app = FastAPI(title="PCI Complaints Analysis API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(complaints.router)
app.include_router(locations.router)
app.include_router(media.router)
app.include_router(visualizations.router)
app.include_router(research.router)

@app.get("/")
def root():
    return {"message": "Welcome to PCI Complaints Analysis API"}