# api_dev/database.py  (replace your current file)
import logging
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("uvicorn.error")

# Resolve complaints.db relative to this file so the path is deterministic
HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "complaints.db"   # adjust if real file is in a different folder

# Make DB_URL absolute and POSIX-style (works on Windows with sqlite:///)
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

# Create engine
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Log what the app actually uses at import/startup
logger.info("Using SQLite DB at: %s", DB_PATH)
try:
    inspector = inspect(engine)
    logger.info("Tables available at startup: %s", inspector.get_table_names())
except Exception as e:
    logger.exception("Failed to inspect DB: %s", e)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
