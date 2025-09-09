from __future__ import annotations

import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


def _default_db_url() -> str:
    # Store sqlite DB under data/ by default
    p = Path("data") / "auth.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{p.as_posix()}"


DB_URL = os.getenv("DB_URL", _default_db_url())
ENGINE = create_engine(DB_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

