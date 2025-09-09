from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

from .db import Base


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    salt = Column(String(64), nullable=False)
    role = Column(String(10), nullable=False, default="AN")  # SC, CREW, OM, DH, AN, ADM
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    sessions = relationship("SessionToken", back_populates="user", cascade="all, delete-orphan")


class SessionToken(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    token = Column(String(64), unique=True, nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="sessions")

