from __future__ import annotations

import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from .db import ENGINE, SessionLocal, Base
from .models import User, SessionToken


def init_db() -> None:
    Base.metadata.create_all(bind=ENGINE)


def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000)
    return dk.hex()


def create_user(username: str, password: str, role: str = "AN", *, db: Optional[Session] = None) -> User:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        salt = secrets.token_hex(16)
        ph = _hash_password(password, salt)
        u = User(username=username, password_hash=ph, salt=salt, role=role)
        db.add(u)
        db.commit()
        db.refresh(u)
        return u
    finally:
        if close:
            db.close()


def authenticate(username: str, password: str, *, db: Optional[Session] = None) -> Optional[User]:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        u = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
        if not u:
            return None
        if _hash_password(password, u.salt) != u.password_hash:
            return None
        return u
    finally:
        if close:
            db.close()


def issue_token(user: User, ttl_hours: int = 12, *, db: Optional[Session] = None) -> SessionToken:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        tok = secrets.token_hex(20)
        sess = SessionToken(
            token=tok,
            user_id=user.id,
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=ttl_hours),
        )
        db.add(sess)
        db.commit()
        db.refresh(sess)
        return sess
    finally:
        if close:
            db.close()


def get_user_by_token(token: str, *, db: Optional[Session] = None) -> Optional[User]:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        sess = db.execute(select(SessionToken).where(SessionToken.token == token)).scalar_one_or_none()
        if not sess:
            return None
        if sess.expires_at and sess.expires_at < datetime.now(timezone.utc):
            # Expired: cleanup
            db.execute(delete(SessionToken).where(SessionToken.id == sess.id))
            db.commit()
            return None
        return db.execute(select(User).where(User.id == sess.user_id)).scalar_one()
    finally:
        if close:
            db.close()


def change_role(username: str, role: str, *, db: Optional[Session] = None) -> Optional[User]:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        u = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
        if not u:
            return None
        u.role = role
        db.add(u)
        db.commit()
        db.refresh(u)
        return u
    finally:
        if close:
            db.close()


def list_users(*, db: Optional[Session] = None) -> list[dict]:
    close = False
    if db is None:
        db = SessionLocal()
        close = True
    try:
        rows = db.execute(select(User)).scalars().all()
        return [{"username": r.username, "role": r.role, "created_at": r.created_at.isoformat()} for r in rows]
    finally:
        if close:
            db.close()

