# trainer_app.py
# Club 24 - Multi-Club PT Management System
# Includes trainer logins, weekly KPI submission, document center,
# trainer calendar, director master calendar, client roster,
# action dashboard, lead pipeline, reactivation board, coaching log,
# and ABC calendar sync.

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import base64
import hashlib
import hmac
import io
import os
import secrets
from typing import Dict, Tuple, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from abc_calendar_sync import fetch_calendar_events, ABC_CLUBS

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Club 24 PT Dashboard",
    page_icon="🏋️",
    layout="wide",
)


# -------------------------------------------------
# CONSTANTS
# -------------------------------------------------
CLUBS = [
    "New Milford",
    "Brookfield",
    "Ridgefield",
    "Torrington",
    "Newtown",
    "Wallingford",
    "Middletown",
]

DIRECTOR_EMAILS = {
    "club24chase@gmail.com",
    "feedsatwork@gmail.com",
    "johnny.saumell@gmail.com",
}

ALLOWED_DOCUMENT_TYPES = {
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls": "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "ppt": "application/vnd.ms-powerpoint",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "csv": "text/csv",
    "txt": "text/plain",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
}

PACK_TYPES = [
    "1x per Week",
    "2x per Week",
    "8 Flex Pack",
    "12 Flex Pack",
    "24 Flex Pack",
]

PACK_WEIGHTS = {
    "1x per Week": 1.0,
    "2x per Week": 1.5,
    "8 Flex Pack": 1.25,
    "12 Flex Pack": 2.0,
    "24 Flex Pack": 3.0,
}

CLIENT_STATUS_OPTIONS = [
    "Active",
    "At Risk",
    "Expired",
    "Reactivation",
    "Lost",
]

LEAD_STAGE_OPTIONS = [
    "New Lead",
    "Contacted",
    "Consult Scheduled",
    "Kickoff Booked",
    "Kickoff Completed",
    "Pack Sold",
    "Active Client",
    "Lost",
    "Reactivation",
]

NOTE_TYPE_OPTIONS = [
    "Coaching",
    "Warning",
    "Praise",
    "Follow-up",
]

DOCUMENT_CATEGORY_OPTIONS = [
    "SOP",
    "Sales Script",
    "PT Education",
    "Cleaning",
    "HR",
    "Promotions",
    "Other",
]

DEFAULT_SCORING = {
    "target_hours": 25,
    "target_booked": 8,
    "target_completed": 6,
    "target_pt_sold": 20,
    "weight_hours": 15,
    "weight_booked": 20,
    "weight_completed": 20,
    "weight_pt_sold": 25,
    "weight_booked_to_completed": 10,
    "weight_completed_to_sold": 10,
}

PBKDF2_ITERATIONS = 200_000
EST = ZoneInfo("America/New_York")


# -------------------------------------------------
# CONFIG HELPERS
# -------------------------------------------------
def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        try:
            database_url = st.secrets["DATABASE_URL"]
        except Exception:
            database_url = ""
    return database_url.strip()


def get_director_master_password() -> str:
    master_password = os.getenv("DIRECTOR_MASTER_PASSWORD", "")
    if not master_password:
        try:
            master_password = st.secrets["DIRECTOR_MASTER_PASSWORD"]
        except Exception:
            master_password = ""
    return str(master_password).strip()


DATABASE_URL = get_database_url()
DIRECTOR_MASTER_PASSWORD = get_director_master_password()


# -------------------------------------------------
# PASSWORD HELPERS
# -------------------------------------------------
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        PBKDF2_ITERATIONS,
    )
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${derived_key.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations, salt, stored_key = stored_hash.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        derived_key = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt),
            int(iterations),
        ).hex()
        return hmac.compare_digest(derived_key, stored_key)
    except Exception:
        return False


# -------------------------------------------------
# DATABASE
# -------------------------------------------------
@st.cache_resource
def get_engine():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set. Add it as an environment variable or Streamlit secret.")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    id SERIAL PRIMARY KEY,
                    week_start DATE NOT NULL,
                    trainer_email VARCHAR(255),
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    hours_worked NUMERIC(10,2) NOT NULL,
                    kickoffs_booked INTEGER NOT NULL,
                    kickoffs_completed INTEGER NOT NULL,
                    pt_sold NUMERIC(12,2) NOT NULL,
                    weighted_pack_points NUMERIC(12,2) NOT NULL DEFAULT 0,
                    sessions_scheduled INTEGER NOT NULL DEFAULT 0,
                    sessions_completed INTEGER NOT NULL DEFAULT 0,
                    no_shows INTEGER NOT NULL DEFAULT 0,
                    cancellations INTEGER NOT NULL DEFAULT 0,
                    reschedules INTEGER NOT NULL DEFAULT 0,
                    pack_1x_per_week INTEGER NOT NULL DEFAULT 0,
                    pack_2x_per_week INTEGER NOT NULL DEFAULT 0,
                    pack_8_flex INTEGER NOT NULL DEFAULT 0,
                    pack_12_flex INTEGER NOT NULL DEFAULT 0,
                    pack_24_flex INTEGER NOT NULL DEFAULT 0,
                    submitted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        for alter_sql in [
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS trainer_email VARCHAR(255)",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS weighted_pack_points NUMERIC(12,2) NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS sessions_scheduled INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS sessions_completed INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS no_shows INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS cancellations INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS reschedules INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS pack_1x_per_week INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS pack_2x_per_week INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS pack_8_flex INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS pack_12_flex INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE submissions ADD COLUMN IF NOT EXISTS pack_24_flex INTEGER NOT NULL DEFAULT 0",
        ]:
            conn.execute(text(alter_sql))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS scoring_settings (
                    id INTEGER PRIMARY KEY,
                    target_hours NUMERIC(10,2) NOT NULL,
                    target_booked INTEGER NOT NULL,
                    target_completed INTEGER NOT NULL,
                    target_pt_sold NUMERIC(12,2) NOT NULL,
                    weight_hours NUMERIC(10,2) NOT NULL,
                    weight_booked NUMERIC(10,2) NOT NULL,
                    weight_completed NUMERIC(10,2) NOT NULL,
                    weight_pt_sold NUMERIC(10,2) NOT NULL,
                    weight_booked_to_completed NUMERIC(10,2) NOT NULL DEFAULT 0,
                    weight_completed_to_sold NUMERIC(10,2) NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        for alter_sql in [
            "ALTER TABLE scoring_settings ADD COLUMN IF NOT EXISTS weight_booked_to_completed NUMERIC(10,2) NOT NULL DEFAULT 0",
            "ALTER TABLE scoring_settings ADD COLUMN IF NOT EXISTS weight_completed_to_sold NUMERIC(10,2) NOT NULL DEFAULT 0",
        ]:
            conn.execute(text(alter_sql))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_accounts (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    full_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    role VARCHAR(20) NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS trainer_documents (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    category VARCHAR(100) NOT NULL DEFAULT 'Other',
                    original_filename VARCHAR(255) NOT NULL,
                    file_ext VARCHAR(20) NOT NULL,
                    mime_type VARCHAR(255) NOT NULL,
                    file_data BYTEA NOT NULL,
                    file_size_bytes INTEGER NOT NULL,
                    club_scope VARCHAR(50) NOT NULL DEFAULT 'All Clubs',
                    requires_acknowledgment BOOLEAN NOT NULL DEFAULT FALSE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    uploaded_by_email VARCHAR(255) NOT NULL,
                    uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        for alter_sql in [
            "ALTER TABLE trainer_documents ADD COLUMN IF NOT EXISTS category VARCHAR(100) NOT NULL DEFAULT 'Other'",
            "ALTER TABLE trainer_documents ADD COLUMN IF NOT EXISTS requires_acknowledgment BOOLEAN NOT NULL DEFAULT FALSE",
        ]:
            conn.execute(text(alter_sql))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS document_acknowledgments (
                    id SERIAL PRIMARY KEY,
                    document_id INTEGER NOT NULL,
                    trainer_email VARCHAR(255) NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    acknowledged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (document_id, trainer_email)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS trainer_calendar_events (
                    id SERIAL PRIMARY KEY,
                    trainer_email VARCHAR(255) NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    event_date DATE NOT NULL,
                    start_time TIME NOT NULL,
                    end_time TIME NOT NULL,
                    event_title VARCHAR(255) NOT NULL,
                    notes TEXT,
                    external_source VARCHAR(50),
                    external_event_id VARCHAR(255),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        for alter_sql in [
            "ALTER TABLE trainer_calendar_events ADD COLUMN IF NOT EXISTS external_source VARCHAR(50)",
            "ALTER TABLE trainer_calendar_events ADD COLUMN IF NOT EXISTS external_event_id VARCHAR(255)",
        ]:
            conn.execute(text(alter_sql))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS trainer_clients (
                    id SERIAL PRIMARY KEY,
                    trainer_email VARCHAR(255) NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    client_name VARCHAR(150) NOT NULL,
                    goal TEXT,
                    start_date DATE,
                    current_pack_type VARCHAR(100),
                    sessions_remaining INTEGER NOT NULL DEFAULT 0,
                    last_session_date DATE,
                    next_session_date DATE,
                    status VARCHAR(50) NOT NULL DEFAULT 'Active',
                    notes TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS trainer_goals (
                    id SERIAL PRIMARY KEY,
                    trainer_email VARCHAR(255) NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    week_start DATE NOT NULL,
                    target_hours NUMERIC(10,2) NOT NULL,
                    target_booked INTEGER NOT NULL,
                    target_completed INTEGER NOT NULL,
                    target_packs NUMERIC(10,2) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (trainer_email, week_start)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pt_leads (
                    id SERIAL PRIMARY KEY,
                    assigned_trainer_email VARCHAR(255),
                    assigned_trainer_name VARCHAR(100),
                    club VARCHAR(50) NOT NULL,
                    lead_name VARCHAR(150) NOT NULL,
                    phone VARCHAR(50),
                    email VARCHAR(255),
                    source VARCHAR(100),
                    stage VARCHAR(50) NOT NULL DEFAULT 'New Lead',
                    goal TEXT,
                    next_action_date DATE,
                    notes TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        for alter_sql in [
            "ALTER TABLE pt_leads ADD COLUMN IF NOT EXISTS assigned_trainer_email VARCHAR(255)",
            "ALTER TABLE pt_leads ADD COLUMN IF NOT EXISTS assigned_trainer_name VARCHAR(100)",
        ]:
            conn.execute(text(alter_sql))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS coaching_notes (
                    id SERIAL PRIMARY KEY,
                    trainer_email VARCHAR(255) NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    note_type VARCHAR(50) NOT NULL,
                    note_text TEXT NOT NULL,
                    created_by_email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        existing = conn.execute(text("SELECT COUNT(*) FROM scoring_settings WHERE id = 1")).scalar()
        if existing == 0:
            conn.execute(
                text(
                    """
                    INSERT INTO scoring_settings (
                        id,
                        target_hours,
                        target_booked,
                        target_completed,
                        target_pt_sold,
                        weight_hours,
                        weight_booked,
                        weight_completed,
                        weight_pt_sold,
                        weight_booked_to_completed,
                        weight_completed_to_sold,
                        updated_at
                    ) VALUES (
                        1,
                        :target_hours,
                        :target_booked,
                        :target_completed,
                        :target_pt_sold,
                        :weight_hours,
                        :weight_booked,
                        :weight_completed,
                        :weight_pt_sold,
                        :weight_booked_to_completed,
                        :weight_completed_to_sold,
                        :updated_at
                    )
                    """
                ),
                {**DEFAULT_SCORING, "updated_at": datetime.now(EST)},
            )


def create_submission_unique_index():
    engine = get_engine()
    with engine.begin() as conn:
        dupes = conn.execute(
            text(
                """
                SELECT week_start, COALESCE(trainer_email, ''), trainer_name, club, COUNT(*) AS c
                FROM submissions
                GROUP BY week_start, COALESCE(trainer_email, ''), trainer_name, club
                HAVING COUNT(*) > 1
                LIMIT 1
                """
            )
        ).fetchone()
        if dupes is None:
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS submissions_unique_trainer_week_idx
                    ON submissions (week_start, trainer_name, club)
                    """
                )
            )


def sql_df(query: str, params: Optional[dict] = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


def get_settings() -> Dict:
    df = sql_df("SELECT * FROM scoring_settings WHERE id = 1")
    if df.empty:
        raise ValueError("Scoring settings not found.")
    return df.iloc[0].to_dict()


def get_user_account(email: str) -> Optional[Dict]:
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT email, full_name, club, role, password_hash, is_active, created_at, updated_at
                FROM user_accounts
                WHERE LOWER(email) = LOWER(:email)
                LIMIT 1
                """
            ),
            {"email": email.strip().lower()},
        ).mappings().first()
    return dict(row) if row else None


def get_all_user_accounts() -> pd.DataFrame:
    return sql_df(
        """
        SELECT email, full_name, club, role, is_active, created_at, updated_at
        FROM user_accounts
        ORDER BY role DESC, club, full_name
        """
    )


def upsert_trainer_account(email: str, full_name: str, club: str, password: str):
    engine = get_engine()
    password_hash = hash_password(password)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO user_accounts (email, full_name, club, role, password_hash, is_active, updated_at)
                VALUES (:email, :full_name, :club, 'trainer', :password_hash, TRUE, :updated_at)
                ON CONFLICT (email)
                DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    club = EXCLUDED.club,
                    password_hash = EXCLUDED.password_hash,
                    is_active = TRUE,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "email": email.strip().lower(),
                "full_name": full_name.strip(),
                "club": club,
                "password_hash": password_hash,
                "updated_at": datetime.now(EST),
            },
        )


def deactivate_user_account(email: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE user_accounts
                SET is_active = FALSE,
                    updated_at = :updated_at
                WHERE LOWER(email) = LOWER(:email)
                """
            ),
            {"email": email.strip().lower(), "updated_at": datetime.now(EST)},
        )


def update_user_password(email: str, new_password: str):
    engine = get_engine()
    password_hash = hash_password(new_password)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE user_accounts
                SET password_hash = :password_hash,
                    updated_at = :updated_at
                WHERE LOWER(email) = LOWER(:email)
                """
            ),
            {
                "email": email.strip().lower(),
                "password_hash": password_hash,
                "updated_at": datetime.now(EST),
            },
        )


def update_settings(
    target_hours: float,
    target_booked: int,
    target_completed: int,
    target_pt_sold: float,
    weight_hours: float,
    weight_booked: float,
    weight_completed: float,
    weight_pt_sold: float,
    weight_booked_to_completed: float,
    weight_completed_to_sold: float,
):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE scoring_settings
                SET target_hours = :target_hours,
                    target_booked = :target_booked,
                    target_completed = :target_completed,
                    target_pt_sold = :target_pt_sold,
                    weight_hours = :weight_hours,
                    weight_booked = :weight_booked,
                    weight_completed = :weight_completed,
                    weight_pt_sold = :weight_pt_sold,
                    weight_booked_to_completed = :weight_booked_to_completed,
                    weight_completed_to_sold = :weight_completed_to_sold,
                    updated_at = :updated_at
                WHERE id = 1
                """
            ),
            {
                "target_hours": target_hours,
                "target_booked": target_booked,
                "target_completed": target_completed,
                "target_pt_sold": target_pt_sold,
                "weight_hours": weight_hours,
                "weight_booked": weight_booked,
                "weight_completed": weight_completed,
                "weight_pt_sold": weight_pt_sold,
                "weight_booked_to_completed": weight_booked_to_completed,
                "weight_completed_to_sold": weight_completed_to_sold,
                "updated_at": datetime.now(EST),
            },
        )


def add_submission(
    week_start: date,
    trainer_email: str,
    trainer_name: str,
    club: str,
    hours_worked: float,
    kickoffs_booked: int,
    kickoffs_completed: int,
    pt_sold: float,
    weighted_pack_points: float,
    sessions_scheduled: int,
    sessions_completed: int,
    no_shows: int,
    cancellations: int,
    reschedules: int,
    pack_1x_per_week: int,
    pack_2x_per_week: int,
    pack_8_flex: int,
    pack_12_flex: int,
    pack_24_flex: int,
):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO submissions (
                    week_start,
                    trainer_email,
                    trainer_name,
                    club,
                    hours_worked,
                    kickoffs_booked,
                    kickoffs_completed,
                    pt_sold,
                    weighted_pack_points,
                    sessions_scheduled,
                    sessions_completed,
                    no_shows,
                    cancellations,
                    reschedules,
                    pack_1x_per_week,
                    pack_2x_per_week,
                    pack_8_flex,
                    pack_12_flex,
                    pack_24_flex,
                    submitted_at
                ) VALUES (
                    :week_start,
                    :trainer_email,
                    :trainer_name,
                    :club,
                    :hours_worked,
                    :kickoffs_booked,
                    :kickoffs_completed,
                    :pt_sold,
                    :weighted_pack_points,
                    :sessions_scheduled,
                    :sessions_completed,
                    :no_shows,
                    :cancellations,
                    :reschedules,
                    :pack_1x_per_week,
                    :pack_2x_per_week,
                    :pack_8_flex,
                    :pack_12_flex,
                    :pack_24_flex,
                    :submitted_at
                )
                """
            ),
            {
                "week_start": week_start,
                "trainer_email": trainer_email.strip().lower(),
                "trainer_name": trainer_name.strip(),
                "club": club,
                "hours_worked": hours_worked,
                "kickoffs_booked": kickoffs_booked,
                "kickoffs_completed": kickoffs_completed,
                "pt_sold": pt_sold,
                "weighted_pack_points": weighted_pack_points,
                "sessions_scheduled": sessions_scheduled,
                "sessions_completed": sessions_completed,
                "no_shows": no_shows,
                "cancellations": cancellations,
                "reschedules": reschedules,
                "pack_1x_per_week": pack_1x_per_week,
                "pack_2x_per_week": pack_2x_per_week,
                "pack_8_flex": pack_8_flex,
                "pack_12_flex": pack_12_flex,
                "pack_24_flex": pack_24_flex,
                "submitted_at": datetime.now(EST),
            },
        )


def has_submission_for_week(week_start: date, trainer_name: str, club: str) -> bool:
    engine = get_engine()
    with engine.begin() as conn:
        existing = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM submissions
                WHERE week_start = :week_start
                  AND trainer_name = :trainer_name
                  AND club = :club
                """
            ),
            {"week_start": week_start, "trainer_name": trainer_name.strip(), "club": club},
        ).scalar()
    return int(existing or 0) > 0


def get_submissions() -> pd.DataFrame:
    return sql_df("SELECT * FROM submissions ORDER BY week_start DESC, submitted_at DESC")


def add_calendar_event(
    trainer_email: str,
    trainer_name: str,
    club: str,
    event_date: date,
    start_time: time,
    end_time: time,
    event_title: str,
    notes: str,
    external_source: Optional[str] = None,
    external_event_id: Optional[str] = None,
):
    engine = get_engine()
    with engine.begin() as conn:
        if external_source and external_event_id:
            existing = conn.execute(
                text(
                    """
                    SELECT id
                    FROM trainer_calendar_events
                    WHERE external_source = :external_source
                      AND external_event_id = :external_event_id
                      AND is_active = TRUE
                    LIMIT 1
                    """
                ),
                {
                    "external_source": external_source,
                    "external_event_id": external_event_id,
                },
            ).fetchone()

            if existing:
                return

        conn.execute(
            text(
                """
                INSERT INTO trainer_calendar_events (
                    trainer_email, trainer_name, club, event_date, start_time, end_time,
                    event_title, notes, external_source, external_event_id,
                    is_active, created_at, updated_at
                ) VALUES (
                    :trainer_email, :trainer_name, :club, :event_date, :start_time, :end_time,
                    :event_title, :notes, :external_source, :external_event_id,
                    TRUE, :created_at, :updated_at
                )
                """
            ),
            {
                "trainer_email": trainer_email.strip().lower() if trainer_email else "",
                "trainer_name": trainer_name.strip() if trainer_name else "",
                "club": club,
                "event_date": event_date,
                "start_time": start_time,
                "end_time": end_time,
                "event_title": event_title.strip(),
                "notes": notes.strip(),
                "external_source": external_source,
                "external_event_id": external_event_id,
                "created_at": datetime.now(EST),
                "updated_at": datetime.now(EST),
            },
        )


def get_calendar_events(trainer_email: Optional[str] = None, club: Optional[str] = None) -> pd.DataFrame:
    base_query = """
        SELECT id, trainer_email, trainer_name, club, event_date, start_time, end_time,
               event_title, notes, external_source, external_event_id, created_at, updated_at
        FROM trainer_calendar_events
        WHERE is_active = TRUE
    """
    conditions = []
    params = {}
    if trainer_email:
        conditions.append("LOWER(trainer_email) = LOWER(:trainer_email)")
        params["trainer_email"] = trainer_email.strip().lower()
    if club and club != "All":
        conditions.append("club = :club")
        params["club"] = club
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    base_query += " ORDER BY event_date ASC, start_time ASC, trainer_name ASC"
    return sql_df(base_query, params)


def deactivate_calendar_event(event_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE trainer_calendar_events
                SET is_active = FALSE,
                    updated_at = :updated_at
                WHERE id = :event_id
                """
            ),
            {"event_id": int(event_id), "updated_at": datetime.now(EST)},
        )


def add_trainer_document(
    title: str,
    category: str,
    original_filename: str,
    file_ext: str,
    mime_type: str,
    file_bytes: bytes,
    club_scope: str,
    requires_acknowledgment: bool,
    uploaded_by_email: str,
):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO trainer_documents (
                    title, category, original_filename, file_ext, mime_type, file_data,
                    file_size_bytes, club_scope, requires_acknowledgment, is_active,
                    uploaded_by_email, uploaded_at, updated_at
                ) VALUES (
                    :title, :category, :original_filename, :file_ext, :mime_type, :file_data,
                    :file_size_bytes, :club_scope, :requires_acknowledgment, TRUE,
                    :uploaded_by_email, :uploaded_at, :updated_at
                )
                """
            ),
            {
                "title": title.strip(),
                "category": category,
                "original_filename": original_filename.strip(),
                "file_ext": file_ext.strip().lower(),
                "mime_type": mime_type.strip(),
                "file_data": file_bytes,
                "file_size_bytes": len(file_bytes),
                "club_scope": club_scope,
                "requires_acknowledgment": requires_acknowledgment,
                "uploaded_by_email": uploaded_by_email.strip().lower(),
                "uploaded_at": datetime.now(EST),
                "updated_at": datetime.now(EST),
            },
        )


def get_trainer_documents(club: Optional[str] = None) -> pd.DataFrame:
    if club and club != "All Clubs":
        return sql_df(
            """
            SELECT id, title, category, original_filename, file_ext, mime_type, file_size_bytes,
                   club_scope, requires_acknowledgment, uploaded_by_email, uploaded_at, updated_at
            FROM trainer_documents
            WHERE is_active = TRUE
              AND (club_scope = 'All Clubs' OR club_scope = :club_scope)
            ORDER BY uploaded_at DESC, title ASC
            """,
            {"club_scope": club},
        )
    return sql_df(
        """
        SELECT id, title, category, original_filename, file_ext, mime_type, file_size_bytes,
               club_scope, requires_acknowledgment, uploaded_by_email, uploaded_at, updated_at
        FROM trainer_documents
        WHERE is_active = TRUE
        ORDER BY uploaded_at DESC, title ASC
        """
    )


def get_trainer_document_file(document_id: int) -> Optional[Dict]:
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, title, category, original_filename, file_ext, mime_type, file_data, file_size_bytes,
                       club_scope, requires_acknowledgment, uploaded_by_email, uploaded_at, updated_at
                FROM trainer_documents
                WHERE id = :document_id AND is_active = TRUE
                LIMIT 1
                """
            ),
            {"document_id": int(document_id)},
        ).mappings().first()
    return dict(row) if row else None


def deactivate_trainer_document(document_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE trainer_documents
                SET is_active = FALSE,
                    updated_at = :updated_at
                WHERE id = :document_id
                """
            ),
            {"document_id": int(document_id), "updated_at": datetime.now(EST)},
        )


def acknowledge_document(document_id: int, trainer_email: str, trainer_name: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO document_acknowledgments (document_id, trainer_email, trainer_name, acknowledged_at)
                VALUES (:document_id, :trainer_email, :trainer_name, :acknowledged_at)
                ON CONFLICT (document_id, trainer_email) DO NOTHING
                """
            ),
            {
                "document_id": int(document_id),
                "trainer_email": trainer_email.strip().lower(),
                "trainer_name": trainer_name.strip(),
                "acknowledged_at": datetime.now(EST),
            },
        )


def get_document_acknowledgments() -> pd.DataFrame:
    return sql_df(
        """
        SELECT da.document_id, td.title, da.trainer_email, da.trainer_name, da.acknowledged_at
        FROM document_acknowledgments da
        JOIN trainer_documents td ON td.id = da.document_id
        ORDER BY da.acknowledged_at DESC
        """
    )


def get_trainer_document_ack(document_id: int, trainer_email: str) -> bool:
    df = sql_df(
        """
        SELECT COUNT(*) AS c
        FROM document_acknowledgments
        WHERE document_id = :document_id AND LOWER(trainer_email) = LOWER(:trainer_email)
        """,
        {"document_id": int(document_id), "trainer_email": trainer_email.strip().lower()},
    )
    return int(df.iloc[0]["c"]) > 0


def upsert_client(
    client_id: Optional[int],
    trainer_email: str,
    trainer_name: str,
    club: str,
    client_name: str,
    goal: str,
    start_date: Optional[date],
    current_pack_type: str,
    sessions_remaining: int,
    last_session_date: Optional[date],
    next_session_date: Optional[date],
    status: str,
    notes: str,
):
    engine = get_engine()
    with engine.begin() as conn:
        if client_id:
            conn.execute(
                text(
                    """
                    UPDATE trainer_clients
                    SET trainer_email = :trainer_email,
                        trainer_name = :trainer_name,
                        club = :club,
                        client_name = :client_name,
                        goal = :goal,
                        start_date = :start_date,
                        current_pack_type = :current_pack_type,
                        sessions_remaining = :sessions_remaining,
                        last_session_date = :last_session_date,
                        next_session_date = :next_session_date,
                        status = :status,
                        notes = :notes,
                        updated_at = :updated_at
                    WHERE id = :client_id
                    """
                ),
                {
                    "client_id": int(client_id),
                    "trainer_email": trainer_email.strip().lower(),
                    "trainer_name": trainer_name.strip(),
                    "club": club,
                    "client_name": client_name.strip(),
                    "goal": goal.strip(),
                    "start_date": start_date,
                    "current_pack_type": current_pack_type,
                    "sessions_remaining": sessions_remaining,
                    "last_session_date": last_session_date,
                    "next_session_date": next_session_date,
                    "status": status,
                    "notes": notes.strip(),
                    "updated_at": datetime.now(EST),
                },
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO trainer_clients (
                        trainer_email, trainer_name, club, client_name, goal, start_date,
                        current_pack_type, sessions_remaining, last_session_date, next_session_date,
                        status, notes, is_active, created_at, updated_at
                    ) VALUES (
                        :trainer_email, :trainer_name, :club, :client_name, :goal, :start_date,
                        :current_pack_type, :sessions_remaining, :last_session_date, :next_session_date,
                        :status, :notes, TRUE, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "trainer_email": trainer_email.strip().lower(),
                    "trainer_name": trainer_name.strip(),
                    "club": club,
                    "client_name": client_name.strip(),
                    "goal": goal.strip(),
                    "start_date": start_date,
                    "current_pack_type": current_pack_type,
                    "sessions_remaining": sessions_remaining,
                    "last_session_date": last_session_date,
                    "next_session_date": next_session_date,
                    "status": status,
                    "notes": notes.strip(),
                    "created_at": datetime.now(EST),
                    "updated_at": datetime.now(EST),
                },
            )


def get_clients(trainer_email: Optional[str] = None, club: Optional[str] = None) -> pd.DataFrame:
    base_query = """
        SELECT *
        FROM trainer_clients
        WHERE is_active = TRUE
    """
    params = {}
    filters = []
    if trainer_email:
        filters.append("LOWER(trainer_email) = LOWER(:trainer_email)")
        params["trainer_email"] = trainer_email.strip().lower()
    if club and club != "All":
        filters.append("club = :club")
        params["club"] = club
    if filters:
        base_query += " AND " + " AND ".join(filters)
    base_query += " ORDER BY client_name ASC"
    return sql_df(base_query, params)


def deactivate_client(client_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE trainer_clients
                SET is_active = FALSE,
                    updated_at = :updated_at
                WHERE id = :client_id
                """
            ),
            {"client_id": int(client_id), "updated_at": datetime.now(EST)},
        )


def upsert_lead(
    lead_id: Optional[int],
    assigned_trainer_email: Optional[str],
    assigned_trainer_name: Optional[str],
    club: str,
    lead_name: str,
    phone: str,
    email: str,
    source: str,
    stage: str,
    goal: str,
    next_action_date: Optional[date],
    notes: str,
):
    engine = get_engine()
    with engine.begin() as conn:
        payload = {
            "assigned_trainer_email": assigned_trainer_email.strip().lower() if assigned_trainer_email else None,
            "assigned_trainer_name": assigned_trainer_name.strip() if assigned_trainer_name else None,
            "club": club,
            "lead_name": lead_name.strip(),
            "phone": phone.strip(),
            "email": email.strip(),
            "source": source.strip(),
            "stage": stage,
            "goal": goal.strip(),
            "next_action_date": next_action_date,
            "notes": notes.strip(),
            "updated_at": datetime.now(EST),
        }
        if lead_id:
            payload["lead_id"] = int(lead_id)
            conn.execute(
                text(
                    """
                    UPDATE pt_leads
                    SET assigned_trainer_email = :assigned_trainer_email,
                        assigned_trainer_name = :assigned_trainer_name,
                        club = :club,
                        lead_name = :lead_name,
                        phone = :phone,
                        email = :email,
                        source = :source,
                        stage = :stage,
                        goal = :goal,
                        next_action_date = :next_action_date,
                        notes = :notes,
                        updated_at = :updated_at
                    WHERE id = :lead_id
                    """
                ),
                payload,
            )
        else:
            payload["created_at"] = datetime.now(EST)
            conn.execute(
                text(
                    """
                    INSERT INTO pt_leads (
                        assigned_trainer_email, assigned_trainer_name, club, lead_name, phone, email, source,
                        stage, goal, next_action_date, notes, is_active, created_at, updated_at
                    ) VALUES (
                        :assigned_trainer_email, :assigned_trainer_name, :club, :lead_name, :phone, :email, :source,
                        :stage, :goal, :next_action_date, :notes, TRUE, :created_at, :updated_at
                    )
                    """
                ),
                payload,
            )


def get_leads(trainer_email: Optional[str] = None, club: Optional[str] = None, assigned_only: bool = False) -> pd.DataFrame:
    base_query = "SELECT * FROM pt_leads WHERE is_active = TRUE"
    params = {}
    filters = []
    if trainer_email:
        filters.append("LOWER(assigned_trainer_email) = LOWER(:trainer_email)")
        params["trainer_email"] = trainer_email.strip().lower()
    elif assigned_only:
        filters.append("assigned_trainer_email IS NOT NULL")
    if club and club != "All":
        filters.append("club = :club")
        params["club"] = club
    if filters:
        base_query += " AND " + " AND ".join(filters)
    base_query += " ORDER BY updated_at DESC, lead_name ASC"
    return sql_df(base_query, params)


def deactivate_lead(lead_id: int):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE pt_leads
                SET is_active = FALSE,
                    updated_at = :updated_at
                WHERE id = :lead_id
                """
            ),
            {"lead_id": int(lead_id), "updated_at": datetime.now(EST)},
        )


def upsert_goal(
    trainer_email: str,
    trainer_name: str,
    club: str,
    week_start: date,
    target_hours: float,
    target_booked: int,
    target_completed: int,
    target_packs: float,
):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO trainer_goals (
                    trainer_email, trainer_name, club, week_start,
                    target_hours, target_booked, target_completed, target_packs,
                    created_at, updated_at
                ) VALUES (
                    :trainer_email, :trainer_name, :club, :week_start,
                    :target_hours, :target_booked, :target_completed, :target_packs,
                    :created_at, :updated_at
                )
                ON CONFLICT (trainer_email, week_start)
                DO UPDATE SET
                    trainer_name = EXCLUDED.trainer_name,
                    club = EXCLUDED.club,
                    target_hours = EXCLUDED.target_hours,
                    target_booked = EXCLUDED.target_booked,
                    target_completed = EXCLUDED.target_completed,
                    target_packs = EXCLUDED.target_packs,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "trainer_email": trainer_email.strip().lower(),
                "trainer_name": trainer_name.strip(),
                "club": club,
                "week_start": week_start,
                "target_hours": target_hours,
                "target_booked": target_booked,
                "target_completed": target_completed,
                "target_packs": target_packs,
                "created_at": datetime.now(EST),
                "updated_at": datetime.now(EST),
            },
        )


def get_goals(week_start: Optional[date] = None, trainer_email: Optional[str] = None) -> pd.DataFrame:
    query = "SELECT * FROM trainer_goals WHERE 1=1"
    params = {}
    if week_start:
        query += " AND week_start = :week_start"
        params["week_start"] = week_start
    if trainer_email:
        query += " AND LOWER(trainer_email) = LOWER(:trainer_email)"
        params["trainer_email"] = trainer_email.strip().lower()
    query += " ORDER BY club, trainer_name"
    return sql_df(query, params)


def add_coaching_note(trainer_email: str, trainer_name: str, club: str, note_type: str, note_text: str, created_by_email: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO coaching_notes (
                    trainer_email, trainer_name, club, note_type, note_text, created_by_email, created_at
                ) VALUES (
                    :trainer_email, :trainer_name, :club, :note_type, :note_text, :created_by_email, :created_at
                )
                """
            ),
            {
                "trainer_email": trainer_email.strip().lower(),
                "trainer_name": trainer_name.strip(),
                "club": club,
                "note_type": note_type,
                "note_text": note_text.strip(),
                "created_by_email": created_by_email.strip().lower(),
                "created_at": datetime.now(EST),
            },
        )


def get_coaching_notes(trainer_email: Optional[str] = None, club: Optional[str] = None) -> pd.DataFrame:
    query = "SELECT * FROM coaching_notes WHERE 1=1"
    params = {}
    if trainer_email:
        query += " AND LOWER(trainer_email) = LOWER(:trainer_email)"
        params["trainer_email"] = trainer_email.strip().lower()
    if club and club != "All":
        query += " AND club = :club"
        params["club"] = club
    query += " ORDER BY created_at DESC"
    return sql_df(query, params)


# -------------------------------------------------
# SCORING + ANALYTICS
# -------------------------------------------------
def get_current_week_start() -> date:
    today_est = datetime.now(EST).date()
    return today_est - timedelta(days=today_est.weekday())


def safe_pct(numerator: float, denominator: float) -> float:
    if float(denominator or 0) <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100, 2)


def metric_score(actual: float, target: float, weight: float) -> float:
    if float(target) <= 0:
        return 0.0
    ratio = min(float(actual) / float(target), 1.0)
    return round(ratio * float(weight), 2)


def conversion_score(actual_pct: float, target_pct: float, weight: float) -> float:
    if target_pct <= 0:
        return 0.0
    ratio = min(actual_pct / target_pct, 1.0)
    return round(ratio * weight, 2)


def calculate_score(row: pd.Series, settings: Dict) -> Tuple[float, Dict[str, float]]:
    booked_to_completed_pct = safe_pct(row["kickoffs_completed"], row["kickoffs_booked"])
    completed_to_sold_pct = safe_pct(row["pt_sold"], row["kickoffs_completed"])

    target_b2c = 80.0
    target_c2s = 70.0

    parts = {
        "Hours Score": metric_score(row["hours_worked"], settings["target_hours"], settings["weight_hours"]),
        "Booked Score": metric_score(row["kickoffs_booked"], settings["target_booked"], settings["weight_booked"]),
        "Completed Score": metric_score(row["kickoffs_completed"], settings["target_completed"], settings["weight_completed"]),
        "Packs Sold Score": metric_score(row["pt_sold"], settings["target_pt_sold"], settings["weight_pt_sold"]),
        "Booked->Completed Score": conversion_score(booked_to_completed_pct, target_b2c, settings.get("weight_booked_to_completed", 0)),
        "Completed->Sold Score": conversion_score(completed_to_sold_pct, target_c2s, settings.get("weight_completed_to_sold", 0)),
    }
    total = round(sum(parts.values()), 2)
    return total, parts


def build_scored_df(df: pd.DataFrame, settings: Dict) -> pd.DataFrame:
    if df.empty:
        return df

    rows = []
    for _, row in df.iterrows():
        total, parts = calculate_score(row, settings)
        item = row.to_dict()
        item["Booked to Completed %"] = safe_pct(row["kickoffs_completed"], row["kickoffs_booked"])
        item["Completed to Sold %"] = safe_pct(row["pt_sold"], row["kickoffs_completed"])
        item["Booked to Sold %"] = safe_pct(row["pt_sold"], row["kickoffs_booked"])
        item["Show Rate %"] = safe_pct(row["sessions_completed"], row["sessions_scheduled"])
        item["Revenue Per Hour Proxy"] = round(float(row["weighted_pack_points"]) / max(float(row["hours_worked"] or 0.01), 0.01), 2)
        item.update(parts)
        item["Trainer Score"] = total
        rows.append(item)

    scored = pd.DataFrame(rows)
    ordered_cols = [
        "week_start",
        "trainer_email",
        "trainer_name",
        "club",
        "hours_worked",
        "kickoffs_booked",
        "kickoffs_completed",
        "pt_sold",
        "weighted_pack_points",
        "sessions_scheduled",
        "sessions_completed",
        "no_shows",
        "cancellations",
        "reschedules",
        "pack_1x_per_week",
        "pack_2x_per_week",
        "pack_8_flex",
        "pack_12_flex",
        "pack_24_flex",
        "Booked to Completed %",
        "Completed to Sold %",
        "Booked to Sold %",
        "Show Rate %",
        "Revenue Per Hour Proxy",
        "Trainer Score",
        "Hours Score",
        "Booked Score",
        "Completed Score",
        "Packs Sold Score",
        "Booked->Completed Score",
        "Completed->Sold Score",
        "submitted_at",
    ]
    existing = [c for c in ordered_cols if c in scored.columns]
    return scored[existing]


def build_calendar_view(df: pd.DataFrame, view_mode: str, anchor_date: date) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame["event_date"] = pd.to_datetime(frame["event_date"]).dt.date
    frame["start_time_str"] = frame["start_time"].astype(str).str.slice(0, 5)
    frame["end_time_str"] = frame["end_time"].astype(str).str.slice(0, 5)
    if view_mode == "Day":
        frame = frame[frame["event_date"] == anchor_date]
    elif view_mode == "Week":
        week_start = anchor_date - timedelta(days=anchor_date.weekday())
        week_end = week_start + timedelta(days=6)
        frame = frame[(frame["event_date"] >= week_start) & (frame["event_date"] <= week_end)]
    else:
        dt_series = pd.to_datetime(frame["event_date"])
        frame = frame[(dt_series.dt.month == anchor_date.month) & (dt_series.dt.year == anchor_date.year)]
    if frame.empty:
        return frame
    return frame[["event_date", "start_time_str", "end_time_str", "trainer_name", "club", "event_title", "notes"]].rename(
        columns={
            "event_date": "Date",
            "start_time_str": "Start",
            "end_time_str": "End",
            "trainer_name": "Trainer",
            "club": "Club",
            "event_title": "Title",
            "notes": "Notes",
        }
    )


def client_at_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    today = datetime.now(EST).date()
    frame["last_session_date"] = pd.to_datetime(frame["last_session_date"], errors="coerce").dt.date
    frame["next_session_date"] = pd.to_datetime(frame["next_session_date"], errors="coerce").dt.date

    frame["No Session 14+ Days"] = frame["last_session_date"].apply(
        lambda x: False if pd.isna(x) else (today - x).days >= 14
    )
    frame["No Future Session"] = frame["next_session_date"].apply(
        lambda x: True if pd.isna(x) else x < today
    )
    frame["Low Sessions Remaining"] = frame["sessions_remaining"].fillna(0).astype(int) <= 2
    frame["At Risk Flag"] = (
        frame["No Session 14+ Days"] | frame["No Future Session"] | frame["Low Sessions Remaining"]
    )
    return frame


def pack_mix_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    cols = ["pack_1x_per_week", "pack_2x_per_week", "pack_8_flex", "pack_12_flex", "pack_24_flex"]
    summary = df.groupby("club", as_index=False)[cols].sum()
    return summary


# -------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------
def normalize_email(email: str) -> str:
    return email.strip().lower()


def is_director_email(email: str) -> bool:
    return normalize_email(email) in DIRECTOR_EMAILS


def authenticate_user(email: str, password: str) -> Optional[Dict]:
    email = normalize_email(email)
    password = password.strip()
    if not email or not password:
        return None

    account = get_user_account(email)
    if account:
        if not account.get("is_active", False):
            return None
        if verify_password(password, str(account["password_hash"])):
            return {
                "email": account["email"],
                "full_name": account["full_name"],
                "club": account["club"],
                "role": account["role"],
            }
        return None

    if is_director_email(email) and DIRECTOR_MASTER_PASSWORD and password == DIRECTOR_MASTER_PASSWORD:
        return {
            "email": email,
            "full_name": "PT Director",
            "club": "All Clubs",
            "role": "director",
        }
    return None


def show_login_screen():
    st.title("Club 24 PT Management Dashboard")
    st.caption("Better Clubs. Better Price. Always Open.")
    st.subheader("Website Login")
    st.write("Log in with your Club 24 email and password.")
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        login = st.form_submit_button("Log In")
        if login:
            user = authenticate_user(email, password)
            if user:
                st.session_state["auth_user"] = user
                st.rerun()
            else:
                st.error("Invalid email or password.")
    st.info("Trainer logins are created by the PT director. Director access is restricted to approved director emails only.")
    st.stop()


def require_login() -> Dict:
    if "auth_user" not in st.session_state:
        show_login_screen()
    user = st.session_state["auth_user"]
    with st.sidebar:
        st.success(f"Logged in as {user['email']}")
        st.write(f"Role: {user['role'].title()}")
        if user["role"] == "trainer":
            st.write(f"Trainer: {user['full_name']}")
            st.write(f"Club: {user['club']}")
        if st.button("Log Out"):
            del st.session_state["auth_user"]
            st.rerun()
    return user


# -------------------------------------------------
# UI HELPERS
# -------------------------------------------------
def format_file_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def preview_document(doc_record: Dict):
    file_bytes = bytes(doc_record["file_data"])
    file_ext = str(doc_record["file_ext"]).lower()
    mime_type = doc_record["mime_type"]

    st.write(f"### Preview: {doc_record['title']}")

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label=f"Download {doc_record['title']}",
            data=file_bytes,
            file_name=doc_record["original_filename"],
            mime=mime_type,
            use_container_width=True,
        )
    with col2:
        st.download_button(
            label="Open Full Screen",
            data=file_bytes,
            file_name=doc_record["original_filename"],
            mime=mime_type,
            use_container_width=True,
        )

    try:
        if file_ext == "pdf":
            st.info("PDF preview can vary by browser. Use Download if preview does not render.")
        elif file_ext in ["png", "jpg", "jpeg"]:
            st.image(file_bytes, use_container_width=True)
        elif file_ext == "txt":
            st.text_area("Text Preview", file_bytes.decode("utf-8", errors="ignore"), height=400, disabled=True)
        elif file_ext == "csv":
            st.dataframe(pd.read_csv(io.BytesIO(file_bytes)), use_container_width=True)
        elif file_ext in ["xlsx", "xls"]:
            st.dataframe(pd.read_excel(io.BytesIO(file_bytes)), use_container_width=True)
        else:
            st.info("Preview not available for this file type. Use the buttons above to open or download it.")
    except Exception as e:
        st.warning(f"Preview could not be generated for this file: {e}")


def filter_by_week_club_trainer(scored_df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    weeks = ["All"] + sorted(scored_df["week_start"].astype(str).unique().tolist(), reverse=True)
    selected_week = st.selectbox("Filter by Week", weeks, key=f"{key_prefix}_week")
    clubs = ["All"] + CLUBS
    selected_club = st.selectbox("Filter by Club", clubs, key=f"{key_prefix}_club")

    trainer_source = scored_df.copy()
    if selected_club != "All":
        trainer_source = trainer_source[trainer_source["club"] == selected_club]
    trainers = ["All"] + sorted(trainer_source["trainer_name"].astype(str).unique().tolist())
    selected_trainer = st.selectbox("Filter by Trainer", trainers, key=f"{key_prefix}_trainer")

    filtered = scored_df.copy()
    if selected_week != "All":
        filtered = filtered[filtered["week_start"].astype(str) == selected_week]
    if selected_club != "All":
        filtered = filtered[filtered["club"] == selected_club]
    if selected_trainer != "All":
        filtered = filtered[filtered["trainer_name"] == selected_trainer]
    return filtered


def trainer_options_df() -> pd.DataFrame:
    return sql_df(
        """
        SELECT email, full_name, club
        FROM user_accounts
        WHERE is_active = TRUE AND role = 'trainer'
        ORDER BY club, full_name
        """
    )


# -------------------------------------------------
# UI SECTIONS
# -------------------------------------------------
def render_change_password_tab(user: Dict):
    st.write("### Change Password")
    st.caption("Use this to update your own login password.")
    with st.form("change_password_form"):
        current_password = st.text_input("Current Password", type="password")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")
        save_password = st.form_submit_button("Update Password")
        if save_password:
            account = get_user_account(user["email"])
            if user["role"] == "director":
                if current_password != DIRECTOR_MASTER_PASSWORD:
                    st.error("Current password is incorrect.")
                elif not new_password.strip():
                    st.error("New password is required.")
                elif new_password != confirm_password:
                    st.error("New passwords do not match.")
                else:
                    st.error("Director password is controlled in Streamlit secrets. Update DIRECTOR_MASTER_PASSWORD there.")
            else:
                if not account:
                    st.error("User account not found.")
                elif not verify_password(current_password, str(account["password_hash"])):
                    st.error("Current password is incorrect.")
                elif len(new_password.strip()) < 8:
                    st.error("New password must be at least 8 characters.")
                elif new_password != confirm_password:
                    st.error("New passwords do not match.")
                else:
                    update_user_password(user["email"], new_password)
                    st.success("Password updated successfully.")


def render_trainer_documents_tab(user: Dict):
    st.subheader("Trainer Documents")
    st.write("Documents uploaded by the PT director appear here.")
    docs = get_trainer_documents(user["club"])
    if docs.empty:
        st.info("No documents available right now.")
        return
    docs = docs.copy()
    docs["size"] = docs["file_size_bytes"].apply(format_file_size)
    docs["Acknowledged"] = docs["id"].apply(lambda doc_id: get_trainer_document_ack(int(doc_id), user["email"]))
    st.dataframe(
        docs[["title", "category", "original_filename", "file_ext", "club_scope", "requires_acknowledgment", "Acknowledged", "size", "uploaded_at"]],
        use_container_width=True,
        hide_index=True,
    )
    selected_doc_id = st.selectbox(
        "Select a document to view",
        options=docs["id"].tolist(),
        format_func=lambda doc_id: f"{docs.loc[docs['id'] == doc_id, 'title'].iloc[0]} ({docs.loc[docs['id'] == doc_id, 'original_filename'].iloc[0]})",
    )
    doc_record = get_trainer_document_file(int(selected_doc_id))
    if doc_record:
        preview_document(doc_record)
        if doc_record.get("requires_acknowledgment"):
            acked = get_trainer_document_ack(int(selected_doc_id), user["email"])
            if acked:
                st.success("You acknowledged this document.")
            else:
                if st.button("Acknowledge Document"):
                    acknowledge_document(int(selected_doc_id), user["email"], user["full_name"])
                    st.success("Document acknowledged.")
                    st.rerun()


def render_trainer_calendar_tab(user: Dict):
    st.subheader("Trainer Calendar")
    st.write("Add and manage your training schedule here.")
    with st.form("trainer_calendar_form"):
        event_date = st.date_input("Event Date", value=datetime.now(EST).date())
        col1, col2 = st.columns(2)
        with col1:
            start_time = st.time_input("Start Time", value=time(9, 0))
        with col2:
            end_time = st.time_input("End Time", value=time(10, 0))
        event_title = st.text_input("Event Title")
        notes = st.text_area("Notes")
        add_event = st.form_submit_button("Add Calendar Event")
        if add_event:
            if not event_title.strip():
                st.error("Event title is required.")
            elif end_time <= start_time:
                st.error("End time must be later than start time.")
            else:
                add_calendar_event(
                    trainer_email=user["email"],
                    trainer_name=user["full_name"],
                    club=user["club"],
                    event_date=event_date,
                    start_time=start_time,
                    end_time=end_time,
                    event_title=event_title,
                    notes=notes,
                )
                st.success("Calendar event added.")
                st.rerun()

    events = get_calendar_events(trainer_email=user["email"])
    if events.empty:
        st.info("No calendar events yet.")
        return

    view_mode = st.selectbox("Calendar View", ["Day", "Week", "Month"], key="trainer_calendar_view")
    anchor_date = st.date_input("Calendar Date", value=datetime.now(EST).date(), key="trainer_calendar_anchor")
    view_df = build_calendar_view(events, view_mode, anchor_date)
    if view_df.empty:
        st.info(f"No events in this {view_mode.lower()} view.")
    else:
        st.dataframe(view_df, use_container_width=True, hide_index=True)

    events_display = events.copy()
    events_display["label"] = events_display.apply(
        lambda row: f"{row['event_date']} | {str(row['start_time'])[:5]}-{str(row['end_time'])[:5]} | {row['event_title']}",
        axis=1,
    )
    selected_event_id = st.selectbox(
        "Select your event to remove",
        options=events_display["id"].tolist(),
        format_func=lambda event_id: events_display.loc[events_display["id"] == event_id, "label"].iloc[0],
        key="trainer_remove_event_id",
    )
    if st.button("Remove Selected Event"):
        deactivate_calendar_event(int(selected_event_id))
        st.success("Event removed.")
        st.rerun()


def render_client_roster_tab(user: Dict):
    st.subheader("Client Roster")
    st.caption("Manage your active PT clients and flag at-risk members.")

    with st.form("client_form"):
        client_name = st.text_input("Client Name")
        goal = st.text_input("Primary Goal")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now(EST).date())
            current_pack_type = st.selectbox("Current Pack Type", [""] + PACK_TYPES)
            sessions_remaining = st.number_input("Sessions Remaining", min_value=0, step=1)
        with col2:
            last_session_date = st.date_input("Last Session Date", value=datetime.now(EST).date())
            next_session_date = st.date_input("Next Session Date", value=datetime.now(EST).date())
            status = st.selectbox("Status", CLIENT_STATUS_OPTIONS)
        notes = st.text_area("Notes")
        save_client = st.form_submit_button("Save Client")
        if save_client:
            if not client_name.strip():
                st.error("Client name is required.")
            else:
                upsert_client(
                    client_id=None,
                    trainer_email=user["email"],
                    trainer_name=user["full_name"],
                    club=user["club"],
                    client_name=client_name,
                    goal=goal,
                    start_date=start_date,
                    current_pack_type=current_pack_type,
                    sessions_remaining=int(sessions_remaining),
                    last_session_date=last_session_date,
                    next_session_date=next_session_date,
                    status=status,
                    notes=notes,
                )
                st.success("Client saved.")
                st.rerun()

    clients = client_at_risk_flags(get_clients(trainer_email=user["email"]))
    if clients.empty:
        st.info("No clients yet.")
        return
    st.dataframe(
        clients[[
            "client_name", "goal", "current_pack_type", "sessions_remaining", "last_session_date",
            "next_session_date", "status", "No Session 14+ Days", "No Future Session",
            "Low Sessions Remaining", "At Risk Flag"
        ]],
        use_container_width=True,
        hide_index=True,
    )
    deactivate_id = st.selectbox(
        "Select client to deactivate",
        options=clients["id"].tolist(),
        format_func=lambda client_id: clients.loc[clients["id"] == client_id, "client_name"].iloc[0],
        key="trainer_deactivate_client",
    )
    if st.button("Deactivate Selected Client"):
        deactivate_client(int(deactivate_id))
        st.success("Client deactivated.")
        st.rerun()


def render_lead_pipeline_tab(user: Dict):
    st.subheader("PT Lead Pipeline")
    st.caption("Club-level leads can be assigned to a trainer later. Trainers only see leads assigned to them.")
    leads = get_leads(trainer_email=user["email"])
    if leads.empty:
        st.info("No assigned leads yet.")
    else:
        st.dataframe(
            leads[["lead_name", "phone", "email", "source", "stage", "goal", "next_action_date", "notes"]],
            use_container_width=True,
            hide_index=True,
        )
        deactivate_id = st.selectbox(
            "Select assigned lead to deactivate",
            options=leads["id"].tolist(),
            format_func=lambda lead_id: leads.loc[leads["id"] == lead_id, "lead_name"].iloc[0],
            key="trainer_deactivate_lead",
        )
        if st.button("Deactivate Selected Lead"):
            deactivate_lead(int(deactivate_id))
            st.success("Lead deactivated.")
            st.rerun()


def render_trainer_action_items_tab(user: Dict):
    st.subheader("Today’s Action Items")
    today = datetime.now(EST).date()

    events = get_calendar_events(trainer_email=user["email"])
    today_events = events[pd.to_datetime(events["event_date"]).dt.date == today] if not events.empty else pd.DataFrame()
    clients = client_at_risk_flags(get_clients(trainer_email=user["email"]))
    leads = get_leads(trainer_email=user["email"])

    col1, col2, col3 = st.columns(3)
    col1.metric("Today’s Events", len(today_events))
    col2.metric("At-Risk Clients", int(clients["At Risk Flag"].sum()) if not clients.empty else 0)
    col3.metric("Leads Needing Action", int((pd.to_datetime(leads["next_action_date"], errors="coerce").dt.date <= today).fillna(False).sum()) if not leads.empty else 0)

    st.write("### Today’s Schedule")
    if today_events.empty:
        st.info("No events scheduled today.")
    else:
        today_events = today_events.copy()
        today_events["start_time"] = today_events["start_time"].astype(str).str.slice(0, 5)
        today_events["end_time"] = today_events["end_time"].astype(str).str.slice(0, 5)
        st.dataframe(today_events[["event_date", "start_time", "end_time", "event_title", "notes"]], use_container_width=True, hide_index=True)

    st.write("### Clients Requiring Follow-Up")
    if clients.empty:
        st.info("No clients yet.")
    else:
        follow_up_clients = clients[clients["At Risk Flag"] == True]
        if follow_up_clients.empty:
            st.success("No client alerts right now.")
        else:
            st.dataframe(
                follow_up_clients[["client_name", "sessions_remaining", "last_session_date", "next_session_date", "status"]],
                use_container_width=True,
                hide_index=True,
            )

    st.write("### Leads Requiring Action")
    if leads.empty:
        st.info("No leads yet.")
    else:
        leads = leads.copy()
        leads["next_action_date"] = pd.to_datetime(leads["next_action_date"], errors="coerce").dt.date
        action_leads = leads[leads["next_action_date"].notna() & (leads["next_action_date"] <= today)]
        if action_leads.empty:
            st.success("No lead actions due today.")
        else:
            st.dataframe(action_leads[["lead_name", "stage", "next_action_date", "goal", "notes"]], use_container_width=True, hide_index=True)


def render_trainer_view(user: Dict):
    trainer_name = user["full_name"]
    club = user["club"]
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        ["Weekly Submission", "Today", "Calendar", "Client Roster", "Lead Pipeline", "Trainer Documents", "Change Password"]
    )

    with tab1:
        st.subheader("Weekly Trainer Submission")
        st.write("Use this once per week for each trainer.")
        col1, col2 = st.columns(2)
        col1.text_input("Trainer Name", value=trainer_name, disabled=True)
        col2.text_input("Club", value=club, disabled=True)
        reporting_week = get_current_week_start()
        st.caption(f"Reporting Week: {reporting_week.strftime('%A, %B %d, %Y')}")

        with st.form("trainer_form", clear_on_submit=True):
            week_start = st.date_input(
                "Week Starting",
                value=reporting_week,
                disabled=True,
                help="Week Starting is locked to the current Monday for all trainers.",
            )
            already_submitted = has_submission_for_week(week_start, trainer_name, club)
            if already_submitted:
                st.warning("You already submitted your numbers for this week. Only one submission is allowed per week.")

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5)
                kickoffs_booked = st.number_input("Kickoffs Booked", min_value=0, step=1)
                kickoffs_completed = st.number_input("Kickoffs Completed", min_value=0, step=1)
            with col_b:
                sessions_scheduled = st.number_input("Sessions Scheduled", min_value=0, step=1)
                sessions_completed = st.number_input("Sessions Completed", min_value=0, step=1)
                no_shows = st.number_input("No-Shows", min_value=0, step=1)
                cancellations = st.number_input("Cancellations", min_value=0, step=1)
                reschedules = st.number_input("Reschedules", min_value=0, step=1)
            with col_c:
                pack_1x_per_week = st.number_input("1x per Week Sold", min_value=0, step=1)
                pack_2x_per_week = st.number_input("2x per Week Sold", min_value=0, step=1)
                pack_8_flex = st.number_input("8 Flex Pack Sold", min_value=0, step=1)
                pack_12_flex = st.number_input("12 Flex Pack Sold", min_value=0, step=1)
                pack_24_flex = st.number_input("24 Flex Pack Sold", min_value=0, step=1)

            total_packs_sold = int(pack_1x_per_week) + int(pack_2x_per_week) + int(pack_8_flex) + int(pack_12_flex) + int(pack_24_flex)
            weighted_pack_points = (
                pack_1x_per_week * PACK_WEIGHTS["1x per Week"]
                + pack_2x_per_week * PACK_WEIGHTS["2x per Week"]
                + pack_8_flex * PACK_WEIGHTS["8 Flex Pack"]
                + pack_12_flex * PACK_WEIGHTS["12 Flex Pack"]
                + pack_24_flex * PACK_WEIGHTS["24 Flex Pack"]
            )
            st.caption(f"Total Packs Sold: {total_packs_sold} | Weighted Pack Points: {weighted_pack_points:.2f}")

            submit = st.form_submit_button("Submit Weekly Numbers", disabled=already_submitted)
            if submit:
                if kickoffs_completed > kickoffs_booked:
                    st.error("Kickoffs Completed cannot be greater than Kickoffs Booked.")
                elif sessions_completed > sessions_scheduled:
                    st.error("Sessions Completed cannot be greater than Sessions Scheduled.")
                elif has_submission_for_week(week_start, trainer_name, club):
                    st.error("Submission already exists for this week.")
                else:
                    try:
                        add_submission(
                            week_start=week_start,
                            trainer_email=user["email"],
                            trainer_name=trainer_name,
                            club=club,
                            hours_worked=float(hours_worked),
                            kickoffs_booked=int(kickoffs_booked),
                            kickoffs_completed=int(kickoffs_completed),
                            pt_sold=float(total_packs_sold),
                            weighted_pack_points=float(weighted_pack_points),
                            sessions_scheduled=int(sessions_scheduled),
                            sessions_completed=int(sessions_completed),
                            no_shows=int(no_shows),
                            cancellations=int(cancellations),
                            reschedules=int(reschedules),
                            pack_1x_per_week=int(pack_1x_per_week),
                            pack_2x_per_week=int(pack_2x_per_week),
                            pack_8_flex=int(pack_8_flex),
                            pack_12_flex=int(pack_12_flex),
                            pack_24_flex=int(pack_24_flex),
                        )
                        st.success("Weekly submission saved.")
                        st.rerun()
                    except IntegrityError:
                        st.error("A submission already exists for this week.")
                    except SQLAlchemyError as e:
                        st.error(f"Could not save submission: {e}")
        st.divider()
        st.caption("Trainer score stays hidden from trainers.")

    with tab2:
        render_trainer_action_items_tab(user)
    with tab3:
        render_trainer_calendar_tab(user)
    with tab4:
        render_client_roster_tab(user)
    with tab5:
        render_lead_pipeline_tab(user)
    with tab6:
        render_trainer_documents_tab(user)
    with tab7:
        render_change_password_tab(user)


def render_dashboard_tab(user: Dict):
    df = get_submissions()
    settings = get_settings()
    scored_df = build_scored_df(df, settings)
    if scored_df.empty:
        st.info("No submissions yet.")
        return

    filtered = filter_by_week_club_trainer(scored_df, "director_main")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Submissions", len(filtered))
    c2.metric("Avg Trainer Score", round(filtered["Trainer Score"].mean(), 2) if not filtered.empty else 0)
    c3.metric("Booked → Completed %", round(filtered["Booked to Completed %"].mean(), 2) if not filtered.empty else 0)
    c4.metric("Completed → Sold %", round(filtered["Completed to Sold %"].mean(), 2) if not filtered.empty else 0)
    c5.metric("Show Rate %", round(filtered["Show Rate %"].mean(), 2) if not filtered.empty else 0)

    st.write("### Trainer Leaderboard")
    leaderboard_cols = [
        "week_start", "trainer_name", "club", "hours_worked", "kickoffs_booked", "kickoffs_completed",
        "pt_sold", "weighted_pack_points", "sessions_completed", "no_shows", "cancellations",
        "Booked to Completed %", "Completed to Sold %", "Show Rate %", "Trainer Score"
    ]
    st.dataframe(filtered[leaderboard_cols].sort_values(by="Trainer Score", ascending=False), use_container_width=True)

    st.write("### Club PT Health Scoreboard")
    club_summary = (
        filtered.groupby("club", as_index=False)
        .agg(
            submissions=("trainer_name", "count"),
            total_hours=("hours_worked", "sum"),
            total_booked=("kickoffs_booked", "sum"),
            total_completed=("kickoffs_completed", "sum"),
            total_packs_sold=("pt_sold", "sum"),
            weighted_pack_points=("weighted_pack_points", "sum"),
            avg_score=("Trainer Score", "mean"),
            avg_booked_to_completed=("Booked to Completed %", "mean"),
            avg_completed_to_sold=("Completed to Sold %", "mean"),
            avg_show_rate=("Show Rate %", "mean"),
        )
        .sort_values(by="avg_score", ascending=False)
    )
    club_summary["Club Health Score"] = (
        club_summary["avg_score"] * 0.5
        + club_summary["avg_booked_to_completed"] * 0.2
        + club_summary["avg_completed_to_sold"] * 0.2
        + club_summary["avg_show_rate"] * 0.1
    ).round(2)
    st.dataframe(club_summary, use_container_width=True)

    st.write("### Pack Mix Report")
    st.dataframe(pack_mix_summary(filtered), use_container_width=True)

    st.write("### Goal Tracking")
    selected_week = get_current_week_start()
    goals = get_goals(week_start=selected_week)
    if goals.empty:
        st.info("No trainer goals set for this week.")
    else:
        actuals = (
            filtered.groupby(["trainer_email", "trainer_name", "club"], as_index=False)
            .agg(
                actual_hours=("hours_worked", "sum"),
                actual_booked=("kickoffs_booked", "sum"),
                actual_completed=("kickoffs_completed", "sum"),
                actual_packs=("pt_sold", "sum"),
            )
        )
        merged = goals.merge(actuals, on=["trainer_email", "trainer_name", "club"], how="left")
        for col in ["actual_hours", "actual_booked", "actual_completed", "actual_packs"]:
            merged[col] = merged[col].fillna(0)
        merged["Hours % to Goal"] = merged.apply(lambda r: safe_pct(r["actual_hours"], r["target_hours"]), axis=1)
        merged["Booked % to Goal"] = merged.apply(lambda r: safe_pct(r["actual_booked"], r["target_booked"]), axis=1)
        merged["Completed % to Goal"] = merged.apply(lambda r: safe_pct(r["actual_completed"], r["target_completed"]), axis=1)
        merged["Packs % to Goal"] = merged.apply(lambda r: safe_pct(r["actual_packs"], r["target_packs"]), axis=1)
        st.dataframe(merged, use_container_width=True)

    st.write("### Full Director View")
    st.dataframe(filtered, use_container_width=True)


def render_master_calendar_tab():
    st.write("### Master Calendar")

    st.write("### ABC Calendar Sync")

    col1, col2 = st.columns([2, 1])

    with col1:
        selected_abc_club = st.selectbox(
            "Select ABC Club to Sync",
            list(ABC_CLUBS.keys()),
            key="abc_sync_club",
        )

    with col2:
        st.caption("ABC Club Numbers")
        st.write(ABC_CLUBS[selected_abc_club])

    sync_col1, sync_col2 = st.columns(2)

    with sync_col1:
        if st.button("Sync Selected ABC Club", use_container_width=True):
            try:
                events = fetch_calendar_events(selected_abc_club)
                imported_count = 0

                for e in events:
                    add_calendar_event(
                        trainer_email=e.get("trainer_email") or "",
                        trainer_name=e.get("trainer_name") or "Unknown Trainer",
                        club=e["club"],
                        event_date=e["date"],
                        start_time=e["start"],
                        end_time=e["end"],
                        event_title=e["title"],
                        notes="Imported from ABC",
                        external_source="ABC",
                        external_event_id=e["external_event_id"],
                    )
                    imported_count += 1

                st.success(
                    f"ABC sync completed for {selected_abc_club}. Imported {imported_count} event(s)."
                )
                st.rerun()

            except Exception as e:
                st.error(f"ABC calendar sync failed for {selected_abc_club}: {e}")

    with sync_col2:
        if st.button("Sync All ABC Clubs", use_container_width=True):
            try:
                total_imported = 0

                for club_name in ABC_CLUBS.keys():
                    events = fetch_calendar_events(club_name)

                    for e in events:
                        add_calendar_event(
                            trainer_email=e.get("trainer_email") or "",
                            trainer_name=e.get("trainer_name") or "Unknown Trainer",
                            club=e["club"],
                            event_date=e["date"],
                            start_time=e["start"],
                            end_time=e["end"],
                            event_title=e["title"],
                            notes="Imported from ABC",
                            external_source="ABC",
                            external_event_id=e["external_event_id"],
                        )
                        total_imported += 1

                st.success(
                    f"ABC sync completed for all clubs. Imported {total_imported} event(s)."
                )
                st.rerun()

            except Exception as e:
                st.error(f"ABC calendar sync failed: {e}")

    st.divider()

    events = get_calendar_events()
    if events.empty:
        st.info("No trainer calendar events yet.")
        return

    selected_calendar_club = st.selectbox(
        "Filter Calendar by Club",
        ["All"] + CLUBS,
        key="director_calendar_club",
    )

    event_source = events.copy()
    if selected_calendar_club != "All":
        event_source = event_source[event_source["club"] == selected_calendar_club]

    trainer_options = ["All"] + sorted(
        event_source["trainer_name"].astype(str).unique().tolist()
    )
    selected_calendar_trainer = st.selectbox(
        "Filter Calendar by Trainer",
        trainer_options,
        key="director_calendar_trainer",
    )

    if selected_calendar_trainer != "All":
        event_source = event_source[event_source["trainer_name"] == selected_calendar_trainer]

    calendar_view = st.selectbox(
        "Calendar View",
        ["Day", "Week", "Month"],
        key="director_calendar_view",
    )

    calendar_anchor_date = st.date_input(
        "Calendar Date",
        value=datetime.now(EST).date(),
        key="director_calendar_anchor",
    )

    master_view_df = build_calendar_view(event_source, calendar_view, calendar_anchor_date)

    if master_view_df.empty:
        st.info(f"No events in this {calendar_view.lower()} view.")
    else:
        st.dataframe(master_view_df, use_container_width=True, hide_index=True)

    st.write("### All Active Calendar Events")

    events_table = event_source.copy()
    events_table["start_time"] = events_table["start_time"].astype(str).str.slice(0, 5)
    events_table["end_time"] = events_table["end_time"].astype(str).str.slice(0, 5)

    display_cols = [
        "event_date",
        "start_time",
        "end_time",
        "trainer_name",
        "club",
        "event_title",
        "notes",
    ]

    if "external_source" in events_table.columns:
        display_cols.append("external_source")

    st.dataframe(
        events_table[display_cols],
        use_container_width=True,
        hide_index=True,
    )

def render_scoring_setup_tab():
    settings = get_settings()
    st.write("Adjust the targets and score weights here.")
    st.caption("Weights should total 100.")
    with st.form("settings_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            target_hours = st.number_input("Target Hours Worked", min_value=0.0, value=float(settings["target_hours"]), step=1.0)
            target_booked = st.number_input("Target Kickoffs Booked", min_value=0, value=int(settings["target_booked"]), step=1)
            target_completed = st.number_input("Target Kickoffs Completed", min_value=0, value=int(settings["target_completed"]), step=1)
            target_pt_sold = st.number_input("Target Packs Sold", min_value=0.0, value=float(settings["target_pt_sold"]), step=1.0)
        with col2:
            weight_hours = st.number_input("Weight: Hours", min_value=0.0, value=float(settings["weight_hours"]), step=1.0)
            weight_booked = st.number_input("Weight: Booked", min_value=0.0, value=float(settings["weight_booked"]), step=1.0)
            weight_completed = st.number_input("Weight: Completed", min_value=0.0, value=float(settings["weight_completed"]), step=1.0)
            weight_pt_sold = st.number_input("Weight: Packs Sold", min_value=0.0, value=float(settings["weight_pt_sold"]), step=1.0)
        with col3:
            weight_b2c = st.number_input("Weight: Booked → Completed %", min_value=0.0, value=float(settings.get("weight_booked_to_completed", 0)), step=1.0)
            weight_c2s = st.number_input("Weight: Completed → Sold %", min_value=0.0, value=float(settings.get("weight_completed_to_sold", 0)), step=1.0)
        save = st.form_submit_button("Save Scoring Settings")
        if save:
            total_weight = weight_hours + weight_booked + weight_completed + weight_pt_sold + weight_b2c + weight_c2s
            if round(total_weight, 2) != 100.0:
                st.error("Weights must total exactly 100.")
            else:
                update_settings(
                    target_hours=float(target_hours),
                    target_booked=int(target_booked),
                    target_completed=int(target_completed),
                    target_pt_sold=float(target_pt_sold),
                    weight_hours=float(weight_hours),
                    weight_booked=float(weight_booked),
                    weight_completed=float(weight_completed),
                    weight_pt_sold=float(weight_pt_sold),
                    weight_booked_to_completed=float(weight_b2c),
                    weight_completed_to_sold=float(weight_c2s),
                )
                st.success("Scoring settings updated.")
                st.rerun()


def render_user_logins_tab(user: Dict):
    st.write("### Create or Reset Trainer Login")
    st.caption("Only directors can create logins. Trainer login uses email + password.")
    with st.form("user_accounts_form"):
        new_email = st.text_input("Trainer Email")
        full_name = st.text_input("Trainer Name")
        club = st.selectbox("Club", CLUBS, key="user_account_club")
        temp_password = st.text_input("Password", type="password")
        save_user = st.form_submit_button("Save Trainer Login")
        if save_user:
            if not new_email.strip() or not full_name.strip() or not temp_password.strip():
                st.error("Email, trainer name, and password are required.")
            elif len(temp_password.strip()) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                upsert_trainer_account(new_email, full_name, club, temp_password)
                st.success("Trainer login saved.")
                st.rerun()

    st.write("### Disable Existing Login")
    with st.form("deactivate_user_form"):
        disable_email = st.text_input("Email to Disable")
        disable_submit = st.form_submit_button("Disable Login")
        if disable_submit:
            if not disable_email.strip():
                st.error("Email is required.")
            elif normalize_email(disable_email) in DIRECTOR_EMAILS:
                st.error("Director emails cannot be disabled here.")
            else:
                deactivate_user_account(disable_email)
                st.success("Login disabled.")
                st.rerun()
    accounts = get_all_user_accounts()
    if accounts.empty:
        st.info("No trainer logins yet.")
    else:
        st.dataframe(accounts, use_container_width=True)
    st.write("### Director Login Notes")
    st.write("Director access is limited to the approved emails and uses the secret `DIRECTOR_MASTER_PASSWORD`.")


def render_documents_tab(user: Dict):
    st.write("### Upload Trainer Documents")
    st.caption("Upload files for all clubs or a specific club. Trainers can view or download approved files from their portal.")
    with st.form("upload_documents_form"):
        document_scope = st.selectbox("Document Visibility", ["All Clubs"] + CLUBS)
        category = st.selectbox("Document Category", DOCUMENT_CATEGORY_OPTIONS)
        requires_acknowledgment = st.checkbox("Require trainer acknowledgment")
        uploaded_files = st.file_uploader(
            "Upload Documents",
            type=list(ALLOWED_DOCUMENT_TYPES.keys()),
            accept_multiple_files=True,
            help="Accepted file types: PDF, Word, Excel, PowerPoint, CSV, TXT, PNG, JPG, JPEG.",
        )
        upload_submit = st.form_submit_button("Upload Documents")
        if upload_submit:
            if not uploaded_files:
                st.error("Please choose at least one file to upload.")
            else:
                upload_count = 0
                for uploaded_file in uploaded_files:
                    filename = uploaded_file.name or "document"
                    file_ext = filename.split(".")[-1].lower() if "." in filename else ""
                    mime_type = uploaded_file.type or ALLOWED_DOCUMENT_TYPES.get(file_ext, "application/octet-stream")
                    if file_ext not in ALLOWED_DOCUMENT_TYPES:
                        st.error(f"{filename} is not an allowed file type.")
                        continue
                    file_bytes = uploaded_file.getvalue()
                    if not file_bytes:
                        st.error(f"{filename} is empty and was skipped.")
                        continue
                    title = filename.rsplit(".", 1)[0]
                    add_trainer_document(
                        title=title,
                        category=category,
                        original_filename=filename,
                        file_ext=file_ext,
                        mime_type=mime_type,
                        file_bytes=file_bytes,
                        club_scope=document_scope,
                        requires_acknowledgment=requires_acknowledgment,
                        uploaded_by_email=user["email"],
                    )
                    upload_count += 1
                if upload_count:
                    st.success(f"Uploaded {upload_count} document(s).")
                    st.rerun()

    docs = get_trainer_documents()
    if docs.empty:
        st.info("No active documents uploaded yet.")
    else:
        docs_display = docs.copy()
        docs_display["size"] = docs_display["file_size_bytes"].apply(format_file_size)
        st.dataframe(
            docs_display[["id", "title", "category", "original_filename", "file_ext", "club_scope", "requires_acknowledgment", "size", "uploaded_by_email", "uploaded_at"]],
            use_container_width=True,
            hide_index=True,
        )
        selected_doc_id = st.selectbox(
            "Select Document to Preview / Remove",
            options=docs_display["id"].tolist(),
            format_func=lambda doc_id: f"{docs_display.loc[docs_display['id'] == doc_id, 'title'].iloc[0]} ({docs_display.loc[docs_display['id'] == doc_id, 'club_scope'].iloc[0]})",
            key="disable_document_id",
        )
        doc_record = get_trainer_document_file(int(selected_doc_id))
        if doc_record:
            preview_document(doc_record)
        if st.button("Remove Selected Document"):
            deactivate_trainer_document(int(selected_doc_id))
            st.success("Document removed.")
            st.rerun()

        st.write("### Document Acknowledgments")
        ack_df = get_document_acknowledgments()
        if ack_df.empty:
            st.info("No acknowledgments yet.")
        else:
            st.dataframe(ack_df, use_container_width=True)


def render_client_roster_manager_tab():
    st.write("### Master Client Roster")
    selected_club = st.selectbox("Filter Clients by Club", ["All"] + CLUBS, key="director_clients_club")
    clients = client_at_risk_flags(get_clients(club=selected_club))
    if clients.empty:
        st.info("No clients yet.")
        return
    st.dataframe(
        clients[["trainer_name", "club", "client_name", "goal", "current_pack_type", "sessions_remaining", "last_session_date", "next_session_date", "status", "At Risk Flag"]],
        use_container_width=True,
        hide_index=True,
    )
    at_risk = clients[clients["At Risk Flag"] == True]
    st.write("### At-Risk Clients")
    if at_risk.empty:
        st.success("No at-risk clients right now.")
    else:
        st.dataframe(at_risk[["trainer_name", "client_name", "sessions_remaining", "last_session_date", "next_session_date", "status"]], use_container_width=True, hide_index=True)


def render_leads_manager_tab():
    st.write("### Master Lead Pipeline")
    trainers = trainer_options_df()
    selected_club = st.selectbox("Filter Leads by Club", ["All"] + CLUBS, key="director_leads_club")

    with st.form("director_lead_form"):
        lead_name = st.text_input("Lead Name")
        col1, col2, col3 = st.columns(3)
        with col1:
            club = st.selectbox("Lead Club", CLUBS)
            source = st.text_input("Source")
            stage = st.selectbox("Stage", LEAD_STAGE_OPTIONS)
        with col2:
            phone = st.text_input("Phone")
            email = st.text_input("Email")
            next_action_date = st.date_input("Next Action Date", value=datetime.now(EST).date())
        with col3:
            assigned_option = st.selectbox(
                "Assign Trainer",
                options=[""] + trainers["email"].tolist(),
                format_func=lambda v: "Unassigned" if v == "" else f"{trainers.loc[trainers['email'] == v, 'full_name'].iloc[0]} - {trainers.loc[trainers['email'] == v, 'club'].iloc[0]}",
            )
            goal = st.text_input("Goal")
        notes = st.text_area("Notes")
        save_lead = st.form_submit_button("Save Lead")
        if save_lead:
            if not lead_name.strip():
                st.error("Lead name is required.")
            else:
                assigned_name = None
                if assigned_option:
                    trainer_row = trainers[trainers["email"] == assigned_option].iloc[0]
                    assigned_name = trainer_row["full_name"]
                upsert_lead(
                    lead_id=None,
                    assigned_trainer_email=assigned_option or None,
                    assigned_trainer_name=assigned_name,
                    club=club,
                    lead_name=lead_name,
                    phone=phone,
                    email=email,
                    source=source,
                    stage=stage,
                    goal=goal,
                    next_action_date=next_action_date,
                    notes=notes,
                )
                st.success("Lead saved.")
                st.rerun()

    leads = get_leads(club=selected_club)
    if leads.empty:
        st.info("No leads yet.")
        return

    stage_summary = leads.groupby("stage", as_index=False).agg(leads=("id", "count")).sort_values(by="leads", ascending=False)
    st.dataframe(stage_summary, use_container_width=True, hide_index=True)

    st.write("### Unassigned Leads")
    unassigned = leads[leads["assigned_trainer_email"].isna() | (leads["assigned_trainer_email"] == "")]
    if unassigned.empty:
        st.success("No unassigned leads right now.")
    else:
        st.dataframe(unassigned[["club", "lead_name", "source", "stage", "goal", "next_action_date", "notes"]], use_container_width=True, hide_index=True)

    st.write("### All Leads")
    st.dataframe(leads[["assigned_trainer_name", "club", "lead_name", "stage", "source", "goal", "next_action_date", "notes"]], use_container_width=True, hide_index=True)

    st.write("### Assign or Reassign Lead")
    selected_lead_id = st.selectbox(
        "Lead",
        options=leads["id"].tolist(),
        format_func=lambda lead_id: f"{leads.loc[leads['id'] == lead_id, 'lead_name'].iloc[0]} - {leads.loc[leads['id'] == lead_id, 'club'].iloc[0]}",
        key="director_assign_lead",
    )
    assign_option = st.selectbox(
        "Assign to Trainer",
        options=[""] + trainers["email"].tolist(),
        format_func=lambda v: "Unassigned" if v == "" else f"{trainers.loc[trainers['email'] == v, 'full_name'].iloc[0]} - {trainers.loc[trainers['email'] == v, 'club'].iloc[0]}",
        key="director_assign_lead_trainer",
    )
    if st.button("Save Lead Assignment"):
        lead_row = leads[leads["id"] == selected_lead_id].iloc[0]
        assigned_name = None
        if assign_option:
            trainer_row = trainers[trainers["email"] == assign_option].iloc[0]
            assigned_name = trainer_row["full_name"]
        lead_next_action = pd.to_datetime(lead_row.get("next_action_date"), errors="coerce")
        upsert_lead(
            lead_id=int(selected_lead_id),
            assigned_trainer_email=assign_option or None,
            assigned_trainer_name=assigned_name,
            club=lead_row["club"],
            lead_name=lead_row["lead_name"],
            phone=lead_row.get("phone", "") or "",
            email=lead_row.get("email", "") or "",
            source=lead_row.get("source", "") or "",
            stage=lead_row["stage"],
            goal=lead_row.get("goal", "") or "",
            next_action_date=lead_next_action.date() if pd.notna(lead_next_action) else None,
            notes=lead_row.get("notes", "") or "",
        )
        st.success("Lead assignment updated.")
        st.rerun()

    deactivate_id = st.selectbox(
        "Select lead to deactivate",
        options=leads["id"].tolist(),
        format_func=lambda lead_id: leads.loc[leads["id"] == lead_id, "lead_name"].iloc[0],
        key="director_deactivate_lead",
    )
    if st.button("Deactivate Selected Lead"):
        deactivate_lead(int(deactivate_id))
        st.success("Lead deactivated.")
        st.rerun()


def render_goals_tab():
    st.write("### Weekly Trainer Goals")
    trainers = trainer_options_df()
    if trainers.empty:
        st.info("No trainer accounts yet.")
        return
    with st.form("goals_form"):
        goal_week = st.date_input("Goal Week Starting", value=get_current_week_start())
        trainer_email = st.selectbox(
            "Trainer",
            options=trainers["email"].tolist(),
            format_func=lambda email: f"{trainers.loc[trainers['email'] == email, 'full_name'].iloc[0]} - {trainers.loc[trainers['email'] == email, 'club'].iloc[0]}",
        )
        trainer_row = trainers[trainers["email"] == trainer_email].iloc[0]
        col1, col2 = st.columns(2)
        with col1:
            target_hours = st.number_input("Goal Hours", min_value=0.0, step=1.0)
            target_booked = st.number_input("Goal Kickoffs Booked", min_value=0, step=1)
        with col2:
            target_completed = st.number_input("Goal Kickoffs Completed", min_value=0, step=1)
            target_packs = st.number_input("Goal Packs Sold", min_value=0.0, step=1.0)
        save_goal = st.form_submit_button("Save Goal")
        if save_goal:
            upsert_goal(
                trainer_email=trainer_email,
                trainer_name=trainer_row["full_name"],
                club=trainer_row["club"],
                week_start=goal_week,
                target_hours=float(target_hours),
                target_booked=int(target_booked),
                target_completed=int(target_completed),
                target_packs=float(target_packs),
            )
            st.success("Goal saved.")
            st.rerun()
    goals = get_goals(week_start=get_current_week_start())
    if goals.empty:
        st.info("No goals for the current week yet.")
    else:
        st.dataframe(goals, use_container_width=True)


def render_coaching_log_tab(user: Dict):
    st.write("### Coaching & Accountability Log")
    trainers = trainer_options_df()
    if trainers.empty:
        st.info("No trainer accounts yet.")
        return
    with st.form("coaching_form"):
        trainer_email = st.selectbox(
            "Trainer",
            options=trainers["email"].tolist(),
            format_func=lambda email: f"{trainers.loc[trainers['email'] == email, 'full_name'].iloc[0]} - {trainers.loc[trainers['email'] == email, 'club'].iloc[0]}",
            key="coaching_trainer",
        )
        trainer_row = trainers[trainers["email"] == trainer_email].iloc[0]
        note_type = st.selectbox("Note Type", NOTE_TYPE_OPTIONS)
        note_text = st.text_area("Note")
        save_note = st.form_submit_button("Save Note")
        if save_note:
            if not note_text.strip():
                st.error("Note text is required.")
            else:
                add_coaching_note(
                    trainer_email=trainer_email,
                    trainer_name=trainer_row["full_name"],
                    club=trainer_row["club"],
                    note_type=note_type,
                    note_text=note_text,
                    created_by_email=user["email"],
                )
                st.success("Coaching note saved.")
                st.rerun()
    selected_club = st.selectbox("Filter Notes by Club", ["All"] + CLUBS, key="notes_club")
    notes = get_coaching_notes(club=selected_club)
    if notes.empty:
        st.info("No coaching notes yet.")
    else:
        st.dataframe(notes, use_container_width=True)


def render_action_dashboard_tab():
    st.write("### Director Action Dashboard")
    today = datetime.now(EST).date()
    clients = client_at_risk_flags(get_clients())
    leads = get_leads()
    events = get_calendar_events()

    due_leads = pd.DataFrame()
    if not leads.empty:
        leads = leads.copy()
        leads["next_action_date"] = pd.to_datetime(leads["next_action_date"], errors="coerce").dt.date
        due_leads = leads[leads["next_action_date"].notna() & (leads["next_action_date"] <= today)]

    today_events = pd.DataFrame()
    if not events.empty:
        today_events = events[pd.to_datetime(events["event_date"]).dt.date == today]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Today’s Events", len(today_events))
    col2.metric("At-Risk Clients", int(clients["At Risk Flag"].sum()) if not clients.empty else 0)
    col3.metric("Lead Actions Due", len(due_leads))
    col4.metric("Reactivation Clients", int((clients["status"] == "Reactivation").sum()) if not clients.empty else 0)

    st.write("### Trainers Missing Current Week Submission")
    trainers = trainer_options_df()
    current_week = get_current_week_start()
    submissions = get_submissions()
    missing_rows = []
    for _, row in trainers.iterrows():
        mask = (
            (pd.to_datetime(submissions["week_start"]).dt.date == current_week)
            & (submissions["trainer_name"] == row["full_name"])
            & (submissions["club"] == row["club"])
        ) if not submissions.empty else pd.Series([], dtype=bool)
        if submissions.empty or not mask.any():
            missing_rows.append(row.to_dict())
    if missing_rows:
        st.dataframe(pd.DataFrame(missing_rows), use_container_width=True, hide_index=True)
    else:
        st.success("All trainers submitted for the current week.")

    st.write("### Today’s Schedule")
    if today_events.empty:
        st.info("No events scheduled today.")
    else:
        today_events = today_events.copy()
        today_events["start_time"] = today_events["start_time"].astype(str).str.slice(0, 5)
        today_events["end_time"] = today_events["end_time"].astype(str).str.slice(0, 5)
        st.dataframe(today_events[["event_date", "start_time", "end_time", "trainer_name", "club", "event_title"]], use_container_width=True, hide_index=True)

    st.write("### Leads Needing Action")
    if due_leads.empty:
        st.info("No lead follow-ups due today.")
    else:
        st.dataframe(due_leads[["assigned_trainer_name", "club", "lead_name", "stage", "next_action_date", "notes"]], use_container_width=True, hide_index=True)

    st.write("### Reactivation Board")
    if clients.empty:
        st.info("No clients yet.")
    else:
        reactivation = clients[(clients["status"].isin(["Reactivation", "Expired", "At Risk"])) | (clients["At Risk Flag"] == True)]
        if reactivation.empty:
            st.info("No reactivation targets right now.")
        else:
            st.dataframe(reactivation[["trainer_name", "club", "client_name", "sessions_remaining", "last_session_date", "next_session_date", "status"]], use_container_width=True, hide_index=True)


def render_exports_tab():
    scored_df = build_scored_df(get_submissions(), get_settings())
    if scored_df.empty:
        st.info("No export data yet.")
        return
    director_csv = scored_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Director CSV",
        data=director_csv,
        file_name="club24_pt_director_dashboard.csv",
        mime="text/csv",
    )
    trainer_csv = scored_df[[c for c in scored_df.columns if c not in ["Trainer Score", "Hours Score", "Booked Score", "Completed Score", "Packs Sold Score", "Booked->Completed Score", "Completed->Sold Score"]]].to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Trainer Input CSV",
        data=trainer_csv,
        file_name="club24_trainer_inputs.csv",
        mime="text/csv",
    )


def render_director_dashboard(user: Dict):
    st.subheader("PT Director Dashboard")
    tabs = st.tabs([
        "Dashboard",
        "Action Items",
        "Master Calendar",
        "Client Roster",
        "Lead Pipeline",
        "Goals",
        "Scoring Setup",
        "User Logins",
        "Trainer Documents",
        "Coaching Log",
        "Change Password",
        "Exports",
    ])
    with tabs[0]:
        render_dashboard_tab(user)
    with tabs[1]:
        render_action_dashboard_tab()
    with tabs[2]:
        render_master_calendar_tab()
    with tabs[3]:
        render_client_roster_manager_tab()
    with tabs[4]:
        render_leads_manager_tab()
    with tabs[5]:
        render_goals_tab()
    with tabs[6]:
        render_scoring_setup_tab()
    with tabs[7]:
        render_user_logins_tab(user)
    with tabs[8]:
        render_documents_tab(user)
    with tabs[9]:
        render_coaching_log_tab(user)
    with tabs[10]:
        render_change_password_tab(user)
    with tabs[11]:
        render_exports_tab()


# -------------------------------------------------
# APP STARTUP
# -------------------------------------------------
try:
    init_db()
    create_submission_unique_index()
except (ValueError, SQLAlchemyError) as e:
    st.error(f"Database connection failed: {e}")
    st.info("Set DATABASE_URL first, then restart the app.")
    st.stop()

if not DIRECTOR_MASTER_PASSWORD:
    st.error("DIRECTOR_MASTER_PASSWORD is not set.")
    st.info("Add DIRECTOR_MASTER_PASSWORD to your Streamlit secrets before using the app.")
    st.stop()

user = require_login()

st.title("Club 24 PT Management Dashboard")
st.caption("Real Gyms. Real Goals. Real Results.")

if user["role"] == "director":
    mode = st.sidebar.radio("Choose view", ["Trainer Input", "PT Director Dashboard"])
else:
    mode = "Trainer Input"

if mode == "Trainer Input":
    if user["role"] == "trainer":
        render_trainer_view(user)
    else:
        st.subheader("Director trainer input locked")
        st.info("Director logins do not submit trainer numbers directly. Use a trainer login for submissions.")
else:
    render_director_dashboard(user)
