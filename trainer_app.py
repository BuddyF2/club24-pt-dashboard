# trainer_app.py
# Club 24 - Multi-Club PT Score Dashboard with Website Login
# Custom email/password login stored in PostgreSQL.
# Trainer name and club are auto-filled from the login.
# Only approved director emails can access the director dashboard.
# Only directors can create trainer logins.
# Trainers are locked to one submission per week.
# Users can change their own password.

from datetime import datetime, date
import hashlib
import hmac
import os
import secrets
from typing import Dict, Tuple, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


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

DEFAULT_SCORING = {
    "target_hours": 25,
    "target_booked": 8,
    "target_completed": 6,
    "target_pt_sold": 1000,
    "weight_hours": 20,
    "weight_booked": 25,
    "weight_completed": 25,
    "weight_pt_sold": 30,
}

PBKDF2_ITERATIONS = 200_000


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
        raise ValueError(
            "DATABASE_URL is not set. Add it as an environment variable or Streamlit secret."
        )
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
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    hours_worked NUMERIC(10,2) NOT NULL,
                    kickoffs_booked INTEGER NOT NULL,
                    kickoffs_completed INTEGER NOT NULL,
                    pt_sold NUMERIC(12,2) NOT NULL,
                    submitted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

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
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

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
                CREATE UNIQUE INDEX IF NOT EXISTS submissions_unique_trainer_week_idx
                ON submissions (week_start, trainer_name, club)
                """
            )
        )

        existing = conn.execute(
            text("SELECT COUNT(*) FROM scoring_settings WHERE id = 1")
        ).scalar()

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
                        updated_at
                    )
                    VALUES (
                        1,
                        :target_hours,
                        :target_booked,
                        :target_completed,
                        :target_pt_sold,
                        :weight_hours,
                        :weight_booked,
                        :weight_completed,
                        :weight_pt_sold,
                        :updated_at
                    )
                    """
                ),
                {
                    **DEFAULT_SCORING,
                    "updated_at": datetime.now(),
                },
            )



def get_settings() -> Dict:
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM scoring_settings WHERE id = 1", engine)

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
    engine = get_engine()
    return pd.read_sql(
        """
        SELECT email, full_name, club, role, is_active, created_at, updated_at
        FROM user_accounts
        ORDER BY role DESC, club, full_name
        """,
        engine,
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
                "updated_at": datetime.now(),
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
            {
                "email": email.strip().lower(),
                "updated_at": datetime.now(),
            },
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
                "updated_at": datetime.now(),
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
            {
                "week_start": week_start,
                "trainer_name": trainer_name.strip(),
                "club": club,
            },
        ).scalar()

    return int(existing or 0) > 0



def update_settings(
    target_hours: float,
    target_booked: int,
    target_completed: int,
    target_pt_sold: float,
    weight_hours: float,
    weight_booked: float,
    weight_completed: float,
    weight_pt_sold: float,
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
                "updated_at": datetime.now(),
            },
        )



def add_submission(
    week_start: date,
    trainer_name: str,
    club: str,
    hours_worked: float,
    kickoffs_booked: int,
    kickoffs_completed: int,
    pt_sold: float,
):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO submissions (
                    week_start,
                    trainer_name,
                    club,
                    hours_worked,
                    kickoffs_booked,
                    kickoffs_completed,
                    pt_sold,
                    submitted_at
                )
                VALUES (
                    :week_start,
                    :trainer_name,
                    :club,
                    :hours_worked,
                    :kickoffs_booked,
                    :kickoffs_completed,
                    :pt_sold,
                    :submitted_at
                )
                """
            ),
            {
                "week_start": week_start,
                "trainer_name": trainer_name.strip(),
                "club": club,
                "hours_worked": hours_worked,
                "kickoffs_booked": kickoffs_booked,
                "kickoffs_completed": kickoffs_completed,
                "pt_sold": pt_sold,
                "submitted_at": datetime.now(),
            },
        )



def get_submissions() -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM submissions ORDER BY week_start DESC, submitted_at DESC",
        engine,
    )


# -------------------------------------------------
# SCORING
# -------------------------------------------------
def metric_score(actual: float, target: float, weight: float) -> float:
    if float(target) <= 0:
        return 0.0
    ratio = min(float(actual) / float(target), 1.0)
    return round(ratio * float(weight), 2)



def calculate_score(row: pd.Series, settings: Dict) -> Tuple[float, Dict[str, float]]:
    parts = {
        "Hours Score": metric_score(row["hours_worked"], settings["target_hours"], settings["weight_hours"]),
        "Booked Score": metric_score(row["kickoffs_booked"], settings["target_booked"], settings["weight_booked"]),
        "Completed Score": metric_score(row["kickoffs_completed"], settings["target_completed"], settings["weight_completed"]),
        "PT Sold Score": metric_score(row["pt_sold"], settings["target_pt_sold"], settings["weight_pt_sold"]),
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
        item.update(parts)
        item["Trainer Score"] = total
        rows.append(item)

    scored = pd.DataFrame(rows)
    return scored[
        [
            "week_start",
            "trainer_name",
            "club",
            "hours_worked",
            "kickoffs_booked",
            "kickoffs_completed",
            "pt_sold",
            "Trainer Score",
            "Hours Score",
            "Booked Score",
            "Completed Score",
            "PT Sold Score",
            "submitted_at",
        ]
    ]


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
    st.title("Club 24 PT Score Dashboard")
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

    st.info(
        "Trainer logins are created by the PT director. "
        "Director access is restricted to approved director emails only."
    )
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
                    try:
                        update_user_password(user["email"], new_password)
                        st.success("Password updated successfully.")
                    except SQLAlchemyError as e:
                        st.error(f"Could not update password: {e}")



def render_trainer_view(user: Dict):
    trainer_name = user["full_name"]
    club = user["club"]

    tab1, tab2 = st.tabs(["Weekly Submission", "Change Password"])

    with tab1:
        st.subheader("Weekly Trainer Submission")
        st.write("Use this once per week for each trainer.")

        col1, col2 = st.columns(2)
        col1.text_input("Trainer Name", value=trainer_name, disabled=True)
        col2.text_input("Club", value=club, disabled=True)

        with st.form("trainer_form", clear_on_submit=True):
            week_start = st.date_input("Week Starting", value=date.today())

            already_submitted = has_submission_for_week(week_start, trainer_name, club)
            if already_submitted:
                st.warning("You already submitted your numbers for this week. Only one submission is allowed per week.")

            col_a, col_b = st.columns(2)
            with col_a:
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5)
                kickoffs_booked = st.number_input("Kickoffs Booked", min_value=0, step=1)
            with col_b:
                kickoffs_completed = st.number_input("Kickoffs Completed", min_value=0, step=1)
                pt_sold = st.number_input("PT Sold ($)", min_value=0.0, step=50.0)

            submit = st.form_submit_button("Submit Weekly Numbers", disabled=already_submitted)

            if submit:
                if kickoffs_completed > kickoffs_booked:
                    st.error("Kickoffs Completed cannot be greater than Kickoffs Booked.")
                elif has_submission_for_week(week_start, trainer_name, club):
                    st.error("Submission already exists for this week.")
                else:
                    try:
                        add_submission(
                            week_start=week_start,
                            trainer_name=trainer_name,
                            club=club,
                            hours_worked=float(hours_worked),
                            kickoffs_booked=int(kickoffs_booked),
                            kickoffs_completed=int(kickoffs_completed),
                            pt_sold=float(pt_sold),
                        )
                        st.success("Weekly submission saved.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"Could not save submission: {e}")

        st.divider()
        st.caption("Trainer score stays hidden from trainers.")

    with tab2:
        render_change_password_tab(user)



def render_director_dashboard(user: Dict):
    st.subheader("PT Director Dashboard")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Dashboard", "Scoring Setup", "User Logins", "Change Password", "Exports"]
    )

    with tab1:
        try:
            df = get_submissions()
            settings = get_settings()
        except SQLAlchemyError as e:
            st.error(f"Could not load dashboard: {e}")
            st.stop()

        scored_df = build_scored_df(df, settings)

        if scored_df.empty:
            st.info("No submissions yet.")
        else:
            weeks = ["All"] + sorted(
                scored_df["week_start"].astype(str).unique().tolist(),
                reverse=True,
            )
            selected_week = st.selectbox("Filter by Week", weeks)

            clubs = ["All"] + CLUBS
            selected_club = st.selectbox("Filter by Club", clubs)

            filtered = scored_df.copy()
            if selected_week != "All":
                filtered = filtered[filtered["week_start"].astype(str) == selected_week]
            if selected_club != "All":
                filtered = filtered[filtered["club"] == selected_club]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Submissions", len(filtered))
            c2.metric("Avg Trainer Score", round(filtered["Trainer Score"].mean(), 2))
            c3.metric("Kickoffs Completed", int(filtered["kickoffs_completed"].sum()))
            c4.metric("PT Sold", f"${filtered['pt_sold'].sum():,.2f}")

            st.write("### Trainer Leaderboard")
            leaderboard = filtered.sort_values(by="Trainer Score", ascending=False).reset_index(drop=True)
            st.dataframe(
                leaderboard[
                    [
                        "week_start",
                        "trainer_name",
                        "club",
                        "hours_worked",
                        "kickoffs_booked",
                        "kickoffs_completed",
                        "pt_sold",
                        "Trainer Score",
                    ]
                ],
                use_container_width=True,
            )

            st.write("### Club Scoreboard")
            club_summary = (
                filtered.groupby("club", as_index=False)
                .agg(
                    submissions=("trainer_name", "count"),
                    total_hours=("hours_worked", "sum"),
                    total_booked=("kickoffs_booked", "sum"),
                    total_completed=("kickoffs_completed", "sum"),
                    total_pt_sold=("pt_sold", "sum"),
                    avg_score=("Trainer Score", "mean"),
                )
                .sort_values(by="avg_score", ascending=False)
            )
            club_summary["avg_score"] = club_summary["avg_score"].round(2)
            st.dataframe(club_summary, use_container_width=True)

            st.write("### Full Director View")
            st.dataframe(filtered, use_container_width=True)

    with tab2:
        try:
            settings = get_settings()
        except SQLAlchemyError as e:
            st.error(f"Could not load scoring settings: {e}")
            st.stop()

        st.write("Adjust the targets and score weights here.")
        st.caption("Weights must total 100.")

        with st.form("settings_form"):
            col1, col2 = st.columns(2)
            with col1:
                target_hours = st.number_input(
                    "Target Hours Worked",
                    min_value=0.0,
                    value=float(settings["target_hours"]),
                    step=1.0,
                )
                target_booked = st.number_input(
                    "Target Kickoffs Booked",
                    min_value=0,
                    value=int(settings["target_booked"]),
                    step=1,
                )
                target_completed = st.number_input(
                    "Target Kickoffs Completed",
                    min_value=0,
                    value=int(settings["target_completed"]),
                    step=1,
                )
                target_pt_sold = st.number_input(
                    "Target PT Sold ($)",
                    min_value=0.0,
                    value=float(settings["target_pt_sold"]),
                    step=50.0,
                )
            with col2:
                weight_hours = st.number_input(
                    "Weight: Hours",
                    min_value=0.0,
                    value=float(settings["weight_hours"]),
                    step=1.0,
                )
                weight_booked = st.number_input(
                    "Weight: Booked",
                    min_value=0.0,
                    value=float(settings["weight_booked"]),
                    step=1.0,
                )
                weight_completed = st.number_input(
                    "Weight: Completed",
                    min_value=0.0,
                    value=float(settings["weight_completed"]),
                    step=1.0,
                )
                weight_pt_sold = st.number_input(
                    "Weight: PT Sold",
                    min_value=0.0,
                    value=float(settings["weight_pt_sold"]),
                    step=1.0,
                )

            save = st.form_submit_button("Save Scoring Settings")

            if save:
                total_weight = (
                    weight_hours + weight_booked + weight_completed + weight_pt_sold
                )
                if round(total_weight, 2) != 100.0:
                    st.error("Weights must total exactly 100.")
                else:
                    try:
                        update_settings(
                            target_hours=float(target_hours),
                            target_booked=int(target_booked),
                            target_completed=int(target_completed),
                            target_pt_sold=float(target_pt_sold),
                            weight_hours=float(weight_hours),
                            weight_booked=float(weight_booked),
                            weight_completed=float(weight_completed),
                            weight_pt_sold=float(weight_pt_sold),
                        )
                        st.success("Scoring settings updated.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"Could not update settings: {e}")

    with tab3:
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
                    try:
                        upsert_trainer_account(new_email, full_name, club, temp_password)
                        st.success("Trainer login saved.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"Could not save trainer login: {e}")

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
                    try:
                        deactivate_user_account(disable_email)
                        st.success("Login disabled.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"Could not disable login: {e}")

        try:
            accounts = get_all_user_accounts()
            if accounts.empty:
                st.info("No trainer logins yet.")
            else:
                st.dataframe(accounts, use_container_width=True)
        except SQLAlchemyError as e:
            st.error(f"Could not load user logins: {e}")

        st.write("### Director Login Notes")
        st.write(
            "Director access is limited to the approved emails and uses the secret `DIRECTOR_MASTER_PASSWORD`."
        )

    with tab4:
        render_change_password_tab(user)

    with tab5:
        try:
            df = get_submissions()
            settings = get_settings()
            scored_df = build_scored_df(df, settings)
        except SQLAlchemyError as e:
            st.error(f"Could not generate exports: {e}")
            st.stop()

        if scored_df.empty:
            st.info("No export data yet.")
        else:
            director_csv = scored_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Director CSV",
                data=director_csv,
                file_name="club24_pt_director_dashboard.csv",
                mime="text/csv",
            )

            trainer_csv = scored_df[
                [
                    "week_start",
                    "trainer_name",
                    "club",
                    "hours_worked",
                    "kickoffs_booked",
                    "kickoffs_completed",
                    "pt_sold",
                    "submitted_at",
                ]
            ].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Trainer Input CSV",
                data=trainer_csv,
                file_name="club24_trainer_inputs.csv",
                mime="text/csv",
            )


# -------------------------------------------------
# APP STARTUP
# -------------------------------------------------
try:
    init_db()
except (ValueError, SQLAlchemyError) as e:
    st.error(f"Database connection failed: {e}")
    st.info("Set DATABASE_URL first, then restart the app.")
    st.stop()

if not DIRECTOR_MASTER_PASSWORD:
    st.error("DIRECTOR_MASTER_PASSWORD is not set.")
    st.info("Add DIRECTOR_MASTER_PASSWORD to your Streamlit secrets before using the app.")
    st.stop()

user = require_login()

st.title("Club 24 PT Score Dashboard")
st.caption("Real Gyms. Real Goals. Real Results.")

if user["role"] == "director":
    mode = st.sidebar.radio(
        "Choose view",
        ["Trainer Input", "PT Director Dashboard"],
    )
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
