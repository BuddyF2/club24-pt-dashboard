# trainer_app.py
# Club 24 - Multi-Club PT Score Dashboard with Login
# Trainers log in with Google or Microsoft through Streamlit auth.
# Trainer name and club are auto-filled from the trainer_accounts table.
# Director access is granted only to approved director emails.

from datetime import datetime, date
import os
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


DATABASE_URL = get_database_url()


# -------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------
def get_logged_in_email() -> str:
    """
    Pull email from Streamlit native auth.
    Depending on provider, st.user may behave like a dict/object.
    This helper safely checks common shapes.
    """
    try:
        user = st.user
    except Exception:
        return ""

    if not getattr(user, "is_logged_in", False):
        return ""

    # dict-like access
    try:
        email = user.get("email", "")
        if email:
            return str(email).strip().lower()
    except Exception:
        pass

    # object-like access
    try:
        email = getattr(user, "email", "")
        if email:
            return str(email).strip().lower()
    except Exception:
        pass

    # nested data fallback
    try:
        data = getattr(user, "to_dict", lambda: {})()
        email = data.get("email", "")
        if email:
            return str(email).strip().lower()
    except Exception:
        pass

    return ""


def require_login() -> str:
    st.title("Club 24 PT Score Dashboard")
    st.caption("Better Clubs. Better Price. Always Open.")

    if not st.user.is_logged_in:
        st.subheader("Sign in required")
        st.write("Please sign in with your approved Club 24 account to continue.")
        if st.button("Log in"):
            st.login()
        st.stop()

    email = get_logged_in_email()

    if not email:
        st.error("Could not read your login email from the authentication provider.")
        if st.button("Log out"):
            st.logout()
        st.stop()

    with st.sidebar:
        st.success(f"Logged in as {email}")
        if st.button("Log out"):
            st.logout()

    return email


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
                CREATE TABLE IF NOT EXISTS trainer_accounts (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    trainer_name VARCHAR(100) NOT NULL,
                    club VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
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


def get_trainer_account(email: str) -> Optional[Dict]:
    engine = get_engine()
    query = text(
        """
        SELECT email, trainer_name, club
        FROM trainer_accounts
        WHERE LOWER(email) = LOWER(:email)
        LIMIT 1
        """
    )

    with engine.begin() as conn:
        row = conn.execute(query, {"email": email}).mappings().first()

    return dict(row) if row else None


def upsert_trainer_account(email: str, trainer_name: str, club: str):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO trainer_accounts (email, trainer_name, club)
                VALUES (:email, :trainer_name, :club)
                ON CONFLICT (email)
                DO UPDATE SET
                    trainer_name = EXCLUDED.trainer_name,
                    club = EXCLUDED.club
                """
            ),
            {
                "email": email.strip().lower(),
                "trainer_name": trainer_name.strip(),
                "club": club,
            },
        )


def get_all_trainer_accounts() -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(
        "SELECT email, trainer_name, club, created_at FROM trainer_accounts ORDER BY club, trainer_name",
        engine,
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
# ROLE HELPERS
# -------------------------------------------------
def is_director(email: str) -> bool:
    return email.strip().lower() in DIRECTOR_EMAILS


def show_registration_gate(email: str):
    st.subheader("Trainer account setup")
    st.write("Your email is signed in, but it is not assigned to a trainer profile yet.")
    st.write("A director can create your trainer profile below.")

    if not is_director(email):
        st.warning("Your email is not linked to a trainer account yet. Contact the PT director.")
        st.stop()

    st.info("Director access detected. Add trainer accounts below.")

    with st.form("create_trainer_account"):
        new_email = st.text_input("Trainer Email")
        trainer_name = st.text_input("Trainer Name")
        club = st.selectbox("Club", CLUBS, key="new_trainer_club")
        submit = st.form_submit_button("Save Trainer Account")

        if submit:
            if not new_email.strip() or not trainer_name.strip():
                st.error("Trainer email and trainer name are required.")
            else:
                try:
                    upsert_trainer_account(new_email, trainer_name, club)
                    st.success("Trainer account saved.")
                    st.rerun()
                except SQLAlchemyError as e:
                    st.error(f"Could not save trainer account: {e}")

    st.write("### Current Trainer Accounts")
    try:
        accounts = get_all_trainer_accounts()
        if accounts.empty:
            st.info("No trainer accounts created yet.")
        else:
            st.dataframe(accounts, use_container_width=True)
    except SQLAlchemyError as e:
        st.error(f"Could not load trainer accounts: {e}")

    st.stop()


# -------------------------------------------------
# UI SECTIONS
# -------------------------------------------------
def render_trainer_view(account: Dict):
    trainer_name = account["trainer_name"]
    club = account["club"]

    st.subheader("Weekly Trainer Submission")
    st.write("Use this once per week for each trainer.")

    col1, col2 = st.columns(2)
    col1.text_input("Trainer Name", value=trainer_name, disabled=True)
    col2.text_input("Club", value=club, disabled=True)

    with st.form("trainer_form", clear_on_submit=True):
        week_start = st.date_input("Week Starting", value=date.today())

        col_a, col_b = st.columns(2)
        with col_a:
            hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5)
            kickoffs_booked = st.number_input("Kickoffs Booked", min_value=0, step=1)
        with col_b:
            kickoffs_completed = st.number_input("Kickoffs Completed", min_value=0, step=1)
            pt_sold = st.number_input("PT Sold ($)", min_value=0.0, step=50.0)

        submit = st.form_submit_button("Submit Weekly Numbers")

        if submit:
            if kickoffs_completed > kickoffs_booked:
                st.error("Kickoffs Completed cannot be greater than Kickoffs Booked.")
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
                except SQLAlchemyError as e:
                    st.error(f"Could not save submission: {e}")

    st.divider()
    st.caption("Trainer score stays hidden from trainers.")


def render_director_dashboard():
    st.subheader("PT Director Dashboard")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["Dashboard", "Scoring Setup", "Trainer Accounts", "Exports"]
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
        st.write("### Add or Update Trainer Accounts")
        with st.form("trainer_accounts_form"):
            new_email = st.text_input("Trainer Email")
            trainer_name = st.text_input("Trainer Name")
            club = st.selectbox("Club", CLUBS, key="trainer_account_club")
            save_trainer = st.form_submit_button("Save Trainer Account")

            if save_trainer:
                if not new_email.strip() or not trainer_name.strip():
                    st.error("Trainer email and trainer name are required.")
                else:
                    try:
                        upsert_trainer_account(new_email, trainer_name, club)
                        st.success("Trainer account saved.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"Could not save trainer account: {e}")

        try:
            accounts = get_all_trainer_accounts()
            if accounts.empty:
                st.info("No trainer accounts yet.")
            else:
                st.dataframe(accounts, use_container_width=True)
        except SQLAlchemyError as e:
            st.error(f"Could not load trainer accounts: {e}")

    with tab4:
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
email = require_login()

try:
    init_db()
except (ValueError, SQLAlchemyError) as e:
    st.error(f"Database connection failed: {e}")
    st.info("Set DATABASE_URL first, then restart the app.")
    st.stop()

account = get_trainer_account(email)

director = is_director(email)

if not account and not director:
    show_registration_gate(email)

st.title("Club 24 PT Score Dashboard")
st.caption("Real Gyms. Real Goals. Real Results.")

if director:
    mode = st.sidebar.radio(
        "Choose view",
        ["Trainer Input", "PT Director Dashboard"],
    )
else:
    mode = "Trainer Input"

if mode == "Trainer Input":
    if account:
        render_trainer_view(account)
    else:
        st.subheader("Director trainer preview unavailable")
        st.info("Your director email is not tied to a trainer account. Add a trainer account if you want to use trainer input under a specific trainer login.")
else:
    render_director_dashboard()
