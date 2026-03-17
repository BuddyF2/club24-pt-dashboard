from datetime import datetime
from zoneinfo import ZoneInfo
import streamlit as st

from abc_api import abc_get

EST = ZoneInfo("America/New_York")

CLUB = st.secrets["ABC_CLUB_NUMBER"]


def convert_to_est(value):

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

    return dt.astimezone(EST)


def fetch_calendar_events():

    payload = abc_get(f"{CLUB}/calendars/events")

    events = []

    for event in payload.get("results", []):

        start = convert_to_est(event["startDateTime"])
        end = convert_to_est(event["endDateTime"])

        events.append({
            "external_event_id": event["eventId"],
            "title": event.get("eventName", "ABC Event"),
            "trainer_name": event.get("employeeName"),
            "trainer_email": event.get("employeeEmail"),
            "date": start.date(),
            "start": start.time(),
            "end": end.time(),
        })

    return events