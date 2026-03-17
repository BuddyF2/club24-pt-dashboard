# abc_calendar_sync.py

from datetime import datetime
from zoneinfo import ZoneInfo

from abc_api import abc_get

EST = ZoneInfo("America/New_York")

ABC_CLUBS = {
    "Torrington": "9556",
    "Wallingford": "9557",
    "Brookfield": "9558",
    "Ridgefield": "9559",
    "Newtown": "9560",
    "Middletown": "9561",
    "New Milford": "9562",
}


def convert_to_est(value: str):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(EST)


def fetch_calendar_events(club_name: str):
    if club_name not in ABC_CLUBS:
        raise ValueError(f"Unknown ABC club name: {club_name}")

    club_number = ABC_CLUBS[club_name]
    payload = abc_get(f"{club_number}/calendars/events")

    if isinstance(payload, list):
        raw_events = payload
    elif isinstance(payload, dict):
        raw_events = (
            payload.get("results")
            or payload.get("items")
            or payload.get("data")
            or payload.get("events")
            or []
        )
    else:
        raw_events = []

    events = []

    for event in raw_events:
        start_raw = (
            event.get("startDateTime")
            or event.get("start_datetime")
            or event.get("startTime")
            or event.get("start")
        )
        end_raw = (
            event.get("endDateTime")
            or event.get("end_datetime")
            or event.get("endTime")
            or event.get("end")
        )

        if not start_raw or not end_raw:
            continue

        try:
            start = convert_to_est(str(start_raw))
            end = convert_to_est(str(end_raw))
        except Exception:
            continue

        external_event_id = (
            event.get("eventId")
            or event.get("id")
            or event.get("calendarEventId")
            or ""
        )

        title = (
            event.get("eventName")
            or event.get("name")
            or event.get("title")
            or "ABC Event"
        )

        trainer_name = (
            event.get("employeeName")
            or event.get("trainerName")
            or event.get("staffName")
            or ""
        )

        trainer_email = (
            event.get("employeeEmail")
            or event.get("trainerEmail")
            or event.get("staffEmail")
            or ""
        )

        notes = (
            event.get("notes")
            or event.get("description")
            or "Imported from ABC"
        )

        events.append(
            {
                "external_event_id": str(external_event_id),
                "title": str(title),
                "trainer_name": str(trainer_name),
                "trainer_email": str(trainer_email),
                "date": start.date(),
                "start": start.time().replace(second=0, microsecond=0),
                "end": end.time().replace(second=0, microsecond=0),
                "club": club_name,
                "notes": str(notes),
            }
        )

    return events
