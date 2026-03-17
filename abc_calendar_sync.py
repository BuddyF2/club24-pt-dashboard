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
    club_number = ABC_CLUBS[club_name]
    payload = abc_get(f"{club_number}/calendars/events")

    events = []
    for event in payload.get("results", []):
        start = convert_to_est(event["startDateTime"])
        end = convert_to_est(event["endDateTime"])

        events.append(
            {
                "external_event_id": str(event.get("eventId", "")),
                "title": event.get("eventName", "ABC Event"),
                "trainer_name": event.get("employeeName", "") or "",
                "trainer_email": event.get("employeeEmail", "") or "",
                "date": start.date(),
                "start": start.time().replace(second=0, microsecond=0),
                "end": end.time().replace(second=0, microsecond=0),
                "club": club_name,
            }
        )

    return events
