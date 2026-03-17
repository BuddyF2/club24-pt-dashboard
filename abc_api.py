import requests
import streamlit as st

BASE_URL = st.secrets["ABC_BASE_URL"]
APP_ID = st.secrets["ABC_APPLICATION_ID"]
APP_KEY = st.secrets["ABC_APPLICATION_KEY"]

HEADERS = {
    "Accept": "application/json",
    "app_id": APP_ID,
    "app_key": APP_KEY
}


def abc_get(endpoint, params=None):

    url = f"{BASE_URL}/{endpoint.lstrip('/')}"

    response = requests.get(
        url,
        headers=HEADERS,
        params=params or {},
        timeout=30
    )

    response.raise_for_status()

    return response.json()