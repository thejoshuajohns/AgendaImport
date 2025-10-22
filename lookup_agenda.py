#!/usr/bin/env python3

import sys
from db_table import db_table

# database schemas
SESSIONS_SCHEMA = {
    "id": "integer PRIMARY KEY AUTOINCREMENT",
    "data": "text",
    "time_start": "text",
    "time_end": "text",
    "session_type": "text",  # 'Session' or 'Sub-session'
    "title": "text",
    "location": "text",
    "description": "text",
    "parent_id": "integer"  # foreign key connecting sub-sessions to parent
}

SPEAKERS_SCHEMA = {
    "id": "integer PRIMARY KEY AUTOINCREMENT",
    "name": "text NOT NULL UNIQUE"
}

SESSION_SPEAKERS_SCHEMA = {
    "session_id": "integer",
    "speaker_id": "integer"
}

VALID_COLUMNS = [
    "date", 
    "time_start", 
    "time_end", 
    "title", 
    "location", 
    "description", 
    "speaker"
]

def main():
    if len(sys.argv) != 3:
        print("Error: Invalid number of arguments")
        sys.exit(1)

    lookup_column = sys.argv[1]
    lookup_value = sys.argv[2]

    if lookup_column not in VALID_COLUMNS:
        print(f"Error: Invalid lookup column '{lookup_column}'")
        sys.exit(1)

    sessions_table = None
    speakers_table = None
    session_speakers_table = None

    try:
        sessions_table = db_table("sessions", SESSIONS_SCHEMA)
        speakers_table = db_table("speakers", SPEAKERS_SCHEMA)
        session_speakers_table = db_table("session_speakers", SESSION_SPEAKERS_SCHEMA)
        print(f"Looking up sessions where {lookup_column} = '{lookup_value}'...\n")
        print("-" * 40)

        results = []
        if not results:
            print("No sessions found matching the criteria.")
        else:
            pass
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if sessions_table:
            sessions_table.close()
        if speakers_table:
            speakers_table.close()
        if session_speakers_table:
            session_speakers_table.close()