#!/usr/bin/env python3

import sys
import xlrd
from db_table import db_table
import os

# database schemas
SESSIONS_SCHEMA = {
    "id": "integer PRIMARY KEY AUTOINCREMENT",
    "data": "text",
    "time_start": "text",
    "time_end": "text",
    "session_type": "text", # this can either be 'Session' or 'Sub-session'
    "title": "text",
    "location": "text",
    "description": "text",
    "parent_id": "integer" # a foreign key that connects sub-sessions to their parent session
}

SPEAKERS_SCHEMA = {
    "id": "integer PRIMARY KEY AUTOINCREMENT",
    "name": "text NOT NULL UNIQUE"
}

# this table connects sessions to speakers (many-to-many relationship)
SESSION_SPEAKERS_SCHEMA = {
    "session_id": "integer",
    "speaker_id": "integer"
}

def import_agenda(filename):
    # initialize database tables
    sessions_table = db_table("sessions", SESSIONS_SCHEMA)
    speakers_table = db_table("speakers", SPEAKERS_SCHEMA)
    session_speakers_table = db_table("session_speakers", SESSION_SPEAKERS_SCHEMA)

    # try to open the Excel file and catch main errors
    try:
        workbook = xlrd.open_workbook(filename)
        sheet = workbook.sheet_by_index(0)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return
    except xlrd.XLRDError:
        print(f"Error: Unable to read the Excel file '{filename}'.")
        return
    
    last_session_id = None

    # idx 14 is where the agenda starts
    for i in range(14, sheet.nrows):
        row = sheet.row_values(i)
        date, time_start, time_end, session_type, title, location, description, speakers = row
        if not any(row):
            continue  # skip empty rows

        session_data = {
            "data": date,
            "time_start": time_start,
            "time_end": time_end,
            "session_type": session_type,
            "title": title,
            "location": location,
            "description": description,
            "parent_id": None
        }

        # sanitize inputs to prevent SQL injection
        sanitized_data = {
                    k: v.replace("'", "''") if isinstance(v, str) else v
                    for k, v in session_data.items()
        }

        # handle sessions and sub-sessions
        current_session_id = None
        if session_type == "Session":
            sanitized_data["parent_id"] = None
            current_session_id = sessions_table.insert(sanitized_data)
            last_session_id = current_session_id
        elif session_type == "Sub-session":
            if last_session_id is None:
                print(f"Error: Sub-session '{title}' found without a parent session.")
                continue
            sanitized_data["parent_id"] = last_session_id
            current_session_id = sessions_table.insert(sanitized_data)

        # handle speakers
        if current_session_id and speakers:
            speakers = [s.strip() for s in speakers.replace(';', ",").split(",") if s.strip()]
            for speaker_name in speakers:
                existing_speaker = speakers_table.select(["id"], {"name": speaker_name})
                if existing_speaker:
                    speaker_id = existing_speaker[0]["id"]
                else:
                    sanitized_name = speaker_name.replace("'", "''")
                    speaker_id = speakers_table.insert({"name": sanitized_name})

                session_speakers_table.insert({"session_id": current_session_id, "speaker_id": speaker_id})

    # close database connections
    sessions_table.close()
    speakers_table.close()
    session_speakers_table.close()
    print("Agenda import completed successfully.")




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./import_agenda.py <path_to_agenda.xls>")
        sys.exit(1)

    agenda_file = sys.argv[1]
    
    if os.path.exists(db_table.DB_NAME):
        os.remove(db_table.DB_NAME)
        print(f"Removed existing database: {db_table.DB_NAME}")

    import_agenda(agenda_file)