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

# print results in a readable format
def print_results(results):
    if not results:
        print("No sessions found matching the criteria.")
        return
    for session in results:
        print(f"Title: {session.get('title')}\n"
              f"Type: {session.get('session_type')}\n"
              f"Speakers:    {session.get('speakers', 'N/A')}\n"
              f"Location: {session.get('location')}\n"
              f"Time: {session.get('time_start')} - {session.get('time_end')}\n"
              f"Description: {session.get('description', 'N/A')[:70]}...\n"
              + "-" * 40
              )

def find_sessions(sessions_table, speakers_table, session_speakers_table, lookup_column, lookup_value):
    all_sessions = sessions_table.select()
    sessions_by_id = {s["id"]: s for s in all_sessions}

    matched_ids = set()
    # handle speaker lookup separately
    if lookup_column == "speaker":
        all_speakers = speakers_table.select()
        speaker_ids = [s['id'] for s in all_speakers if lookup_value.lower() in s['name'].lower()]
        
        if not speaker_ids:
            return []
        # find all sessions linked to these speaker IDs
        speaker_session_ids = set()
        for speaker_id in speaker_ids:
            links = session_speakers_table.select(["session_id"], {"speaker_id": speaker_id})
            for link in links:
                speaker_session_ids.add(link['session_id'])
        # include parent sessions if a sub-session matches
        for session_id in speaker_session_ids:
            matched_ids.add(session_id)
            session = sessions_by_id.get(session_id)
            if session and session["parent_id"] is None:
                for s in all_sessions:
                    if s["parent_id"] == session_id:
                        matched_ids.add(s["id"])
    # handle other columns
    else:
        search_col = "data" if lookup_column == "date" else lookup_column
        
        exact_match_cols = ["data", "time_start", "time_end"]
        
        # search through sessions
        for session in all_sessions:
            session_value = str(session.get(search_col, ""))
            
            # determine which comparison to use
            match = False
            if search_col in exact_match_cols:
                match = (session_value == lookup_value)
            else:
                match = (lookup_value.lower() in session_value.lower())

            # check for direct match
            if match:
                matched_ids.add(session["id"])
                if session["parent_id"] is None:
                    for s in all_sessions:
                        if s["parent_id"] == session["id"]:
                            matched_ids.add(s["id"])

            # check for parent session match
            elif session["parent_id"] is not None:
                parent = sessions_by_id.get(session["parent_id"])
                if parent:
                    parent_value = str(parent.get(search_col, ""))
                    parent_match = False
                    if search_col in exact_match_cols:
                        parent_match = (parent_value == lookup_value)
                    else:
                        parent_match = (lookup_value.lower() in parent_value.lower())
                    
                    if parent_match:
                        matched_ids.add(session["id"])

    # compile final results
    final_results = [sessions_by_id[id] for id in matched_ids if id in sessions_by_id]

    for session in final_results:
        links = session_speakers_table.select(['speaker_id'], {'session_id': session['id']})
        speaker_names = []
        if links:
            for link in links:
                speaker_data = speakers_table.select(['name'], {'id': link['speaker_id']})
                if speaker_data:
                    speaker_names.append(speaker_data[0]['name'])
        session['speakers'] = '; '.join(speaker_names) if speaker_names else 'N/A'

    final_results.sort(key=lambda x: (x.get("data", ""), x.get("time_start", "")))
    return final_results

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

    # initialize database tables
    try:
        sessions_table = db_table("sessions", SESSIONS_SCHEMA)
        speakers_table = db_table("speakers", SPEAKERS_SCHEMA)
        session_speakers_table = db_table("session_speakers", SESSION_SPEAKERS_SCHEMA)

        print(f"Looking up sessions where {lookup_column} = '{lookup_value}'...\n")
        print("-" * 40)

        results = find_sessions(
            sessions_table,
            speakers_table,
            session_speakers_table,
            lookup_column,
            lookup_value
        )

        print_results(results)

    # cleanup
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if sessions_table:
            sessions_table.close()
        if speakers_table:
            speakers_table.close()
        if session_speakers_table:
            session_speakers_table.close()

if __name__ == "__main__":
    main()