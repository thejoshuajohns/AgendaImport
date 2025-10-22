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