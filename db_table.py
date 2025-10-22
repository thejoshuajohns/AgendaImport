# sqlite db communication
import sqlite3

#
# Very basic SQLite wrapper
#
# Creates table from schema
# Provides small set of utility functions to query the database
#
# If you need to change the schema of an already created table, reset the database
# If you need to reset the database, just delete the database file (db_table.DB_NAME)
#
class db_table:

    # SQLite database filename
    DB_NAME = "interview_test.db"

    #
    # model initialization
    # records table name and schema
    # creates the table if it does not exist yet in DB
    #
    # \param name    string                name of the DB table
    # \param schema  dict<string, string>  schema of DB table, mapping column name to their DB type & constraint
    #
    # Example: table("users", { "id": "integer PRIMARY KEY", "name": "text" })
    #
    def __init__(self, name, schema):
        # error handling
        if not name:
            raise RuntimeError("invalid table name")
        if not schema:
            raise RuntimeError("invalid database schema")

        # init fields and initiate database connection
        self.name    = name
        self.schema  = schema
        self.db_conn = sqlite3.connect(self.DB_NAME)
        
        # ensure the table is created
        self.create_table()

    #
    # CREATE TABLE IF NOT EXISTS wrapper
    # Create the database table based on self.name and self.schema
    # If table already exists, nothing is done even if the schema has changed
    # If you need to apply schema changes, please delete the database file
    #
    def create_table(self):
        # { "id": "integer", "name": "text" } -> "id integer, name text"
        columns_query_string = ', '.join([ "%s %s" % (k,v) for k,v in self.schema.items() ])

        # CREATE TABLE IF NOT EXISTS users (id integer PRIMARY KEY, name text)
        #
        # Note that columns are formatted into the string without using sqlite safe substitution mechanism
        # The reason is that sqlite does not provide substitution mechanism for columns parameters
        # In the context of this project, this is fine (no risk of user malicious input)
        self.db_conn.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (self.name, columns_query_string))
        self.db_conn.commit()

    #
    # SELECT wrapper
    # Query the database by applying the specified filters
    #
    # \param columns  array<string>         columns to be fetched. if empty, will query all the columns
    # \param where    dict<string, string>  where filters to be applied. only combine them using AND and only check for strict equality
    #
    # \return [ { col1: val1, col2: val2, col3: val3 } ]
    #
    # Example table.select(["name"], { "id": "42" })
    #         table.select()
    #         table.select(where={ "name": "John" })
    #
    def select(self, columns=None, where=None):
        if columns is None:
            columns = list(self.schema.keys())
        if where is None:
            where = {}

        columns_query_string = ", ".join(columns)
        query = f"SELECT {columns_query_string} FROM {self.name}"
        
        values = []
        if where:
            # Use placeholders to avoid data type errors and SQL injection
            where_query_string = " AND ".join([f"{k} = ?" for k in where.keys()])
            query += f" WHERE {where_query_string}"
            values = tuple(where.values())

        cursor = self.db_conn.cursor()
        cursor.execute(query, values)
        
        results = []
        rows = cursor.fetchall()
        cursor.close()
        
        for row in rows:
            results.append(dict(zip(columns, row)))
            
        return results

    #
    # INSERT INTO wrapper
    # insert the given item into database
    #
    # \param item  dict<string, string>   item to be insert in DB, mapping column to value
    #
    # \return id of the created record
    #
    # Example table.insert({ "id": "42", "name": "John" })
    #
    def insert(self, item):
            columns = item.keys()
            values = tuple(item.values())

            columns_query = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(values))
            
            query = f"INSERT INTO {self.name} ({columns_query}) VALUES ({placeholders})"

            cursor = self.db_conn.cursor()
            
            # The driver will correctly handle integers, strings, and None
            cursor.execute(query, values)
            
            last_id = cursor.lastrowid
            self.db_conn.commit()
            cursor.close()
            return last_id

    #
    # UPDATE wrapper
    # update multiple rows matching the specified condition
    #
    # \param values  dict<string, string>  values to be updates, mapping column to value
    # \param where   dict<string, string>  where filters to be applied. only combine them using AND and only check for strict equality
    #
    # \return number of updated records
    #
    # Example table.update({ "name": "Simon" }, { "id": 42 })
    #
    def update(self, values, where):
        set_placeholders = ", ".join([f"{k} = ?" for k in values.keys()])
        where_placeholders = " AND ".join([f"{k} = ?" for k in where.keys()])
        
        # Combine values for SET and WHERE clauses in the correct order
        params = tuple(values.values()) + tuple(where.values())
        
        query = f"UPDATE {self.name} SET {set_placeholders} WHERE {where_placeholders}"
        
        cursor = self.db_conn.cursor()
        cursor.execute(query, params)
        
        row_count = cursor.rowcount
        self.db_conn.commit()
        cursor.close()
        return row_count

    #
    # Close the database connection
    #
    def close(self):
        self.db_conn.close()
