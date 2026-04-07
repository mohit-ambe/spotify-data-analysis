import sqlite3


DB_FILENAME = 'music.sqlite'


def load(table, data, db=DB_FILENAME):
    schema = str(tuple(data.keys()))
    values = str(tuple(data.values()))
    try:
        with sqlite3.connect(db) as connection:
            cursor = connection.cursor()
            query = f'''INSERT INTO {table} {schema} VALUES {values}'''
            cursor.execute(query)
    except:
        pass
