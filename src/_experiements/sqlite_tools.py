import sqlite3

def create_sqlite_example_file(location):
    conn = sqlite3.connect(location)
    c.execute('''CREATE TABLE IF NOT EXISTS person (name text, phone text, birthyear real)''')

    for _ in range(0,1000):
        c.execute("INSERT INTO person VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()