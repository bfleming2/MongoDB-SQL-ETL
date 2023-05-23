import sqlite3 

conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM users;")
conn.execute("DELETE FROM moovs;")
conn.execute("DELETE FROM moovs;")
conn.execute("DELETE FROM moovs;")
conn.execute("DELETE FROM moovs;")
conn.execute("DELETE FROM moovs;")
conn.execute("DELETE FROM moovs;")
conn.commit()
conn.close()