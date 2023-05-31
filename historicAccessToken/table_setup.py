import sqlite3 
import os

# Joining the file path for the content.db together to see if the file exists
# Change directory to where your mongodb-sql-etl is located
directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl"
file = "content.db"
file_path = os.path.join(directory, file)

# Checking if the file already exists and if it does it will be deleted
if os.path.exists(file_path):
   os.remove(file_path)
with open(file_path, 'w') as f:
        pass

# Opening a connection so we can execute a query on the content.db
conn = sqlite3.connect("content.db")

conn.execute('''CREATE TABLE historicAccessTokens 
        (id varchar(50) PRIMARY KEY,
        tokenBody varchar(50),
        resellerId varchar(50),
        state int,
        createdTimeStamp varchar(50),
        activationTimeStamp varchar(50));''')

print ("accessToken Table created successfully")

conn.close()