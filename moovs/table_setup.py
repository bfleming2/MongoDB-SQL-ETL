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

conn.execute('''CREATE TABLE moovs 
        (id varchar(50) PRIMARY KEY,
        score int,
        image varchar(50),
        complexity int,
        name varchar(50),
        motivationId varchar(50),
        description varchar(50),
        issueId varchar(50),
        howTo varchar(10),
        contributor varchar(10),
        reasoning varchar(10),
        steps text[],
        conflictId varchar(10));''')
# Persons of interest and motivations need to be array
# will create new type object for them later
# Need to create few lines to see if content.db is empty
# If it isn't empty clear content.db
# may write .sh file to do it if not easy in python
print ("Moovs Table created successfully")

conn.close()