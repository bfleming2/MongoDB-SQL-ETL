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
conn.execute('''CREATE TABLE engagementTips
        (id varchar(50) PRIMARY KEY,
        motivationId varchar(50),
        engagementType int,
        header varchar(50),
        subHeader varchar(10),
        content varchar(50),
        reasoning varchar(50));''')
print ("engagmentTips Table created successfully")
conn.close()