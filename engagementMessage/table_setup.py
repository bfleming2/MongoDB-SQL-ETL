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
conn.execute('''CREATE TABLE engagementMessages
        (id varchar(50) PRIMARY KEY,
        templateId varchar(50),
        userId varchar(50),
        type int,
        counterpartId varchar(10),
        timestamp varchar(50),
        distTemplateName varchar(50),
        messageParamId varchar(50));''')
print("engagementMessages Table created successfully")

conn.execute('''CREATE TABLE messageParams
        (messageParamId int PRIMARY KEY,
        userFirstName varchar(50),
        counterpartName varchar(50),
        header varchar(50),
        subHeader varchar(10),
        content varchar(50));''')
print ("messageParams Table created successfully")
conn.close()