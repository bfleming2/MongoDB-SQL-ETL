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

conn.execute('''CREATE TABLE activeMoovs 
        (id varchar(50) PRIMARY KEY,
         userID varchar(50), 
         counterpartId varchar(50),
         moovId varchar(50),
         priority DOUBLE(5),
         startDate varchar(50),
         endDate varchar(50),
         plannedEndDate varchar(50),
         isOverdue varchar(50),
         notifiedUserForOverdue varchar(50),
         feedbackScore DOUBLE(5),
         feedbackText varchar(50),
         eventTimeStamp text[],
         stepId text[]);''')
print ("activeMoovs Table created successfully")
conn.execute('''CREATE TABLE activeMoovsEvents 
        (id int PRIMARY KEY,
         timeStamp varchar(50),
         type int, 
         content varchar(50),
         additionalText varchar(50),
         score int,
         additionalNumericData int);''')
print ("activeMoovsEvents Table created successfully")
conn.execute('''CREATE TABLE activeMoovsSteps
        (id varchar(50) PRIMARY KEY,
         idx int, 
         state int,
         comment varchar(50));''')
print ("activeMoovsSteps Table created successfully")
# Persons of interest and motivations need to be array
# will create new type object for them later
# Need to create few lines to see if content.db is empty
# If it isn't empty clear content.db
# may write .sh file to do it if not easy in python
print ("Table created successfully")

conn.close()