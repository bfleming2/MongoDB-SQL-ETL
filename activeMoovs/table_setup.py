import sqlite3 
import os

# Joining the file path for the content.db together to see if the file exists
# Change directory to where your mongodb-sql-etl is located
directory = "/Users/mayapollack/Documents/TAMID/Claro"
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
        counterPartId varchar(50),
        moovId varchar(50),
        priority DOUBLE(5),
        startDate varchar(50),
        endDate varchar(50),
        plannedEndDate varchar(50),
        isOverdue BOOL,
        notifiedUserForOverdue BOOL,
        feedbackScore INTEGER(50), 
        feedbackText varchar(50))
;''')

# FOREIGN KEY(userID) REFERENCES users(id)),
# FOREIGN KEY(moovID) REFERENCES moovs(id),
# FOREIGN KEY(counterPart) REFERENCES 
         
         

# We removed events and steps arrays 
# We'll figure out foreign keys after


# Persons of interest and motivations need to be array
# will create new type object for them later
# Need to create few lines to see if content.db is empty
# If it isn't empty clear content.db
# may write .sh file to do it if not easy in python
print ("Table created successfully")

conn.close()
