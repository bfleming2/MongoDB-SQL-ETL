import sqlite3 
import os

directory = "C:/Users/Ben Fleming/Desktop/TAMID/Claro_Work/"
file = "content.db"
file_path = os.path.join(directory, file)

if os.path.exists(file_path):
   os.remove(file_path)
with open(file_path, 'w') as f:
        pass

conn = sqlite3.connect("content.db")

conn.execute('''CREATE TABLE users 
        (_id varchar(50),
        id varchar(50) PRIMARY KEY,
        state int,
        accountType int,
        parentId varchar(10),
        firstName varchar(50),
        lastName varchar(50),
        firstName_Eng varchar(50),
        lastName_Eng varchar(50),
        discoveryStatus int,
        discoveryType int,
        orgId varchar(10),
        role int,
        claroRole int,
        gender int,
        locale varchar(10),
        isRTL boolean,
        color varchar(10),
        mailAddress varchar(50),
        createdTime varchar(50),
        presentGuidedTours boolean,
        presentFullHierarchy boolean,
        motivations varchar(50),
        personsOfInterest varchar(50),
        privacyApprovalDate varchar(50),
        engagementLevel int,
        shouldGetQuestioners boolean,
        resellerId varchar(50),
        accessToken varchar(50),
        lastUpdated varchar(50),
        userInputOrg varchar(50),
        userInputRole varchar(50));''')
# Persons of interest and motivations need to be array
# will create new type object for them later
# Need to create few lines to see if content.db is empty
# If it isn't empty clear content.db
# may write .sh file to do it if not easy in python
print ("Table created successfully")

conn.close()