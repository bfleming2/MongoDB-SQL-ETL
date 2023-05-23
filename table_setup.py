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

##-----------------------------------------
## Users!!!!!!!
##-----------------------------------------
conn.execute('''CREATE TABLE users 
        (id varchar(50) PRIMARY KEY,
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
        personsOfInterest text[],
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
print ("Users table created successfully")

##-----------------------------------------
## Resellers!!!!!!!
##-----------------------------------------
conn.execute('''CREATE TABLE resellers 
        (id String, 
        firstName String, 
        lastName String, 
        mailAddress String, 
        userId String, 
        createdTimeStamp String, 
        description String);''')
print ("Resellers created successfully")

##-----------------------------------------
## Motivations!!!!!!!
##-----------------------------------------
conn.execute('''CREATE TABLE motivationId
        (id varchar(50) PRIMARY KEY,
        name varchar(50),
        shortDescription varchar(50),
        longDescription varchar(50),
        longDescriptionPlural varchar(10),
        additionalData varchar(50),
        imageUrl varchar(50),
        color varchar(50),
        tailResolution varchar(50),
        insights text[]);''')
print ("MotivationId Table created successfully")

##-----------------------------------------
## EngagmentTips!!!!!!!
##-----------------------------------------
conn.execute('''CREATE TABLE engagementTips
        (id varchar(50) PRIMARY KEY,
        motivationId varchar(50),
        engagementType int,
        header varchar(50),
        subHeader varchar(10),
        content varchar(50),
        reasoning varchar(50));''')
print ("engagmentTips Table created successfully")

##-----------------------------------------
## EngagmentMessages and MessageParams!!!!!!!
##-----------------------------------------
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
##-----------------------------------------
## Moovs!!!!!!!
##-----------------------------------------
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
print ("Moovs Table created successfully")
conn.close()