import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## USERS!!!!!
##-----------------------------------------


# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
file = "users.json"
file_path = os.path.join(directory, file)
with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()
lines = len(file)


# Sets the file path for the users_clean.json file
file = "users_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file users_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    df = pd.read_json('users.json', lines=True)
    df.to_json('users_clean.json')

# Clearing the content.db database and then restarting the connection
conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM users;")
conn.commit()
conn.close()
conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)
query = "INSERT INTO users (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) != "_id":
        query = query + "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the users table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json
    for key in data:
        # These three columns are distinct objects need to ask Shaul but making them NULL for now
        if str(key) == "_id":
            continue
        elif str(key) == "personsOfInterest":
            sql_code += "\'{"
            for item in data[key][str(index)]:
                sql_code += "\"" + str(item) + "\"" + ", "
            if len(data[key][str(index)]) != 0:
                sql_code = sql_code[0:len(sql_code) - 2]
            sql_code += "}\', "
        elif str(key) == "motivations":
            sql_code += "NULL, "
        else:
            # Any empty value will be labeled as NULL in the table
            if data[key][str(index)] == None or str(data[key][str(index)]) == "NULL" or data[key][str(index)] == "None":
                sql_code += "NULL, "
            else:
                # Some of the values for createdTime variable are nested in a dictionary
                if str(key) == "createdTime":
                    # This will extract the data if it's nested in a dictionary
                    if type(data[key][str(index)]) == type({}):
                        sql_code += "\'" + data[key][str(index)]["$date"] + "\'"
                    # If the data isn't in a dictionary add it normally
                    else:
                        sql_code += "\'" + data[key][str(index)] + "\'"
                else:
                    # Non string objects will be added normally to the query
                    if type (data[key][str(index)]) == type (2) or type (data[key][str(index)]) == type (2.0) or type (data[key][str(index)]) == type(True):
                        sql_code += str(data[key][str(index)])
                    # Need to surround any string in '' so that the execute command treats them like a string
                    else:
                        sql_code += "\'" + str(data[key][str(index)]) + "\'"
                sql_code += ", "
    if index != lines - 1:
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + "),\n"
    else: 
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + ")"
    query += sql_code
query += ";"

conn.execute(query)
conn.commit()
print("User Values inserted successfully :)")
conn.close()


##-----------------------------------------
## RESELLERS!!!!!
##-----------------------------------------

file = "resellers.json"
file_path = os.path.join(directory, file)

with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()

lines = len(file)

# Sets the file path for the resellers_clean.json file
file = "resellers_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file resellers_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    # Change for your files
    df = pd.read_json('External_data/resellers.json', lines=True)
    df.to_json('External_data/resellers_clean.json')

# Clearing the content.db database and then restarting the connection
conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM resellers;")
conn.commit()
conn.close()
conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)
query = "INSERT INTO resellers (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if key != "_id":
        query = query + "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the resellers table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json (NEED TO DO THIS)
    for key in data: 
        if str(key) == "_id": 
            continue
        else:
            # Any empty value will be labeled as NULL in the table
            if data[key][str(index)] == None or str(data[key][str(index)]) == "NULL" or data[key][str(index)] == "None":
                sql_code += "NULL, "
            #adding the rest of the strings to the query
            else:
                sql_code += "\'" + str(data[key][str(index)]) + "\'"
                sql_code += ", "
    #removing last comma from list 
    if index != lines - 1:
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + "),\n"
    else: 
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + ")"
    query += sql_code
query += ";"
conn.execute(query)
conn.commit()
print("Resellers Values inserted successfully :)")
conn.close()

##-----------------------------------------
## MOTIVATIONID!!!!!
##-----------------------------------------

file = "motivations.json"
file_path = os.path.join(directory, file)
with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()
lines = len(file)

# Sets the file path for the users_clean.json file
file = "motivations_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file users_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    # Change for your files
    df = pd.read_json('External_data/motivations.json', lines=True)
    df.to_json('External_data/motivations_clean.json')

# Clearing the content.db database and then restarting the connection
conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM motivationId;")
conn.commit()
conn.close()
conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)
query = "INSERT INTO motivationId (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) == '_id' :
        continue
    query = query + "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the users table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json (NEED TO DO THIS)
    for key in data :
        if str(key) == '_id' :
            continue
        else :
            if data[key][str(index)] == None or str(data[key][str(index)]) == "NULL" or data[key][str(index)] == "None" :
                sql_code += "NULL, "
            else :
                if str(key) == "insights" :
                    sql_code += "\'{"
                    for item in data[key][str(index)]:
                        sql_code += "\"" + str(item) + "\"" + ", "
                    if len(data[key][str(index)]) != 0 :
                        sql_code = sql_code[0:len(sql_code) - 2]
                    sql_code += "}\'"
                else :
                    sql_code += "\'" + str(data[key][str(index)]) + "\'"
            sql_code += ", "
    sql_code = sql_code[0:len(sql_code) - 2] + ")"
    
    if index != lines - 1 :
        sql_code += ",\n"
    query += sql_code 
query += ";"
conn.execute(query)
conn.commit()
print("MotivationId Values inserted successfully :)")
conn.close()

##-----------------------------------------
## EngagmentTips!!!!!!!
##-----------------------------------------

file = "engagementTips.json"
file_path = os.path.join(directory, file)
with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()
lines = len(file)


# Sets the file path for the engagementTips_clean.json file
file = "engagementTips_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file engagementTips_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    df = pd.read_json('engagementTips.json', lines=True)
    df.to_json('engagementTips_clean.json')

# Clearing the content.db database and then restarting the connection
# This is only there if you need to clear the database and want to not import
# duplicates.

conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM engagementTips;")
conn.commit()
conn.close()

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)
query = "INSERT INTO engagementTips (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) != "_id":
        query = query + "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the engagementTips table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json
    for key in data:
        # These three columns are distinct objects need to ask Shaul but making them NULL for now
        if str(key) == "_id":
            continue
        else:
            currentValue = data[key][str(index)]
            # Any empty value will be labeled as NULL in the table
            if currentValue == None or str(currentValue) == "NULL" or currentValue == "None":
                sql_code += "NULL, "
            else:
                # Non string objects will be added normally to the query
                if type (currentValue) == int or type (currentValue) == float or type (currentValue) == bool:
                    sql_code += str(currentValue)
                # Need to surround any string in '' so that the execute command treats them like a string
                else:
                    sql_code += "\'" + str(currentValue) + "\'"
                sql_code += ", "
    if index != lines - 1:
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + "),\n"
    else: 
        # Removes last comma from the list
        sql_code = sql_code[0:len(sql_code) - 2] + ")"
    query += sql_code
query += ";"

conn.execute(query)
conn.commit()
print("engagementTips Values inserted successfully :)")
conn.close()

##-----------------------------------------
## engagementMessages and messageParams!!!!!
##-----------------------------------------
file = "engagementMessages.json"
file_path = os.path.join(directory, file)
with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()
lines = len(file)

# Sets the file path for the users_clean.json file
file = "engagementMessages_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file users_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    # Change for your files
    df = pd.read_json('External_data/engagementMessages.json', lines=True)
    df.to_json('External_data/engagementMessages_clean.json')

# Clearing the content.db databases and then restarting the connection
conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM engagementMessages;")
conn.execute("DELETE FROM messageParams;")
conn.commit()
conn.close()

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)

# query is used to generate the query for engagementMessages table
# queryMessageParams is used to generate the query for messageParams table
query = "INSERT INTO engagementMessages (" 
queryMessageParams = "INSERT INTO messageParams ("

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) == '_id' :
        continue
    elif str(key) == "messageParams":
        query += "messageParamId, "
        queryMessageParams += "messageParamId, "
        for subKeys in data[str(key)]['0']:
            queryMessageParams += str(subKeys) + ", "
    else:
        query += "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"
queryMessageParams = queryMessageParams[0:len(queryMessageParams) - 2] + ") \nVALUES"

messageParamsCounter = 0
# Access each user in the users table with the outer for loop
for index in range(lines):
    sql_code = "("
    sql_code_message = "("
    # Iterates through the clean json (NEED TO DO THIS)
    for key in data :
        currentValue = data[key][str(index)]
        if str(key) == '_id' :
            continue
        else :
            if currentValue == None or str(currentValue) == "NULL" or currentValue == "None" :
                sql_code += "NULL, "
            else :
                if str(key) == "timestamp":
                    # This will extract the data if it's nested in a dictionary
                    if type(currentValue) == dict:
                        sql_code += "\'" + currentValue["$date"] + "\'"
                    # If the data isn't in a dictionary add it normally
                    else:
                        sql_code += "\'" + currentValue + "\'"
                # When we get to the messageParams key we are going to insert the data into the messageParams table
                # Created a new column in both tables to be used as a foreign key. The foreign key is called messageParamId
                elif str(key) == "messageParams":
                    sql_code += str(messageParamsCounter)
                    sql_code_message += str(messageParamsCounter) + ", "
                    for subKeys in currentValue:
                        sql_code_message += "\'" + currentValue[subKeys] + "\'"
                        sql_code_message += ", "
                    messageParamsCounter += 1
                else :
                    if type (currentValue) == int:
                        sql_code += str(currentValue)
                    else:
                        sql_code += "\'" + str(currentValue) + "\'"
            sql_code += ", "
    sql_code = sql_code[0:len(sql_code) - 2] + ")"
    sql_code_message = sql_code_message[0:len(sql_code_message) - 2] + ")"
    if index != lines - 1 :
        sql_code += ",\n"
        sql_code_message += ",\n"
    query += sql_code 
    queryMessageParams += sql_code_message
    
query += ";"
queryMessageParams += ";"
conn.execute(query)
conn.execute(queryMessageParams)
conn.commit()
print("EngagmentMessages Values inserted successfully :)")
print("messageParams Values inserted successfully :)")
conn.close()


