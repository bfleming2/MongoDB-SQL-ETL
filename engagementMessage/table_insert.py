import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## engagementMessages and messageParams!!!!!
##-----------------------------------------


directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
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

# Clearing the content.db database and then restarting the connection

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
print(query)
conn.execute(query)
conn.execute(queryMessageParams)
conn.commit()
print("EngagmentMessages Values inserted successfully :)")
print("messageParams Values inserted successfully :)")
conn.close()


