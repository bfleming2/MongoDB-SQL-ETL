import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## activeMoovs and messageParams!!!!!
##-----------------------------------------


directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
file = "activeMoovs.json"
file_path = os.path.join(directory, file)
with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()
lines = len(file)

# Sets the file path for the users_clean.json file
file = "activeMoovs_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file users_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    # Change for your files
    df = pd.read_json('External_data/activeMoovs.json', lines=True)
    df.to_json('External_data/activeMoovs_clean.json')

# Clearing the content.db database and then restarting the connection

conn = sqlite3.connect("content.db")
conn.execute("DELETE FROM activeMoovs;")
conn.execute("DELETE FROM activeMoovsEvents;")
conn.execute("DELETE FROM activeMoovsSteps;")
conn.commit()
conn.close()

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)

# query is used to generate the query for activeMoovs table
# queryMessageParams is used to generate the query for messageParams table
query = "INSERT INTO activeMoovs ("
queryEvent = "INSERT INTO activeMoovsEvents (id," 
queryStep = "INSERT INTO activeMoovsSteps ("
idCount = 0
# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) == '_id' :
        continue
    elif str(key) == "events":
        query += "eventTimeStamp, "
        for subKeys in data[str(key)]['0'][0]:
            queryEvent += str(subKeys) + ", "
    elif str(key) == "steps":
        query += "stepId, "
        for subKeys in data[str(key)]['0'][0]:
            queryStep += str(subKeys) + ", "
    else:
        query += "" + key + ", "
query = query[0:len(query) - 2] + ") \nVALUES"
queryEvent = queryEvent[0:len(queryEvent) - 2] + ") \nVALUES"
queryStep = queryStep[0:len(queryStep) - 2] + ") \nVALUES"

sql_code_event = ""
sql_code_step = ""
# Access each user in the users table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the clean json (NEED TO DO THIS)
    for key in data :
        currentValue = data[key][str(index)]
        if str(key) == '_id' :
            continue
        else :
            if currentValue == None or str(currentValue) == "NULL" or currentValue == "None" :
                sql_code += "NULL, "
            else :
                if str(key) == "startDate":
                    # This will extract the data if it's nested in a dictionary
                    if type(currentValue) == dict:
                        sql_code += "\'" + currentValue["$date"] + "\'"
                    # If the data isn't in a dictionary add it normally
                    else:
                        sql_code += "\'" + currentValue + "\'"
                elif str(key) == "endDate":
                    # This will extract the data if it's nested in a dictionary
                    if type(currentValue) == dict:
                        sql_code += "\'" + currentValue["$date"] + "\'"
                    # If the data isn't in a dictionary add it normally
                    else:
                        sql_code += "\'" + currentValue + "\'"
                elif str(key) == "plannedEndDate":
                    # This will extract the data if it's nested in a dictionary
                    if type(currentValue) == dict:
                        sql_code += "\'" + currentValue["$date"] + "\'"
                    # If the data isn't in a dictionary add it normally
                    else:
                        sql_code += "\'" + currentValue + "\'"
                # When we get to the messageParams key we are going to insert the data into the messageParams table
                # Created a new column in both tables to be used as a foreign key. The foreign key is called messageParamId
                elif str(key) == "events":
                    sql_code += "\'{" + ""
                    for item in data[key][str(index)]:
                        sql_code += "\"" + item["timeStamp"] + "\"" + ", "
                        sql_code_event += "(" + str(idCount) +", "
                        for subKeys in item:
                            if type (item[subKeys]) == int or type(item[subKeys]) == float or type(item[subKeys]) == bool:
                                sql_code_event += str(item[subKeys]) + ", "
                            else:
                                sql_code_event += "\'" + str(item[subKeys]) + "\'" + ", "
                        sql_code_event = sql_code_event[0:len(sql_code_event) - 2] + "),"
                        idCount += 1
                    sql_code = sql_code[0:len(sql_code) - 2]
                    sql_code += "}\'"
                elif str(key) == "steps":
                    sql_code += "\'{" + ""
                    for item in data[key][str(index)]:
                        sql_code += "\"" + item["id"] + "\"" + ", "
                        sql_code_step += "("
                        for subKeys in item:
                            if type (item[subKeys]) == int or type(item[subKeys]) == float or type(item[subKeys]) == bool:
                                sql_code_step += str(item[subKeys]) + ", "
                            else:
                                sql_code_step += "\'" + str(item[subKeys]) + "\'" + ", "
                        sql_code_step = sql_code_step[0:len(sql_code_step) - 2] + "),"
                    sql_code = sql_code[0:len(sql_code) - 2]
                    sql_code += "}\'"
                else :
                    if type (currentValue) == int or type(currentValue) == float or type(currentValue) == bool:
                        sql_code += str(currentValue)
                    else:
                        sql_code += "\'" + str(currentValue) + "\'"
            sql_code += ", "
    sql_code = sql_code[0:len(sql_code) - 2] + ")"
    if index != lines - 1 :
        sql_code += ",\n"
    query += sql_code 
queryEvent += sql_code_event
queryStep += sql_code_step
queryEvent = queryEvent[0:len(queryEvent) - 1]
queryStep = queryStep[0:len(queryStep) - 1]
query += ";"
queryEvent += ";"
queryStep += ";"
conn.execute(query)
conn.execute(queryEvent)
conn.execute(queryStep)
conn.commit()
print("activeMoovs Values inserted successfully :)")
print("activeMoovsEvents Values inserted successfully :)")
print("activeMoovsSteps Values inserted successfully :)")
conn.close()


