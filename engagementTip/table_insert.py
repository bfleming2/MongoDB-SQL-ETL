import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## EngagmentTips!!!!!!!
##-----------------------------------------


# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
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

print(query)
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