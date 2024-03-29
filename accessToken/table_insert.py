import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## accessTokens!!!!!
##-----------------------------------------


# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
file = "accessTokens.json"
file_path = os.path.join(directory, file)

with open(file_path, 'rb') as f:
    data = f.read()
file = data.decode('utf-8').splitlines()

lines = len(file)


# Sets the file path for the accessTokens_clean.json file
file = "accessTokens_clean.json"
file_path = os.path.join(directory, file)

# Checking to see if the file accessTokens_clean.json exists so it isn't remade
if not os.path.exists(file_path):
    # Cleans the files
    df = pd.read_json('External_data/accessTokens.json', lines=True)
    df.to_json('External_data/accessTokens_clean.json')

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)

query = "INSERT INTO accessTokens (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    if str(key) != "_id":
        query = query + "" + key + ", "

query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the accessTokens table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json
    for key in data:
        # These three columns are distinct objects need to ask Shaul but making them NULL for now
        if str(key) == "_id":
            continue
        else:
            # Any empty value will be labeled as NULL in the table
            if data[key][str(index)] == None or str(data[key][str(index)]) == "NULL" or data[key][str(index)] == "None":
                sql_code += "NULL, "
            else:
                # Non string objects will be added normally to the query
                if type (data[key][str(index)]) == int or type (data[key][str(index)]) == float or type (data[key][str(index)]) == bool:
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

print("Access Token Values inserted successfully :)")

conn.close()


