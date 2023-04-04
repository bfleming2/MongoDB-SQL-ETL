import sqlite3 
import json
import pandas as pd
import os

# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
directory = "/Users/rohanpandit/mongodb-sql-etl/"
directory += "External_data"
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
    df = pd.read_json('resellers.json', lines=True)
    df.to_json('resellers_clean.json')

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
print(query)

# Ask Shaul his opinion on this?????
# query = query.replace("\'\'", "NULL")
# print(query)

#conn.execute(query)
#conn.commit()

print("Values inserted successfully :)")

conn.close()





