import sqlite3 
import json
import pandas as pd
import os

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

# Ask Shaul his opinion on this?????
# query = query.replace("\'\'", "NULL")
# print(query)

conn.execute(query)
conn.commit()

print("Values inserted successfully :)")

conn.close()





