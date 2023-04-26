import sqlite3 
import json
import pandas as pd
import os

# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
directory = "/Users/joshleeman/Downloads/TAMID_Tech_Spring_2023_Claro/mongodb-sql-etl/"
directory += "External_data"
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
conn.execute("DELETE FROM MOTIVATIONS;")
conn.commit()
conn.close()

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)

query = "INSERT INTO MOTIVATIONS (" 

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
    print(sql_code)
    query += sql_code 
query += ";"
# Ask Shaul his opinion on this?????
# query = query.replace("\'\'", "NULL")



conn.execute(query)
conn.commit()

print("Values inserted successfully :)")

conn.close()





