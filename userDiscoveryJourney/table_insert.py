import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## MOTIVATIONID!!!!!
##-----------------------------------------


directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
file = "userDiscoveryJourney.json"
file_path = os.path.join(directory, file)
data = {}
with open(file_path, 'rb') as f:
    try:
        data = json.load(f)
        print(data)
    except ValueError as e:
        for line in f:
            data = json.load(line)
            print(data)

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


