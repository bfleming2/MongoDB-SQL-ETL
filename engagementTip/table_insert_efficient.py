import sqlite3 
import json
import pandas as pd
import os


##-----------------------------------------
## MOTIVATIONID!!!!!
##-----------------------------------------


directory = "C:/Users/Ben Fleming/Desktop/TAMID/mongodb-sql-etl/"
directory += "External_data"
file = "engagementTips.json"
file_path = os.path.join(directory, file)
data = {}
def validateJSON(file_path):
    with open(file_path, 'r') as file:
        try:
            json.loads(file)
        except:
            return False
        return True
    
# Access each user in the users table with the outer for loop
def parseJSON (jsonObject):
    sql_code = "("
    # Iterates through the json (NEED TO DO THIS)
    for key in jsonObject:
        if str(key) == '_id' :
            continue
        else :
            if data[key] == None or str(data[key]) == "NULL" or data[key] == "None" :
                sql_code += "NULL, "
            else :
                sql_code += "\'" + str(data[key]) + "\'" + ", "
    sql_code = sql_code[0:len(sql_code) - 2] + ")"
    return (sql_code)

with open(file_path, 'r') as file:
     # Clearing the content.db database and then restarting the connection
    conn = sqlite3.connect("content.db")
    conn.execute("DELETE FROM engagementTips;")
    conn.commit()
    conn.close()
    conn = sqlite3.connect("content.db")

    columnNameData = {}
    # Read in the JSON data from a file
    columnNameData = json.load(file)
    query = "INSERT INTO engagementTips (" 

    # Getting all of the keys so we can set the columns in the Insert INTO statement
    for key in columnNameData:
        if str(key) == '_id' :
            continue
        query = query + "" + key + ", "
    query = query[0:len(query) - 2] + ") \nVALUES"

    if validateJSON(file_path):
        jsonObject = json.load(file)
        query += parseJSON(jsonObject)
        print(query)
    else:
        jsonObject = {}
        for line in file:
            jsonObject = json.loads(line)
            query += parseJSON(jsonObject) + ","
        query = query[0:len(query) - 1]
        query += ");"
        print(query)
        conn.execute(query)
        conn.commit()
        print("MotivationId Values inserted successfully :)")
        conn.close()


