import sqlite3 
import json
import pandas as pd
import os

# Counts the total lines in a file
# Change the directory to where the mongodb-sql-etl directory is located
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
conn.commit()
conn.close()

conn = sqlite3.connect("content.db")

data = {}
# Read in the JSON data from a file
with open(file_path, 'r') as f:
    data = json.load(f)

query = "INSERT INTO activeMoovs (" 

# Getting all of the keys so we can set the columns in the Insert INTO statement
for key in data:
    query = query + "" + key + ", "

query = query[0:len(query) - 2] + ") \nVALUES"

# Access each user in the users table with the outer for loop
for index in range(lines):
    sql_code = "("
    # Iterates through the json (NEED TO DO THIS)

# Ask Shaul his opinion on this?????
# query = query.replace("\'\'", "NULL")
# print(query)

conn.execute(query)
conn.commit()

print("Values inserted successfully :)")

conn.close()





