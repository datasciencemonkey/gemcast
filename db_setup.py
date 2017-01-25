import sqlite3
import os
import pandas as pd

__author__ = 'sgangichetty'

"""This just needs to be done at the beginning of the project set up -
this is just a sample script and this is just an optional requirement.
Ideally you would have your own enterprise implementation"""

"""
Write your data files or forecast csv files into the data directory before running the script
"""

connection = sqlite3.connect('gemcast.db')
cursor = connection.cursor()
dir_path = 'C:\\Users\\sagang\\Google Drive\\gemcast\\data'

# Write all files to the database
for file in os.listdir('data'):
    table_name = file.split(sep='.')[0]
    frame = pd.read_csv(dir_path + '\\' + file)
    frame.to_sql(table_name, connection, index=False, if_exists='replace')
# Read all tables and generate a consolidated table
table_names = [i.split(sep='.')[0] for i in os.listdir('data')]
frames_dict = {i: pd.read_sql("select * from {}".format(table_names[i]), con=connection)
               for i in range(len(table_names))}
full_frame = pd.concat(frames_dict.values(), ignore_index=True)
# Finally write all the files to the database
full_frame.to_sql('l1_forecast_results', connection, index=False, if_exists='replace')
