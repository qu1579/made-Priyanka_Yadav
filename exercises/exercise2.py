import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *
import re
import sys

# Fetch the CSV data from the provided URL
data_set_url='https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV'

# Read CSV data into a pandas DataFrame
df = pd.read_csv(data_set_url, delimiter=';')

# Drop the "Status" column
df = df.drop(columns=['Status'])

def is_valid_ifopt(value):
    # Regular expression pattern for IFOPT validation
    pattern = re.compile(r"^\w{2}:\d+:\d+(?::\d+)?$")
    return bool(pattern.match(str(value)))

# Apply data validation and filtering
df['Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
df['Breite'] = df['Breite'].str.replace(',', '.').astype(float)
df = df[ (df['Laenge'] <= 90) & (df['Laenge'] >= -90)]
df = df[ (df['Breite'] <= 90) & (df['Breite'] >= -90)]
df = df[df['Verkehr'].isin(['FV', 'nur DPN', 'RV'])]
df = df[df['IFOPT'].apply(is_valid_ifopt)]
df = df.dropna()  # Drop rows with empty cells

# print(df['Laenge'])
# sys.exit()
# Define column data types
columns_type={
    "EVA_NR": Integer,
    "DS100": Text,
    "IFOPT": Text,
    "NAME": Text,
    "Verkehr": Text,
    "Laenge": Float,
    "Breite": Float,
    "Betreiber_Name": Text,
    "Betreiber_Nr": Integer,
}

# Connect to SQLite database
# conn = sqlite3.connect('exercises/trainstops.sqlite')
conn=create_engine("sqlite:///trainstops.sqlite", echo=True)

# Write the DataFrame to SQLite database
df.to_sql('trainstops', conn, if_exists='replace',index=False,dtype=columns_type)

# Close the database connection
print("DONE")
print("Updated database is trainstopsn.sqlite and table is trainstops")

#triger exercise grading