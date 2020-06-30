import sqlite3
import csv
import os
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(
    description='''
    This program is connected with two other files:
    table_projects.csv and table_tasks.csv.
    To start you need to write down the path and database
    file anywhere you want.      
''')
parser.add_argument('-path', type=str, help='Path and DB file')

args = parser.parse_args()

path = args.path
if os.path.exists(path):
    os.remove(path)

possible_errors = ''
conn = sqlite3.connect(path)
cursor = conn.cursor()
cursor.execute("CREATE TABLE projects ("
               "Name TEXT,"
               "Description TEXT,"
               "Deadline DATE"
               ");")


cursor.execute("CREATE TABLE tasks ("
               "Id INTEGER UNIQUE,"
               "Priority INTEGER,"
               "Details TEXT,"
               "Status TEXT NOT NULL,"
               "Deadline DATETIME,"
               "Completed DATETIME,"
               "Project TEXT NOT NULL)")

with open('table_projects.csv') as pr:
    db = csv.DictReader(pr)
    insert_to_pr = [(i['Name'], i['Description'], i['Deadline'])
                    for i in db]

cursor.executemany("INSERT INTO projects VALUES (?, ?, ?)",
                   insert_to_pr)


with open('table_tasks.csv') as tsk:
    db = csv.DictReader(tsk)

    data_from_csv = [[row['Priority'], row['Details'], row['Status'],
                      row['Deadline'], row['Completed'], row['Project']]
                     for row in db]

    insert_to_tsk = []
    for row in data_from_csv:
        for i in range(len(row)):
            if len(row[i]) == 0:
                row[i] = None
        insert_to_tsk.append(row)

    for i in range(len(insert_to_tsk)):
        insert_to_tsk[i] = [i + 1] + insert_to_tsk[i]

    set_of_rows_with_possible_errors = list()
    for row in range(len(insert_to_tsk)):
        if insert_to_tsk[row][3].lower() not in (
                'done', 'pending', 'new', 'cancelled'):
            possible_errors += f'Logic Error in row {row} (Status)\n'
            if row not in set_of_rows_with_possible_errors:
                set_of_rows_with_possible_errors.append(row)
        try:
            int(insert_to_tsk[row][1]) == int(float(insert_to_tsk[row][1]))
        except ValueError:
            possible_errors += f'Logic Error in row {row+1} (Priority is not integer)\n'
            if row not in set_of_rows_with_possible_errors:
                set_of_rows_with_possible_errors.append(row)
        if insert_to_tsk[row][3].lower() != 'done' \
                and insert_to_tsk[row][5] is not None:
            possible_errors += f'Logic Error in row {row+1} (Completed)\n'
            if row not in set_of_rows_with_possible_errors:
                set_of_rows_with_possible_errors.append(row)
        if insert_to_tsk[row][3].lower() == 'done' \
                and insert_to_tsk[row][5] is None:
            possible_errors += f'Logic Error in row {row+1} (Completed)\n'
            if row not in set_of_rows_with_possible_errors:
                set_of_rows_with_possible_errors.append(row)
        if insert_to_tsk[row][5] is not None:
            if datetime.now() < datetime(int(insert_to_tsk[row][5].split('/')[2]),
                              int(insert_to_tsk[row][5].split('/')[0]),
                              int(insert_to_tsk[row][5].split('/')[1])):
                possible_errors += f'Logic Error in row {row+1} (Completed)\n'
                if row not in set_of_rows_with_possible_errors:
                    set_of_rows_with_possible_errors.append(row)
        if int(insert_to_tsk[row][1]) not in range(1,6):
            possible_errors += f'Logic Error in row {row+1} (Incorrect priority)\n'
            if row not in set_of_rows_with_possible_errors:
                set_of_rows_with_possible_errors.append(row)
set_of_rows_with_possible_errors.sort(reverse=True)
for i in set_of_rows_with_possible_errors:
    del insert_to_tsk[i]

cursor.executemany("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?)",
                  insert_to_tsk)

project_name = input("Choose project name: ")

query = """
SELECT * FROM tasks
WHERE Project = '{}' AND
Priority > 3
""".format(project_name)

cursor.execute(query)

for row in cursor.fetchall():
    print(row)
if len(possible_errors) > 0:
    print(possible_errors.strip())
conn.commit()
conn.close()
