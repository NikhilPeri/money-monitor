import sqlite3
import csv
with sqlite3.connect('../sqlite.db') as connection:
    with open("../csv/sqlite_dumps/stocks.csv", "wb") as write_file:
        output = csv.writer(write_file, delimiter='|')
        for row in connection.execute('SELECT * FROM stocks'):
            output.writerow(row)
