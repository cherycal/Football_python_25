__author__ = 'chance'
import sys
sys.path.append('../modules')
import sqldb

# My python class: sqldb.py

mode = "PROD"

if mode == "TEST":
    bdb = sqldb.DB('BaseballTest.db')
else:
    bdb = sqldb.DB('Baseball.db')

# DB location: ('C:\\Ubuntu\\Shared\\data\\Baseball.db')


# names,c = bdb.select_w_cols("SELECT * FROM ESPNLeagues")
# print(names)
# for t in c:
#     print(t)


data = bdb.select_table("ACheck_ID")

print(data['column_names'])
for row in data['rows']:
    print(row)

bdb.close()
