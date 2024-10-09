import sys
sys.path.append('../modules')
import sqldb

fdb = sqldb.DB('Football.db')

fdb.table_to_html("PlayerDashboard")
