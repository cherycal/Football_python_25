__author__ = 'chance'

import sys
import time

sys.path.append('../modules')
sys.path.append('..')
import sqldb


def run(fdb):
	try:
		data = fdb.select_plus("Select * from testCMR")
		print(data)
	except Exception as ex:
		print(f"Exception: {ex}")
		time.sleep(60)


def main():
	fdb = sqldb.DB('Football.db')
	while True:
		run(fdb)
		time.sleep(5)
	# print(data['column_names'])
	# for row in data['rows']:
	# 	print(row)
	fdb.close()


if __name__ == "__main__":
	main()
