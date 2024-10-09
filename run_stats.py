__author__ = 'chance'

import sys
sys.path.append('./modules')
sys.path.append('..')

from player_stats import Stats


def main():
	process = Stats()
	process.start(threaded=False, sleep_interval=60*60*8)
	# process.table_snapshot(table_name="PlayerDashboard", snap_name="PDSnap")


if __name__ == "__main__":
	main()
