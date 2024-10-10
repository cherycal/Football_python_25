__author__ = 'chance'

import sys
sys.path.append('./modules')
sys.path.append('..')

from modules.player_stats import Stats
from odds import Odds


def main():
	# process = Stats()
	Stats().start(threaded=True, sleep_interval=60*60*8)
	Odds().run_odds()

	# process.table_snapshot(table_name="PlayerDashboard", snap_name="PDSnap")


if __name__ == "__main__":
	main()
