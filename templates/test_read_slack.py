__author__ = 'chance'

import datetime
import sys
sys.path.append('../modules')

# from bs4 import BeautifulSoup as bs
import push
import time
import threading
sleep_interval = 10

def slack_thread():
	push_instance = push.Push()
	while True:
		update_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
		slack_text = push_instance.read_slack()
		print(f"Slack text ({update_time}):{slack_text}.")
		time.sleep(5)

def check_rosters():
	while True:
		print("Check rosters")
		time.sleep(20)


def main():
	# slack_thread()
	# check_rosters()
	read_slack = threading.Thread(target=slack_thread)
	rosters = threading.Thread(target=check_rosters)
	read_slack.start()
	rosters.start()


if __name__ == "__main__":
	main()
