__author__ = 'chance'

import sys

sys.path.append('../modules')

from bs4 import BeautifulSoup as bs
import tools
import time
import push

sleep_interval = 10
# Selenium
driver = tools.get_driver("headless")

msg = ""

inst = push.Push()

def get_page(data):
	[name, home, league, team] = data
	url = "https://fantasy.espn.com/football/boxscore?leagueId="+str(league)+"&matchupPeriodId=12&seasonId=2021&teamId="+str(team)
	print("url is: " + url)

	driver.get(url)
	time.sleep(sleep_interval)
	html = driver.page_source
	soup = bs(html, "html.parser")
	results = soup.find_all('div',{"class": ["statusLabel"]})
	away_score = str(results[2].text).split(':')[1]
	home_score = str(results[6].text).split(':')[1]
	margin = home * round((float(away_score) - float(home_score)),2)

	push_str = f'{name}: {margin} \n'
	print(push_str)
	#inst.push(push_str,"")
	return push_str


def main():
	push_str = ""
	while True:
		push_str += get_page(["Chik",-1.00,499031754,2])
		time.sleep(2)
		push_str += get_page(["Rule", 1.00,484773671, 9])
		time.sleep(2)
		push_str += get_page(["Auto", -1.00, 1109140158, 8])
		time.sleep(2)
		push_str += get_page(["Mega", 1.00, 1221578721, 1])
		inst.push(push_str, "")
		print(push_str)
		push_str = ""
		time.sleep(10)
	driver.close()


if __name__ == "__main__":
	main()
