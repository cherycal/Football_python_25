__author__ = 'chance'

import inspect
import time
import urllib.request
from datetime import datetime
import os
import random
from os import path

from modules import push
import json
import requests


class Request(object):

    def __init__(self, sleep_interval=None):
        if sleep_interval is not None:
            print(f"Request:__init__: sleep interval set to {sleep_interval}")
        self.sleep_interval = sleep_interval
        # ESPN_BASE = 'https://fantasy.espn.com/apis/v3/games/flb/seasons/'
        ESPN_BASE = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/'
        STATCAST_BASE = 'https://baseballsavant.mlb.com'
        DEFAULT_YEAR = datetime.now().strftime("%Y")
        DEFAULT_URL = f"{ESPN_BASE}{str(DEFAULT_YEAR)}/segments/0/leaguedefaults/3?view=kona_player_info"
        DEFAULT_OUTPUT_FILE = "./data/ESPNdata.json"
        self._x = None
        self.TIMEOUT = 10
        self._BASE = ESPN_BASE
        self.STATCAST_BASE = STATCAST_BASE
        self._DEFAULT_URL = DEFAULT_URL
        self._year = DEFAULT_YEAR
        self._default_filters = {
            "players": {"limit": 5000, "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "PPR"}}}
        self._filters = {
            "players": {"limit": 5000, "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "PPR"}}}
        self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
        self._DEFAULT_OUTPUT_FILE = DEFAULT_OUTPUT_FILE
        self.cookies = {'espn_s2': os.environ["espn_s2"], 'SWID': os.environ["SWID"]}
        self.push_instance = push.Push(calling_function="requestor")
        self.manual_mode = False
        #  self.push_instance.logger_instance.info(f"\nInitialized Request object with default url: {self._DEFAULT_URL}\n")

    @property
    def filters(self):
        return self._filters

    def DEFAULT_URL(self):
        print("Default url: " + str(self._DEFAULT_URL))
        return self._DEFAULT_URL

    def ESPN_BASE(self):
        # print("Default base: " + str(self._BASE))
        return self._BASE

    def set_limit(self, value):
        self._filters["players"]["limit"] = value
        self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
        # print("Limit changed to: " + str(value))
        # print(f"filters: {self._filters}")

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, value):
        self._year = value

    @year.getter
    def year(self):
        return self._year

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, value):
        # print("setter of x called")
        self._x = value

    @x.deleter
    def x(self):
        # print("deleter of x called")
        del self._x

    # c = C()
    # c.x = 'foo'  # setter called
    # foo = c.x    # getter called
    # del c.x      # deleter called

    def urlopen(self, print_flag=False, url=None):
        if print_flag:
            self.push_instance.logger_instance.debug(f"requestor urlopen: {url}")
        with urllib.request.urlopen(url, timeout=self.TIMEOUT) as url:
            json_object = json.loads(url.read().decode())
            return json_object

    def make_request(self, write=False, print_flag=True, url=None, input_file=None, headers=None,
                     output_file=None, filters=None, calling_function=None, sleep_int=None):

        url = url or self.DEFAULT_URL()
        output_file = output_file or self._DEFAULT_OUTPUT_FILE
        if filters:
            self._filters = filters
            # print(f"Set filters: {self._filters}")
        else:
            self._filters = self._default_filters
        # print(f"Filters: {self._filters}")
        if headers is None:
            self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
            headers = self._headers
        if print_flag:
            # print(f"headers: {self._headers}")
            print(f"make_request from {calling_function}, using url: {url}")
            # print(f"cookies: {self.cookies}")

        self.push_instance.logger_instance.info(f"requestor: {url} fn {calling_function}")
        pass

        try:
            if self.manual_mode:
                file_name = input_file
                self.push_instance.logger_instance.info(f"requestor text file {input_file} fn {calling_function}")
                if path.exists(file_name):
                    with open(file_name, mode='r') as inp:
                        resp = json.load(inp) or None
            else:
                r = requests.get(url, headers=headers, cookies=self.cookies)
                if r:
                    resp = r.json()
                else:
                    resp = None

            if write:
                with open(output_file, 'w') as outfile:
                    json.dump(resp, outfile)
                    self.push_instance.logger_instance.info("Printed result to " + output_file)

            # if print_flag:
            #     print(f"Resp: {resp}")

        except Exception as ex:
            print(f"Error in make_request(url={url}): {ex}")
            print(f"inspect.stack(): {inspect.stack()}")
            try:
                self.push_instance.logger_instance.critical(f"Error in make_request(url={url}): {ex}")
                self.push_instance.push(f"Error in make_request(url={url}): {ex}")
            except Exception as push_exception:
                print(f"Error in make_request:Exception:push (url={url}): {push_exception}")

        if sleep_int is None and self.sleep_interval is None:
            sleep_int = random.randint(4, 15)
        print(f"requestor:make_request sleep for {sleep_int} at {datetime.now().strftime('%H%M%S')}")
        time.sleep(sleep_int)

        return resp
