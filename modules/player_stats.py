__author__ = 'chance'

import datetime
import threading
from typing import List

import time
from modules import requestor, sqldb, push, tools, odds
import csv
import pandas as pd

from dictdiffer import diff
from modules.scoreboard import Scoreboard


def lineup_slot_map(slot_id: str) -> str:
    position_names = {
        '0': 'QB',
        '1': 'QB',
        '2': 'RB',
        '3': 'WR',
        '4': 'WR',
        '5': 'WR',
        '16': 'D',
        '17': 'K',
        '20': 'B',
        '21': 'IR',
        '23': 'FLEX',
        '6': 'TE'
    }
    return position_names.get(slot_id)


def sleep_countdown(sleep_interval: int) -> None:
    print(f"League process sleep countdown: ", end='')
    while sleep_interval > 0:
        if sleep_interval % 10 == 0:
            print(f"{sleep_interval} ", end='')
        time.sleep(1)
        sleep_interval -= 1


class Stats:
    # New year procedure: just update DEFAULT_LEAGUE_ID to any valid league id
    def __init__(self, season: int = int((datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y'))):
        self._SEASON: int = season
        self.DEFAULT_LEAGUE_ID: str = "1929067716"
        self.request_instance = requestor.Request()
        self.request_instance.year = self._SEASON
        self.SPORT_ID: str = "ffl"  # mlb: flb, nfl: ffl
        self.API_BASE: str = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/{self.SPORT_ID}/seasons"
        self.REFRESH: bool = True
        self.END_OF_DAY = 2200
        self.push_instance = push.Push(calling_function="FBInfo")
        self.logname = './logs/statslog.log'
        self.logger = push.get_logger(logfilename=self.logname)
        self.DB = sqldb.DB('Football.db')
        # self.lineup_slot_map = self.lineup_slot_map()
        self.original_rosters = None
        self.new_rosters = None
        self.threaded = True
        self.gamedays = [0, 3, 6]
        self._gameday = True if datetime.datetime.now().weekday() in self.gamedays else False
        self.positional_team_rankings_url = (f"{self.API_BASE}/{self.SEASON}/segments/0/"
                                             f"leagues/{self.DEFAULT_LEAGUE_ID}?view=mPositionalRatingsStats")
        self.odds = odds

    def __repr__(self):
        return f"Stats object: Season: {self.SEASON}\n"

    def __str__(self):
        return f"Stats object: Season: {self.SEASON}, DB:{self.DB}\n"

    @property
    def gameday(self):
        return self._gameday

    @gameday.getter
    def gameday(self):
        return self._gameday

    @property
    def SEASON(self):
        return self._SEASON

    @SEASON.getter
    def SEASON(self):
        return self._SEASON

    @SEASON.setter
    def SEASON(self, value: bool):
        self.logger.info(f"Set SEASON to {value}")
        self._SEASON = value

    def get_leagues(self):
        return self.DB.query(f"select leagueId, leagueAbbr, Year from Leagues where Year = {self.SEASON}")

    def get_player_stats(self):
        year = self.SEASON
        ###############
        LIMIT = 1500
        ##############
        position_names = {
            '0': 'QB',
            '1': 'QB',
            '2': 'RB',
            '3': 'WR',
            '4': 'TE',
            '5': 'TE',
            '16': 'D',
            '17': 'K',
            '20': 'B',
            '21': 'IR',
            '23': '23'
        }
        ##############
        player_stats = {}
        self.request_instance.set_limit(LIMIT)
        url = f"{self.API_BASE}/{year}/segments/0/leaguedefaults/3?view=kona_player_info"
        player_data = self.request_instance.make_request(url=url,
                                                         output_file=f"../data/ESPNPlayerStats.json",
                                                         write=False, calling_function="get_player_stats")
        try:
            players = player_data['players']
            for player in players:
                # print(player['player']['fullName'])
                player_name = player['player']['fullName']
                player_id = str(player['player']['id'])
                player_team = player['player']['proTeamId']
                player_status = player['player'].get('injuryStatus', "")
                player_positions = player['player'].get('eligibleSlots', [''])
                player_position = position_names.get(str(player_positions[0]), '')
                if player_position == '':
                    player_position = position_names.get(str(player_positions[1]), '')
                # roster_status = player.get('status', "")
                ownership = player['player'].get('ownership', False)
                percentChange = ""
                percentOwned = ""
                percentStarted = ""
                if ownership:
                    percentChange = player['player']['ownership']['percentChange']
                    percentOwned = player['player']['ownership']['percentOwned']
                    percentStarted = player['player']['ownership']['percentStarted']
                if player_stats.get(player_id) is None:
                    player_stats[player_id] = {}
                    player_stats[player_id]['info'] = {}
                    player_stats[player_id]['stats'] = {}
                    player_stats[player_id]['info']['id'] = player_id
                    player_stats[player_id]['info']['name'] = player_name
                    player_stats[player_id]['info']['proTeam'] = player_team
                    player_stats[player_id]['info']['injuryStatus'] = player_status
                    # player_stats[player_id]['info']['rosterStatus'] = roster_status
                    player_stats[player_id]['info']['percentChange'] = percentChange
                    player_stats[player_id]['info']['percentOwned'] = percentOwned
                    player_stats[player_id]['info']['percentStarted'] = percentStarted
                    player_stats[player_id]['info']['position'] = player_position
                    # player_stats[player_id]['proj'] = [0 for i in range(WEEKS)]
                    # player_stats[player_id]['act'] = [0 for i in range(WEEKS)]
                    player_stats[player_id]['stats']['proj'] = {}
                    player_stats[player_id]['stats']['act'] = {}

                stats = player['player'].get('stats', [])
                for stat in stats:
                    if stat['statSplitTypeId'] == 1 and stat['seasonId'] == year:
                        week = str(stat['scoringPeriodId'])
                        total = round(stat['appliedTotal'], 2)
                        if stat['statSourceId'] == 0:
                            player_stats[player_id]['stats']['act'][week] = total
                        else:
                            player_stats[player_id]['stats']['proj'][week] = total
        except Exception as ex:
            self.logger.error(f"error in get_player_stats: {ex}")
            self.push_instance.push(title="Error", body=f"error in get_player_stats: {ex}")

        return player_stats

    def get_team_schedules(self):
        url = f"{self.API_BASE}/{self.SEASON}?view=proTeamSchedules_wl"
        team_schedules = self.request_instance.make_request(url=url, calling_function="get_team_schedules")
        return team_schedules

    def get_positional_team_rankings(self):
        url = self.positional_team_rankings_url
        positional_team_rankings = self.request_instance.make_request(url=url,
                                                                      calling_function="get_positional_team_rankings")
        return positional_team_rankings

    def get_rosters(self, league_id):
        url = f"{self.API_BASE}/{self.SEASON}/segments/0/" \
              f"leagues/{league_id}?view=mRoster&view=mSettings&view=mTeam&view=modular&view=mNavs"
        rosters = self.request_instance.make_request(url=url, calling_function="get_rosters")
        return rosters

    def get_league_player_availability(self, league, scoring_period):
        league_availability = dict()
        league_availability['league'] = league
        league_availability['players'] = dict()
        url = f"{self.API_BASE}/{self.SEASON}/segments/0/leagues/{str(league)}" \
              f"?scoringPeriodId={str(scoring_period)}&view=kona_player_info"
        lg_filters = {"players": {
            "filterSlotIds": {"value": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 23, 24]},
            "filterRanksForScoringPeriodIds": {"value": [scoring_period]}, "limit": 100,
            "offset": 0, "sortPercOwned": {"sortAsc": False, "sortPriority": 1},
            "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "STANDARD"},
            "filterRanksForRankTypes": {"value": ["PPR"]},
            "filterRanksForSlotIds": {"value": [0, 2, 4, 6, 17, 16]},
            "filterStatsForTopScoringPeriodIds":
                {"value": 2, "additionalValue": ["002021", "102021", "002020", "11202114", "022021"]}}}
        data = self.request_instance.make_request(url=url, filters=lg_filters,
                                                  calling_function="get_league_player_availability")
        players = data['players']
        for player in players:
            player_id = str(player['id'])
            status = player['status']
            league_availability['players'][player_id] = status

        return league_availability

    def get_leaguewide_data(self):
        data = {'db': self.DB, 'year': self.SEASON,
                'team_schedules': self.get_team_schedules(),
                'player_stats': self.get_player_stats(),
                'positional_team_rankings': self.get_positional_team_rankings()}
        return data

    def get_league_data(self, league_id, league_name):
        data = dict()
        data['db'] = self.DB
        data['year'] = self.SEASON
        data['team_schedules'] = self.get_team_schedules()
        data['player_stats'] = self.get_player_stats()
        data['positional_team_rankings'] = self.get_positional_team_rankings()
        data['rosters'] = self.get_rosters(league_id)
        data['league_id'] = league_id
        data['league_name'] = league_name
        return data

    def roster_dict(self) -> dict:
        # _roster_dict = dict()
        # roster_query = (f"select id||'_'||coalesce(injuryStatus,'NA')||'_'||"
        #                 f"lineup_slot||'_'||league||'_'||team_abbrev as key, "
        #                 f"name, id, injuryStatus, lineup_slot, league, team_abbrev from PlayerRosters "
        #                 f"where key is not NULL")
        roster_query: str = (f"select id||'_'||league||'_'||team_id as key, "
                             f"name, id, injuryStatus, lineup_slot, league, team_id, team_abbrev from PlayerRosters "
                             f"where key is not NULL")
        print(f"roster_dict: roster_query: {roster_query}")
        rows = self.DB.query(roster_query)
        # print(f"roster_dict: roster_query: {rows} rows found")
        return {str(row['key']): row for row in rows}
        # for row in rows:
        #     key = str(row['key'])
        #     _roster_dict[key] = row
        # return _roster_dict

    def slack_thread(self) -> None:
        slack_instance = push.Push(calling_function="FBStatsSlack")
        while True:
            update_time: str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            # slack_instance.logger_instance.warning(f"Slack_thread read_slack.")
            slack_text: str = slack_instance.read_slack()
            if slack_text != "":
                slack_instance.logger_instance.info(f"Slack text ({update_time}):{slack_text}.")
                slack_instance.push(f"Received slack request: {slack_text}")
                self.process_slack_text(slack_text)
            time.sleep(5)

    def tables_to_html(self, tables: List[str]):
        for table in tables:
            self.DB.table_to_html(table)
            print(f"Saved table {table} to html")

    def process_slack_text(self, text: str) -> None:
        print(f"process_slack_text: {text}")
        if text.upper()[0:2] == "T:":
            table: str = text[2:]
            print(f"{text[2:]}")
            self.push_instance.push(title="Table to web", body=f'Table to web: {table}')
            try:
                fdb = sqldb.DB('Football.db')
                fdb.table_to_html(table)
                fdb.close()
            except Exception as ex:
                self.push_instance.push(title="Table to web error", body=f'Table to web error: {ex}')
                self.logger.error(f"Exception in process_slack_text: {ex}")

    def process_transactions(self, transactions):
        update_time: str = (datetime.datetime.now().strftime("%Y%m%d %#I:%M") +
                            datetime.datetime.now().strftime('%p'))
        if len(transactions) > 400:
            print(f"Transactions count {len(transactions)}"
                  f" is too large - skipping ")
            self.push_instance.push(title="Transaction count",
                                    body=f"Transaction count is too long "
                                         f"({len(transactions)} entries) - skipping ")
        for transaction in transactions:
            # msg: str = ""
            # title: str = ""
            if transaction.get('type') == 'change':
                (transaction_key, transaction_attribute) = transaction['key'].split('.')
                title = transaction_attribute
                (transaction_from, transaction_to) = transaction['details']
                roster_spot = self.new_rosters.get(transaction_key)

                # don't report players on opposing teams that were on the watch list last week
                # do report players on opposing teams that are on the watch list this week
                if transaction_attribute == 'team_abbrev' and transaction_to[-1] != '*':
                    continue

                msg = (f"{roster_spot.get('name')} {transaction_attribute} CHANGE"
                       f"\n\tFROM: {transaction_from} TO: {transaction_to}"
                       f"\n\tTeam: {roster_spot.get('team_abbrev')} League: {roster_spot.get('league')}"
                       f"\n\tSlot: {roster_spot.get('lineup_slot')} Status: {roster_spot.get('injuryStatus')}"
                       f"\n\tat {update_time}")
                self.push_instance.push(title=title, body=f'{msg}\n\n', print_it=True)
            else:
                for transaction_details in transaction.get('details'):
                    transaction_detail = transaction_details[1]
                    if transaction.get('type') == 'remove':
                        ttype = "DROP"
                    else:
                        ttype = "ADD"
                    title = "ADD/DROP"
                    msg = (f"{ttype}: {transaction_detail.get('name')} "
                           f"\n\tTeam: {transaction_detail.get('team_abbrev')}"
                           f"\n\tLg: {transaction_detail.get('league')}"
                           f"\n\tSlot: {transaction_detail.get('lineup_slot')} "
                           f"Status: {transaction_detail.get('injuryStatus')}"
                           f"\n\tReported at {update_time}")
                    # print(msg)
                    self.push_instance.push(title=f"{title}", body=f'{msg}\n\n', print_it=True)
        return

    def write_rosters(self, data):
        update_time = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
        file_name = '../data/roster_data_file.csv'
        table_name = "Rosters"
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['league', 'team_name', 'team_id', 'team_abbrev', 'player_id',
                                 'lineup_slot', 'year', 'update_time'])
            teams = data['rosters'][0]['teams']
            for team in teams:
                team_name = team['name']
                team_id = team['id']
                team_abbrev = team['abbrev']
                players = team['roster']['entries']
                for player in players:
                    player_id = player['playerPoolEntry']['id']
                    # print(player['lineupSlotId'])
                    lineup_slot = lineup_slot_map(str(player['lineupSlotId']))
                    # print([league, team_name, team_id, team_abbrev, player_id])
                    csv_writer.writerow([data['league_name'], team_name, team_id, team_abbrev,
                                         player_id, lineup_slot, self.SEASON, update_time])

        df = pd.read_csv(file_name)
        # print(df)

        delcmd = f"delete from {table_name} where league = '{data['league_name']}'"
        # print(delcmd)
        try:
            self.DB.delete(delcmd)
        except Exception as ex:
            self.push_instance.push(title="Info", body=f'Exception in {delcmd}: {ex}')
            print(f"Exception in {delcmd}: {ex}")
            self.DB.reset()

        try:
            df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)
        except Exception as ex:
            print(f"Exception in df.to_sql: {ex}")
            self.DB.reset()

        return 0

    def diff_rosters(self, new_rosters, original_rosters):
        roster_diffs = list(diff(original_rosters, new_rosters))
        transaction_headers = ['type', 'key', 'details']
        transactions = [{h: i for (h, i) in zip(transaction_headers, item)} for item in roster_diffs]
        diff_count: int = 0
        for transaction in transactions:
            if transaction.get('type') == 'change':
                diff_count += 1
            else:
                diff_count += len(transaction.get('details'))
        print(f"Number of roster differences: {diff_count}\n")
        self.push_instance.push(title="Transaction count",
                                body=f"Transaction count is {diff_count}")
        if new_rosters and original_rosters:
            self.process_transactions(transactions)
        else:
            print(f"new_rosters len:{len(new_rosters)} or "
                  f"original_rosters len:{len(original_rosters)} is empty")

    def write_player_info(self, data) -> int:
        file_name: str = '../data/player_data_file.csv'
        table_name: str = "PlayerInfo"

        is_header: bool = True
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            for player in data['player_stats']:
                player_info = data['player_stats'][player]['info']
                if is_header:
                    header = player_info.keys()
                    csv_writer.writerow(header)
                    is_header = False
                csv_writer.writerow(player_info.values())
        df = pd.read_csv(file_name)
        df = df.assign(year=self.SEASON)
        # print(df)

        delcmd: str = f"delete from {table_name} where Year = {data['year']}"
        self.DB.delete(delcmd)
        self.DB.df_to_sql(df, table_name, register=False)

        return 0

    def write_player_stats(self, data=None) -> int:
        if data is None:
            data = dict()
            data['player_stats'] = self.get_player_stats()
        file_name = "../data/player_stats_file.csv"
        table_name = "PlayerStats"
        leagueId = 0
        # is_header = True
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['id', 'name', 'week', 'proj', 'act', 'leagueId', 'year'])
            for player in data['player_stats']:
                player_id = data['player_stats'][player]['info']['id']
                name = data['player_stats'][player]['info']['name']
                stats = data['player_stats'][player]['stats']
                for week in stats['proj']:
                    proj = stats['proj'].get(week, "")
                    act = stats['act'].get(week, "")
                    # print([player_id, name, week, proj, act, leagueId, data['year']])
                    csv_writer.writerow([player_id, name, week, proj, act, leagueId, self.SEASON])
        df = pd.read_csv(file_name)
        # print(df)

        if df.size > 0:
            delcmd = f"delete from {table_name} where Year = {self.SEASON}"
            self.DB.delete(delcmd)
            df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)

        self.logger.info(f"Wrote table {table_name}")

        return 0

    def write_team_schedules(self, data):
        file_name = '../data/team_file.csv'
        table_name = "TeamSchedules"
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            teams = data['team_schedules']['settings']['proTeams']
            csv_writer.writerow(['team_id', 'team_name', 'away_team', 'home_team', 'game_id',
                                 'game_week', 'game_date', 'year'])
            for team in teams:
                team_id = team['id']
                team_name = team['abbrev']
                if team.get('proGamesByScoringPeriod'):
                    team_games = team['proGamesByScoringPeriod']
                    for game in team_games:
                        away_team = team_games[game][0]['awayProTeamId']
                        home_team = team_games[game][0]['homeProTeamId']
                        game_id = team_games[game][0]['id']
                        game_week = team_games[game][0]['scoringPeriodId']
                        game_date = team_games[game][0]['date']
                        csv_writer.writerow([team_id, team_name, away_team, home_team, game_id,
                                             game_week, game_date, data['year']])
                        # print([team_id,team_name,away_team,home_team,game_id,game_week,game_date])

        df = pd.read_csv(file_name)
        # print(df)

        try:
            delcmd = f"delete from {table_name}"
            self.DB.delete(delcmd)
            df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)
            self.logger.info("Refreshed team_schedules")
        except Exception as ex:
            # self.push_instance.push(title="Info", body=f'Exception: {ex}')
            print(f"Exception in refresh {table_name}: {ex}")
            self.DB.reset()

        return 0

    def write_positional_team_rankings(self, data):
        position_names = {
            '1': 'QB',
            '2': 'RB',
            '3': 'WR',
            '4': 'TE',
            '5': 'K',
            '16': 'D'
        }
        team_stats = dict()
        weekly_stats = dict()
        pro_teams = data['team_schedules']['settings']['proTeams']
        team_names = dict()
        for team in pro_teams:
            team_names[str(team['id'])] = team['abbrev']
        positional_rankings = list()
        if data['positional_team_rankings'].get('positionAgainstOpponent') and \
                data['positional_team_rankings']['positionAgainstOpponent'].get('positionalRatings'):
            positional_rankings = data['positional_team_rankings']['positionAgainstOpponent']['positionalRatings']
        else:
            # self.push_instance.push(title="Info", body=f'Exception: {ex}')
            self.logger.warning(f"positional_rankings not found using url{self.positional_team_rankings_url}")
            print(f"positional_rankings not found using url{self.positional_team_rankings_url}")
        for position in positional_rankings:
            position_name = position_names[position]
            teams = positional_rankings[position].get('ratingsByOpponent', "?")
            for team in teams:
                average = teams[team]['average']
                rank = teams[team]['rank']
                weeks = teams[team]['stats']
                if weekly_stats.get(team_names[team]) is None:
                    weekly_stats[team_names[team]] = dict()
                if weekly_stats[team_names[team]].get(position_name) is None:
                    weekly_stats[team_names[team]][position_name] = dict()
                for week in weeks:
                    weekly_stats[team_names[team]][position_name][str(week['scoringPeriodId'])] = week['appliedTotal']
                    weekly_stats[team_names[team]]['id'] = str(team)
                if team_stats.get(team_names[team]) is None:
                    team_stats[team_names[team]] = dict()
                if team_stats[team_names[team]].get(position_name) is None:
                    team_stats[team_names[team]][position_name] = dict()
                team_stats[team_names[team]][position_name]['average'] = average
                team_stats[team_names[team]][position_name]['rank'] = rank
                team_stats[team_names[team]][position_name]['weeklyStats'] = weekly_stats
                team_stats[team_names[team]][position_name]['id'] = str(team)

        print_it = True
        if print_it:
            file_name = '../data/ranking_file.csv'
            table_name = "TeamRankings"
            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(['team_name', 'team_id', 'position', 'rank', 'average', 'year'])
                for pos in ['QB', 'RB', 'WR', 'TE', 'D', 'K']:
                    for team_name in team_stats:
                        # print([team_name,team,pos,str(team_stats[team_name][pos]['rank']),
                        # str(team_stats[team_name][pos]['average'])])
                        csv_writer.writerow([team_name, team_stats[team_name][pos]['id'], pos,
                                             str(team_stats[team_name][pos]['rank']),
                                             str(team_stats[team_name][pos]['average']), data['year']])

            df = pd.read_csv(file_name)
            # print(df)

            try:
                delcmd = "delete from " + table_name
                self.DB.delete(delcmd)
                df.to_sql(table_name, data['db'].conn, if_exists='append', index=False)
            except Exception as ex:
                print(f"Exception in refresh {table_name}: {ex}")
                self.DB.reset()

            file_name = '../data/weekly_file.csv'
            table_name = "TeamWeeklyStats"
            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(['team_name', 'team_id', 'position', 'week', 'total', 'year'])
                for pos in ['QB', 'RB', 'WR', 'TE', 'D', 'K']:
                    for team_name in weekly_stats:
                        for week in weekly_stats[team_name][pos]:
                            # print([team_name,team_name,pos,week,weekly_stats[team_name][pos][week]])
                            csv_writer.writerow([team_name, weekly_stats[team_name]['id'],
                                                 pos, week, weekly_stats[team_name][pos][week], data['year']])

            df = pd.read_csv(file_name)
            # print(df)

            try:
                delcmd = f"delete from {table_name}"
                self.DB.delete(delcmd)
                df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)
            except Exception as ex:
                print(f"Exception in refresh {table_name}: {ex}")
                self.DB.reset()

            self.logger.info("Wrote positional_team_rankings")

        return 0

    def write_league_availability(self, availability, league_name):
        file_name = '../data/availability_file.csv'
        table_name = "LeagueAvailability"
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            # league = availability['league']
            csv_writer.writerow(['league', 'player_id', 'availability'])
            for player in availability['players']:
                csv_writer.writerow([league_name, player, availability['players'][player]])

        df = pd.read_csv(file_name)
        # print(df)

        try:
            delcmd = f"delete from {table_name} where league = '{league_name}'"
            self.DB.delete(delcmd)
            df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)
        except Exception as ex:
            print(f"Exception in refresh {table_name}: {ex}")
            self.DB.reset()

        return 0

    def table_snapshot(self, table_name: str = "", snap_name: str = ""):
        if table_name == "" or snap_name == "":
            "Must provide a table_name and a snapshot name"
            exit(-1)
        else:
            try:
                self.DB.cmd(f"drop table {snap_name}")
                self.DB.cmd(f"CREATE TABLE {snap_name} AS SELECT * FROM {table_name}")
            except Exception as ex:
                print(f"Exception found in table_snapshot: {ex}")
        print(f"Successfully saved {table_name} to {snap_name}")
        return 0

    def scoreboard_thread(self):
        if self.gameday:
            scores = Scoreboard(main_loop_sleep=600)
        else:
            scores = Scoreboard(main_loop_sleep=7200)
        scores.start()

    def process_league(self, league):
        # Get data from web or flat files
        league_id = league['leagueID']
        league_name = league['leagueAbbr']
        self.logger.info(f"Get league data for {league_name}:")
        league_data = self.get_league_data(league_id, league_name)

        # Write data to DB
        self.write_team_schedules(league_data)
        self.write_positional_team_rankings(league_data)
        self.write_player_info(league_data)
        self.write_player_stats(league_data)

        league_data['rosters'] = self.get_rosters(league_id),
        self.write_rosters(league_data)

        #####
        availability = self.get_league_player_availability(f"{league_id}", 6)
        self.write_league_availability(availability, league_name)

        #####
        self.logger.info(f"League {league_name} processed\n")

    def run_snaps(self):
        self.table_snapshot(table_name="PlayerDashboard", snap_name="PDSnap")
        self.table_snapshot(table_name="FutureDash", snap_name="FDSnap")
        self.table_snapshot(table_name="PlayerFullScheduleStats", snap_name="PFSSnap")

    def run_leagues(self, threaded=True, sleep_interval=120):
        self.DB = sqldb.DB('Football.db')
        leagues = self.get_leagues()
        print(f"run_leagues run scores thread is set to {threaded}")
        print(f"run_leagues sleep interval is set to {sleep_interval}")
        while True:
            self.original_rosters = self.roster_dict().copy()
            try:
                [self.process_league(league) for league in leagues]
            except Exception as ex:
                self.logger.error(f"ERROR in run_leagues: {ex}")
                self.push_instance.push(f"ERROR in run_leagues: {ex}")
            self.new_rosters = self.roster_dict().copy()
            self.diff_rosters(self.new_rosters, self.original_rosters)
            self.odds.run()
            self.table_snapshot(table_name="PlayerDashboard", snap_name="PDSnap")
            self.table_snapshot(table_name="FutureDash", snap_name="FDSnap")
            self.table_snapshot(table_name="PlayerFullScheduleStats", snap_name="PFSSnap")
            self.tables_to_html(['CWOTV','PlayerByWeek'])
            current_time = int(datetime.datetime.now().strftime("%H%M"))
            print(f"current time is {current_time}")
            if current_time > self.END_OF_DAY:
                print("End of day")
                exit(0)
            step = 600
            print(f"Sleep for {sleep_interval} seconds: ", end='')
            count = 0
            for i in range(0, sleep_interval, step):
                if count % 20 == 0:
                    print("")
                print(f"I{sleep_interval - i} ", end='')
                count += 1
                time.sleep(step)

    def start(self, threaded=True, sleep_interval=60 * 60 * 24):
        self.threaded = threaded
        if self.threaded:
            process_league_thread = (
                threading.Thread(target=self.run_leagues, kwargs={'sleep_interval': sleep_interval}))
            scores_thread = threading.Thread(target=self.scoreboard_thread)
            read_slack_thread = threading.Thread(target=self.slack_thread)
            process_league_thread.start()
            scores_thread.start()
            read_slack_thread.start()
        else:
            self.run_leagues(threaded=False, sleep_interval=sleep_interval)


@tools.connection_check
def run(threaded=False, sleep_interval=60 * 60 * 8):
    Stats().start(threaded=threaded, sleep_interval=sleep_interval)


def run_snaps():
    Stats().run_snaps()


def main():
    Stats().start(threaded=False)


if __name__ == "__main__":
    main()
