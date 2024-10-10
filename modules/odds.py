__author__ = 'chance'

import csv
import datetime
import sys

sys.path.append('./modules')
sys.path.append('..')

from modules import sqldb, requestor
import pandas as pd


class Odds:
    # New year procedure: just update DEFAULT_LEAGUE_ID to any valid league id
    def __init__(self, season=int((datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y'))):
        self._SEASON = season
        self.DB = sqldb.DB('Football.db')
        self.request_instance = requestor.Request()
        self.update_time = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")

    def game_odds(self, game):
        file_name = './data/odds.csv'
        table_name = "Odds"
        url = str(f"https://sports.core.api.espn.com/v2/sports/football/"
                  f"leagues/nfl/events/{game}/competitions/{game}/odds")
        odds_data = self.request_instance.make_request(url=url, output_file=f"./data/ESPNOdds.json",
                                                       write=True, calling_function="game_odds")
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            # creating a csv writer object
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['gameid', 'line', 'spread', 'OU', 'favorite',
                                 'provider', 'update_time'])
            for odds_quote in odds_data.get('items'):
                provider = None
                if odds_quote.get('provider'):
                    provider = odds_quote.get('provider').get('name', "?")
                favorite = "?"
                if odds_quote.get('awayTeamOdds'):
                    if odds_quote.get('awayTeamOdds').get('favorite') is True:
                        favorite = "Away"
                    else:
                        favorite = "Home"
                if favorite != "?":
                    line = str(odds_quote.get('details'))
                    fav, spd = line.split(" ")
                    if len(fav) == 2:
                        fav = f"{fav[0]} {fav[1]}"
                    line = f"{fav} {spd}"
                    # print(line)
                    # self.update_time = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
                    print(f"{game},{line},{odds_quote.get('spread')},{odds_quote.get('overUnder')},"
                          f"{favorite},{provider},{self.update_time}")
                    csv_writer.writerow([game, line, odds_quote.get('spread'),
                                         odds_quote.get('overUnder'), favorite, provider, self.update_time])
        df = pd.read_csv(file_name)

        try:
            self.DB.df_to_sql(df, table_name, register=False)
        except Exception as ex:
            print(f"Exception in df.to_sql: {ex}")
            self.DB.reset()

    def run_odds(self):
        games_query = self.DB.query(f"select distinct game_id from LeagueSchedule "
                                    f"where game_week = (select max(week) from PlayerStats where "
                                    f"Year = ( select max(year) from CurrentSeason)) and year = "
                                    f"( select max(year) from CurrentSeason)")

        [self.game_odds(row['game_id']) for row in games_query]


def main():
    # Stats().start(threaded=False, sleep_interval=240)
    Odds().run_odds()


if __name__ == "__main__":
    main()

# def game_odds(game):
#     file_name = './data/odds.csv'
#     table_name = "Odds"
#     url = str(f"https://sports.core.api.espn.com/v2/sports/football/"
#               f"leagues/nfl/events/{game}/competitions/{game}/odds")
#     odds_data = request_instance.make_request(url=url, output_file=f"./data/ESPNOdds.json", write=True)
#     with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
#         # creating a csv writer object
#         csv_writer = csv.writer(csv_file)
#         csv_writer.writerow(['gameid', 'line', 'spread', 'OU', 'favorite',
#                              'provider', 'update_time'])
#         for odds_quote in odds_data.get('items'):
#             provider = None
#             if odds_quote.get('provider'):
#                 provider = odds_quote.get('provider').get('name', "?")
#             favorite = "?"
#             if odds_quote.get('awayTeamOdds'):
#                 if odds_quote.get('awayTeamOdds').get('favorite') is True:
#                     favorite = "Away"
#                 else:
#                     favorite = "Home"
#             if favorite != "?":
#                 line = str(odds_quote.get('details'))
#                 fav, spd = line.split(" ")
#                 if len(fav) == 2:
#                     fav = f"{fav[0]} {fav[1]}"
#                 line = f"{fav} {spd}"
#                 print(line)
#                 print(f"{game},{line},{odds_quote.get('spread')},{odds_quote.get('overUnder')},"
#                       f"{favorite},{provider},{update_time}")
#                 csv_writer.writerow([game, line, odds_quote.get('spread'),
#                                      odds_quote.get('overUnder'), favorite, provider, update_time])
#     df = pd.read_csv(file_name)
#
#     try:
#         fdb.df_to_sql(df, table_name, register=False)
#     except Exception as ex:
#         print(f"Exception in df.to_sql: {ex}")
#         fdb.reset()
#
#
# def odds():
#     games_query = fdb.query(f"select distinct game_id from LeagueSchedule "
#                             f"where game_week = (select max(week) from PlayerStats where "
#                             f"Year = ( select max(year) from CurrentSeason)) and year = "
#                             f"( select max(year) from CurrentSeason)")
#
#     [game_odds(row['game_id']) for row in games_query]
