__author__ = 'chance'

from modules import player_stats


def main():

    player_stats.Stats().tables_to_html(['CWOTV','PlayerByWeek'])

    player_stats.run_snaps()


if __name__ == "__main__":
    main()
