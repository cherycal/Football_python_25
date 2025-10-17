__author__ = 'chance'

import sys
sys.path.append('./modules')
from modules.player_stats import Stats
def main():
    stats = Stats(season=2025)
    print(stats)
    stats.write_player_stats()


if __name__ == "__main__":
    main()
