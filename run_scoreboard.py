__author__ = 'chance'

from modules import scoreboard


def main():
    sleep_minutes = 10
    scoreboard.run(main_loop_sleep=60*sleep_minutes)


if __name__ == "__main__":
    main()
