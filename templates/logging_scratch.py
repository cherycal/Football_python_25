__author__ = 'chance'
import sys
sys.path.append('../modules')
import push


def main():
    logger = push.get_logger(logfilename="./logging_scratch_log.txt")
    logger.info(f"Info green")
    logger.debug(f"Debug cyan")
    logger.warning(f"Warning yellow")
    logger.error(f"Error red")
    logger.critical(f"Critical red")


if __name__ == "__main__":
    main()
