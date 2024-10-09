__author__ = 'chance'
import sys
import time

from tqdm import tqdm

sys.path.append('../modules')
import threading

def loop1():
    for i in tqdm(range(100)):
        time.sleep(2)

def loop2():
    for i in tqdm(range(100)):
        time.sleep(2)

def main():
    loop1_thread = threading.Thread(target=loop1)
    loop2_thread = threading.Thread(target=loop2)
    loop1_thread.start()
    time.sleep(1)
    loop2_thread.start()


if __name__ == "__main__":
    main()
