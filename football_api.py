__author__ = 'chance'

from modules import tools
from typing import Union
from fastapi import FastAPI

app = FastAPI()


class FootballAPI:
    def __init__(self):
        self.name = "Football"

    def start_api(self):
        print(f"Starting api {self.name}")

    @staticmethod
    @app.get("/")
    def read_root():
        return {"Hello": "World"}


@tools.connection_check
def main():
    print(f"Started at {tools.now()}")
    FootballAPI().start_api()


if __name__ == "__main__":
    main()
