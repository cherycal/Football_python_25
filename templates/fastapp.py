from fastapi import FastAPI, Path, Query
from pydantic import BaseModel
from typing import Optional
import sys
sys.path.append('../modules')
import time
import espn_request
import csv
import pandas as pd

import sqldb

fdb = sqldb.DB('Football.db')

app = FastAPI()

class ESPNRosters(BaseModel):
    team_id: Optional[int] = None
    league: str

class PlayerDashboard(BaseModel):
    player_id: Optional[int] = None
    team: Optional[str] = None
    name: str
    lgm: Optional[int] = None
    pos: Optional[str] = None
    FRAN: Optional[str] = None
    avg_ul: Optional[float] = None
    avg_ll: Optional[float] = None

@app.get("/player", status_code=200)
async def views(name: Optional[str] =
                Query(None, title="Player Dashboard", description="Part of the player's name"),
                FRAN: Optional[str] =
                Query(None, title="Player Dashboard", description="FRAN league team"),
                team: Optional[str] =
                Query(None, title="Player Dashboard", description="NFL team"),
                avg_ll: Optional[float] =
                Query(None, title="Player Dashboard", description="Avg lower limit")
                ):
    query = f"Select * from PlayerDashboard where id is not NULL "
    if name is not None:
        query += f" and name like '%{name}%' "
    if FRAN is not None:
        query += f" and FRAN like '%{FRAN}%' "
    if team is not None:
        query += f" and tm like '%{team}%' "
    if avg_ll is not None:
        query += f" and avg >= {avg_ll}"
    print(query)
    return fdb.query(query)


# @app.get("/{name}")
# async def views(name: str, ):
#     return fdb.query(f"Select * from {name}")

@app.get("/query/{view}")
async def views(view: str, league: str, column: str | None = None):
    print(f"Select * from {view} where league = '{league}'")
    return fdb.query(f"Select * from {view} where league is not NULL and league = '{league}'")

@app.get("/")
async def home():
    return fdb.query(f"Select * from ESPNRosters")
