import sqlite3
import pickle
import time


from .conn import conn, cur


#Functions for percentiles, percentiles_generic tables.


def update_percentiles(data, tank_id):
    '''Update generic percentiles row for one tank. No commit.

    Arguments:
        data:Dict[str, List[num]] - percentiles.
        tank_id:int - tank_id of the tank.

    Triggers replace if tank_id already exists.
    '''

    cur.execute('''
        INSERT OR REPLACE INTO percentiles (tank_id, updated_at, data) VALUES (?, ?, ?);
    ''', (tank_id, int(time.time()), pickle.dumps(data)))


def update_percentiles_generic(data, tank_tier, tank_type):
    '''Update generic percentiles row for one tier-class. No commit.

    Arguments:
        data:Dict[str, List[num]] - percentiles.
        tank_tier:int - tier of the data object, from 1 to 10 inclusive.
        tank_type:str - type of the data object.

    Triggers replace if (tier, type) already exists.
    '''

    cur.execute('''
        INSERT OR REPLACE INTO percentiles_generic (tier, type, updated_at, data) VALUES (?, ?, ?, ?);
    ''', (tank_tier, tank_type, int(time.time()), pickle.dumps(data)))
