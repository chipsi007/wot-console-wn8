import sqlite3
import time


from .conn import conn, cur


#Functions for tankopedia table.


def get_tiertype_tankids(tank_tier, tank_type):
    cur.execute('SELECT tank_id FROM tankopedia WHERE tier = ? AND type = ?;', (tank_tier,  tank_type))
    return [x[0] for x in cur]


def get_distinct_tankids():
    cur.execute('SELECT DISTINCT(tank_id) FROM tankopedia;')
    return [x[0] for x in cur]


def get_tankopedia():
    #Get tankopedia as dictionary of dictionaries.
    #Returns: {"111": {...}, ...}

    output = {}
    cur.execute('SELECT tank_id, name, short_name, nation, is_premium, tier, type FROM tankopedia')
    for row in cur:
        output[str(row[0])] = {
            "tank_id":      row[0],
            "name":         row[1],
            "short_name":   row[2],
            "nation":       row[3],
            "is_premium":   True if row[4] == 1 else False,
            "tier":         row[5],
            "type":         row[6]
        }

    return output


def put(tanks):
    #Input: [{...}, {...}, ...]

    for tank in tanks:
        cur.execute('''
            INSERT OR REPLACE INTO tankopedia (tank_id, updated_at, name, short_name, nation, is_premium, tier, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        ''', (
            tank['tank_id'],
            int(time.time()),
            tank['name'],
            tank['short_name'],
            tank['nation'],
            1 if tank['is_premium'] == True else 0,
            tank['tier'],
            tank['type']
        ))

    conn.commit()
