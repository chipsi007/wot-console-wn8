import sqlite3
import pickle


from .conn import conn, cur


#Export data as a tuple of headers and rows.


def export_tankopedia():

    columns = ['tank_id', 'name', 'short_name', 'nation', 'is_premium', 'tier', 'type']
    columns_str = ', '.join(columns)

    cur.execute(f'SELECT {columns_str} FROM tankopedia')
    rows = []
    for row in cur:
        rows.append([
            row[0],
            row[1],
            row[2],
            row[3],
            True if row[4] == 1 else False,
            row[5],
            row[6]
        ])

    return columns, rows


def export_percentiles():

    columns = ['tank_id', 'data']
    columns_str = ', '.join(columns)

    cur.execute(f'SELECT {columns_str} FROM percentiles')
    rows = [[x[0], pickle.loads(x[1])] for x in cur]

    return columns, rows


def export_percentiles_generic():

    columns = ['tier', 'type', 'data']
    columns_str = ', '.join(columns)

    cur.execute(f'SELECT {columns_str} FROM percentiles_generic')
    rows = [[x[0], x[1], pickle.loads(x[2])] for x in cur]

    return columns, rows


def export_wn8_exp_values():

    columns = ['tank_id', 'expFrag', 'expDamage', 'expSpot', 'expDef', 'expWinRate']
    columns_str = ', '.join(columns)

    rows = cur.execute(f'SELECT {columns_str} FROM wn8').fetchall()

    return columns, rows


def export_history(min_timestamp=0):

    columns = [
        'tank_id', 'created_at', 'popularity_index',
        'battle_life_time', 'capture_points', 'damage_assisted_radio',
        'damage_dealt', 'damage_received', 'direct_hits_received',
        'frags', 'hits', 'piercings', 'piercings_received',
        'shots', 'spotted', 'survived_battles', 'wins', 'xp'
    ]
    columns_str = ', '.join(columns)

    rows = cur.execute(f'''
        SELECT {columns_str} FROM history
        WHERE created_at > ?
            AND strftime('%Y%W', created_at, 'unixepoch') != strftime('%Y%W', 'now')
    ''', [min_timestamp]).fetchall()

    return columns, rows
