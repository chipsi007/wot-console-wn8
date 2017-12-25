import sqlite3
import pickle


from .conn import conn, cur


#Export data as list of dictionaries for remote host.


def export_tankopedia():

    output = []
    cur.execute('SELECT tank_id, name, short_name, nation, is_premium, tier, type FROM tankopedia')
    for row in cur:
        output.append({
            "tank_id":      row[0],
            "name":         row[1],
            "short_name":   row[2],
            "nation":       row[3],
            "is_premium":   True if row[4] == 1 else False,
            "tier":         row[5],
            "type":         row[6]
        })

    return output


def export_percentiles():

    cur.execute('SELECT tank_id, data FROM percentiles;')
    return [{'tank_id': x[0], 'data': pickle.loads(x[1])} for x in cur]


def export_percentiles_generic():

    cur.execute('SELECT tier, type, data FROM percentiles_generic;')
    return [{'tier': x[0], 'type':x[1], 'data': pickle.loads(x[2])} for x in cur]


def export_wn8_exp_values():

    output = []
    cur.execute('SELECT tank_id, expFrag, expDamage, expSpot, expDef, expWinRate FROM wn8;')
    for row in cur:
        output.append({
            'tank_id':    row[0],
            'expFrag':    row[1],
            'expDamage':  row[2],
            'expSpot':    row[3],
            'expDef':     row[4],
            'expWinRate': row[5]
        })

    return output


def export_history():
    '''Export history values for all tanks. Export every timestamp except for current week.

    Returns:
        List[Dict[str, Any([int, List[num]])]
    '''

    columns = [
        'tank_id', 'created_at', 'popularity_index',
        'battle_life_time', 'capture_points', 'damage_assisted_radio',
        'damage_dealt', 'damage_received', 'direct_hits_received',
        'frags', 'hits', 'losses', 'piercings', 'piercings_received',
        'shots', 'spotted', 'survived_battles', 'wins', 'xp'
    ]

    columns_str = ', '.join(columns)

    cur.execute('SELECT DISTINCT(tank_id) FROM history;')
    tank_ids = [x[0] for x in cur]

    output = []
    for tank_id in tank_ids:
        cur.execute(f'''
            SELECT {columns_str} FROM history
            WHERE tank_id = ?
                AND strftime('%Y%W', created_at, 'unixepoch') != strftime('%Y%W', 'now')
            ORDER BY created_at DESC LIMIT 50
        ''', [tank_id])

        rows = cur.fetchall()

        if rows:
            #Every field except for tank_id is an array of numbers.
            temp_dict = {key: [x[k] for x in rows] for k, key in enumerate(columns)}
            temp_dict['tank_id'] = tank_id
            output.append(temp_dict)

    return output
