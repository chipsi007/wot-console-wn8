import sqlite3


from .conn import conn, cur


#History table.


def put(list_of_dicts):

    columns = [
        'tank_id', 'created_at', 'popularity_index',
        'battle_life_time', 'capture_points', 'damage_assisted_radio',
        'damage_dealt', 'damage_received', 'direct_hits_received',
        'frags', 'hits', 'losses', 'piercings', 'piercings_received',
        'shots', 'spotted', 'survived_battles', 'wins', 'xp'
    ]

    columns_str = ', '.join(columns)
    question_marks = ', '.join(['?' for i in columns])

    rows = []
    for item in list_of_dicts:
        rows.append([item[col] for col in columns])

    cur.executemany(f'INSERT INTO history ({columns_str}) VALUES ({question_marks})', rows)
    conn.commit()


def aggregate():

    #Get yearweeks as 'YYYYWW' and corresponding timestamps. Ignore current week. UTC time.
    cur.execute('''
        SELECT
            strftime('%Y%W', created_at, 'unixepoch'),
            CAST(AVG(created_at) AS INT) AS created_at
        FROM history
        WHERE strftime('%Y%W', created_at, 'unixepoch') != strftime('%Y%W', 'now')
        GROUP BY
            strftime('%Y', created_at, 'unixepoch'),
            strftime('%W', created_at, 'unixepoch')
        HAVING COUNT(DISTINCT created_at) > 1
    ''')

    yearweeks, timestamps = [], []
    for row in cur:
        yearweeks.append(row[0])
        timestamps.append(row[1])

    #Iterate over all yearweeks.
    for i in range(len(yearweeks)):
        yearweek = yearweeks[i]
        timestamp = timestamps[i]

        #Insert aggregated row with average timestamp. Replace row if the timestamp already exists.
        cur.execute('''
            INSERT OR REPLACE INTO history
            SELECT
                tank_id,
                ? AS created_at,
                ROUND(AVG(popularity_index), 2),
                ROUND(AVG(battle_life_time), 2),
                ROUND(AVG(capture_points), 2),
                ROUND(AVG(damage_assisted_radio), 2),
                ROUND(AVG(damage_dealt), 2),
                ROUND(AVG(damage_received), 2),
                ROUND(AVG(direct_hits_received), 2),
                ROUND(AVG(frags), 2),
                ROUND(AVG(hits), 2),
                ROUND(AVG(losses), 2),
                ROUND(AVG(piercings), 2),
                ROUND(AVG(piercings_received), 2),
                ROUND(AVG(shots), 2),
                ROUND(AVG(spotted), 2),
                ROUND(AVG(survived_battles), 2),
                ROUND(AVG(wins), 2),
                ROUND(AVG(xp), 2)
            FROM history
            WHERE strftime('%Y%W', created_at, 'unixepoch') = ?
            GROUP BY tank_id
        ''', [timestamp, yearweek])

        #Remove all other rows where timestamp is different from the inserted one.
        cur.execute('''
            DELETE FROM history
            WHERE
                created_at != ?
                AND strftime('%Y%W', created_at, 'unixepoch') = ?
        ''', [timestamp, yearweek])

    conn.commit()
