import sqlite3


from .conn import conn, cur


#History table.


def put(list_of_dicts):

    columns = [
        'tank_id', 'created_at', 'popularity_index',
        'battle_life_time', 'capture_points', 'damage_assisted_radio',
        'damage_dealt', 'damage_received', 'direct_hits_received',
        'frags', 'hits', 'piercings', 'piercings_received',
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
    '''Aggregate history into 1 record per week per tank_id. Ignore current week.

    Replaces non-aggregated rows with aggregated row with same created_at for all tank_ids.
    '''

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
                ROUND(AVG(popularity_index), 3),
                ROUND(AVG(battle_life_time), 3),
                ROUND(AVG(capture_points), 3),
                ROUND(AVG(damage_assisted_radio), 3),
                ROUND(AVG(damage_dealt), 3),
                ROUND(AVG(damage_received), 3),
                ROUND(AVG(direct_hits_received), 3),
                ROUND(AVG(frags), 3),
                ROUND(AVG(hits), 3),
                ROUND(AVG(piercings), 3),
                ROUND(AVG(piercings_received), 3),
                ROUND(AVG(shots), 3),
                ROUND(AVG(spotted), 3),
                ROUND(AVG(survived_battles), 3),
                ROUND(AVG(wins), 3),
                ROUND(AVG(xp), 3)
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
