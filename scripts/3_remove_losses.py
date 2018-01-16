#This script is to remove losses column from history table.

import sqlite3

conn = sqlite3.connect('sqlite.db')
cur = conn.cursor()

cur.execute('''
    CREATE TABLE temp_history (
        tank_id INTEGER,
        created_at INTEGER,
        popularity_index INTEGER,
        battle_life_time REAL,
        capture_points REAL,
        damage_assisted_radio REAL,
        damage_dealt REAL,
        damage_received REAL,
        direct_hits_received REAL,
        frags REAL,
        hits REAL,
        piercings REAL,
        piercings_received REAL,
        shots REAL,
        spotted REAL,
        survived_battles REAL,
        wins REAL,
        xp REAL
);''')
conn.commit()

cur.execute('''
    INSERT INTO temp_history 
    SELECT
        tank_id,
        created_at,
        popularity_index,
        battle_life_time,
        capture_points,
        damage_assisted_radio,
        damage_dealt,
        damage_received,
        direct_hits_received,
        frags,
        hits,
        piercings,
        piercings_received,
        shots,
        spotted,
        survived_battles,
        wins,
        xp
    FROM history
''')
conn.commit()

cur.execute('DROP TABLE history')
conn.commit()

cur.execute('''
    CREATE TABLE history (
        tank_id INTEGER,
        created_at INTEGER,
        popularity_index REAL,
        battle_life_time REAL,
        capture_points REAL,
        damage_assisted_radio REAL,
        damage_dealt REAL,
        damage_received REAL,
        direct_hits_received REAL,
        frags REAL,
        hits REAL,
        piercings REAL,
        piercings_received REAL,
        shots REAL,
        spotted REAL,
        survived_battles REAL,
        wins REAL,
        xp REAL,
        PRIMARY KEY (tank_id, created_at)
);''')
conn.commit()

cur.execute('INSERT INTO history SELECT * FROM temp_history')
conn.commit()

cur.execute('DROP TABLE temp_history')
conn.commit()

conn.close()

print('All done')