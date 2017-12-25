#This scrip will change the primary key for "tanks" table.

import sqlite3

conn = sqlite3.connect('sqlite.db')
cur = conn.cursor()

cur.execute('CREATE TABLE temp AS SELECT * FROM tanks;')
conn.commit()

cur.execute('DROP TABLE tanks;')
conn.commit()

cur.execute('''
    CREATE TABLE tanks (
        tank_id INTEGER,
        last_battle_time INTEGER,
        account_id INTEGER,
        server TEXT,
        battle_life_time INTEGER,
        battles INTEGER,
        capture_points INTEGER,
        damage_assisted_radio INTEGER,
        damage_assisted_track INTEGER,
        damage_dealt INTEGER,
        damage_received INTEGER,
        direct_hits_received INTEGER,
        dropped_capture_points INTEGER,
        explosion_hits INTEGER,
        explosion_hits_received INTEGER,
        frags INTEGER,
        hits INTEGER,
        losses INTEGER,
        mark_of_mastery INTEGER,
        max_frags INTEGER,
        max_xp INTEGER,
        no_damage_direct_hits_received INTEGER,
        piercings INTEGER,
        piercings_received INTEGER,
        shots INTEGER,
        spotted INTEGER,
        survived_battles INTEGER,
        trees_cut INTEGER,
        wins INTEGER,
        xp INTEGER,
        PRIMARY KEY (tank_id, account_id, server)
    );
''')
conn.commit()

cur.execute('INSERT INTO tanks SELECT * FROM temp;')
conn.commit()

cur.execute('DROP TABLE temp;')
conn.commit()

cur.execute('VACUUM;')
conn.commit()

cur.close()
conn.close()

print('All done')
