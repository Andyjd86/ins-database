import psycopg2

conn = None
try:
    # Password is contained in a file so that it isn't exposed here.
    conn = psycopg2.connect("dbname = 'ins_data_dev' user = 'andydixon' host = 'localhost'")
except:
    print("I am unable to connect the database")

cur = conn.cursor()

def walk_the_network(sect_start, sect_end = None, dir_key = 1, sect_funct = None, op_code = None, road = None):
    cur.callproc("client.walk_network", [dir_key, sect_start, sect_funct, sect_end, op_code, road])
    conn.commit()

# Define function 'sect_select()'
def sect_select(sect_from, sect_to):
    sect_from = '0900M6/272'
    sect_to = '0900M6/341'
    cur.execute("""SELECT section_label, section
                FROM (
                    SELECT section_label, section
                    FROM client.hapms_master
                    WHERE fid
                    IN (
                        SELECT unnest(fid_trail)
                        FROM client.network_walk
                        WHERE section_label = %(to_sect)s
                    )
                ) as t1
                WHERE NOT EXISTS (
                SELECT section_label
                FROM (
                    SELECT section_label
                    FROM client.hapms_master
                    WHERE section_label != %(from_sect)s
                    AND fid
                    IN (
                        SELECT unnest(fid_trail)
                        FROM client.network_walk
                        WHERE section_label = %(from_sect)s
                    )
                ) as t2
                WHERE t1.section_label = t2.section_label
                );""", {'from_sect': sect_from, 'to_sect': sect_to})
    for r in cur.fetchall():
        print(r)
        a,b = r
        print(a)
        print(b)
        print('next')


