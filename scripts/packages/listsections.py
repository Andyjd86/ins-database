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
def junction_select(route_table, vertex_table, junct_road, junct_list, search_rad, sect_funct):
    cur.callproc("client.junction_select", [route_table, vertex_table, junct_road, junct_list, search_rad, sect_funct])
    #conn.commit()
    r = cur.fetchall()
    print(r)
    return r


