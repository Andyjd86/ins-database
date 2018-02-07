import psycopg2

conn = None
try:
    # Password is contained in a file so that it isn't exposed here.
    conn = psycopg2.connect("dbname = 'ins_data_dev' user = 'andydixon' host = 'localhost'")
except:
    print("I am unable to connect the database")
cur = conn.cursor()
fpath = 'C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\text\\TRL - Section data_M6_M27_A47.csv'
# Table name to use in function
tblname = "hapms_master"
tempname = "hapms_temp"
# Calls the function in PostgreSQL
cur.callproc("client.create_table_hapms", [tblname])
conn.commit()
SQLstate = "COPY client.%s FROM STDIN WITH CSV HEADER DELIMITER AS ',';" % tblname
print(SQLstate)
cur.copy_expert(SQLstate, open(fpath))
conn.commit()
cur.callproc("client.update_geom", [tblname, tempname])
conn.commit()
cur.close()
conn.close()
