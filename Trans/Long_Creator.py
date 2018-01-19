import psycopg2
from psycopg2 import sql
import re
import os
import glob
import sys
try:
    conn = psycopg2.connect("dbname = '24503_M6SMP' user = 'AndyDixon' host = 'localhost' password = '@Uckland86'")
except:
    print ('I am unable to connect the database')
cur = conn.cursor()
ipath = 'edge_beam_hs'
fpath = 'C:\\Users\\Public\\Documents\\'+ipath+'\\'
old_prj = 32630
new_prj = 27700
transfile = 'public.long_master_'+ ipath
thinned = 'thinned_'+ipath
cur.execute("""CREATE TABLE %s
                    (
                    id bigserial,
                    filename text,
                    trace integer,
                    profile integer,
                    cont_profile integer,
                    confidence integer,
                    direction text,
                    x numeric,
                    y numeric,
                    dist_from numeric,
                    pc15 numeric,
                    geom geometry(Point,27700)
                    );""" %(transfile))
conn.commit()
for ffile in glob.glob(os.path.join(fpath+"input\\", '*.txt')):
        sql_table = re.search('m6_(.+?).txt', (os.path.basename(ffile)))
        if sql_table:
            sql_table_name = sql_table.group(1)
        lane_search = re.search('_l(.+?)_', (os.path.basename(ffile)))
        if lane_search:
            col_lane = lane_search.group(1)
        dir_search = re.search('_d(.+?)_', (os.path.basename(ffile)))
        if dir_search:
            col_dir = dir_search.group(1)
        region_find = re.search('(.+?).txt', (os.path.basename(ffile)))
        if region_find:
            col_file = region_find.group(1)
        print(sql_table_name)
        print(lane_search)
        print(col_dir)
        print(col_file)
        with open(ffile,'r') as f:
                nfile = fpath+'output\\'+'new_'+os.path.basename(ffile)
                with open(nfile,'w') as f1:
                        for line in f:
                                if '#X' in line:
                                        new_header_list = []
                                        create_table_list = []
                                        column_header_list = []
                                        old_header=line.split('\t')
                                        j=0
                                        p=1
                                        for i in old_header:
                                                print(i)
                                                print(len(old_header))
                                                if 'Std.dev' in (i) or 'NumValues' in (i)or 'Max' in (i)or 'distribution' in (i):
                                                    new_header_list.append('ignore'+str(j))
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1
                                                elif 'Thickness' in (i) or 'histogram' in (i)or 'Min' in (i):
                                                    new_header_list.append('ignore'+str(j))
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1
                                                elif 'Confidence (PC' in (i):
                                                    new_header_list.append('confidence')
                                                    create_table_list.append('confidence integer')
                                                    column_header_list.append('confidence')                                                    
                                                elif 'PC' in (i):
                                                    m = re.search('PC(.+?)_', (i))
                                                    if m:
                                                        if m.group(1) == p:
                                                            print(m.group(1))
                                                            new_header_list.append('ignore_pc' + m.group(1))
                                                            create_table_list.append('ignore_pc' + m.group(1)+' numeric')
                                                        else:
                                                            print(m.group(1))
                                                            p = m.group(1)
                                                            new_header_list.append('pc' + m.group(1))
                                                            create_table_list.append('pc' + m.group(1)+' numeric')
                                                            column_header_list.append('pc' + m.group(1))
                                                    else:
                                                        m = re.search('PC(.+?)_', (i))
                                                elif (i)=='#X':
                                                    new_header_list.append('x')
                                                    create_table_list.append('trace integer')
                                                    column_header_list.append('trace')
                                                elif (i)=='Y':
                                                    new_header_list.append('y')
                                                    create_table_list.append('profile integer')
                                                    column_header_list.append('profile')
                                                elif (i)=='Northing' or (i)=='Latitude':
                                                    new_header_list.append('y')
                                                    create_table_list.append('y numeric')
                                                    column_header_list.append('y')
                                                    col_y = 'y'
                                                elif (i)=='Easting' or (i)=='Longitude':
                                                    new_header_list.append('x')
                                                    create_table_list.append('x numeric')
                                                    column_header_list.append('x')
                                                    col_x = 'x'
                                                elif (i)=='':
                                                    new_header_list.append('ignore'+str(j))
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1
                                                elif (i)=='\n' or (i)=='\t':
                                                    new_header_list.append('ignore'+str(j)+'\n')
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1
                                                elif (i)=='\t':
                                                    new_header_list.append('ignore'+str(j))
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1                                                
                                                else:
                                                    new_header_list.append('ignore'+str(j))
                                                    create_table_list.append('ignore'+str(j)+' text')
                                                    j +=1
                                        z = len(new_header_list)
                                        print(z)
                                        new_header = '\t'.join(new_header_list)
                                        create_table = ', '.join(create_table_list)
                                        column_header = ', '.join(column_header_list)
                                        print(create_table)
                                        print(new_header_list)
                                        f1.write(new_header)
                                #elif '#Region' in line:
                                #        region_find = re.search('\t(.+?)\n', (line))
                                #        if region_find:
                                #                col_file = region_find.group(1)
                                elif line[0] == '#' or line=='\n':
                                        continue
                                else:
                                    linelength=line.split('\t')
                                    u = len(linelength)
                                    #print(linelength)
                                    new_line = '\t'.join(linelength)
                                    #print(new_line)
                                    new_line = new_line[:-1]
                                    ##print(u)
                                    c=''
                                    if z - u == 0:
                                        f1.write(line)
                                    else:
                                        k = z-u
                                        for g in range(k):
                                            c = '\t'+'a'+c
                                        #print(c+'\n')
                                        new_line2 = new_line+c+'\n'
                                        #print(new_line2)
                                        
                                        f1.write(new_line+c+'\n')
        sqlfile = open(nfile)
        bufferfile = 'public.buffer_'+ ipath
        cur.execute("""CREATE TABLE %s (%s);""" %(sql_table_name, create_table))
        conn.commit()
        SQLstate = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '\t' NULL 'NaN';" % (sql_table_name)
        print(SQLstate)
        cur.copy_expert(SQLstate,sqlfile)
        conn.commit()
        SQLstate1 = "INSERT INTO %s (%s, filename, direction) (SELECT %s, '%s', 'nb' FROM %s WHERE confidence IS NOT NULL ORDER BY trace, profile);" % (transfile, column_header, column_header, col_file, sql_table_name)
        print(SQLstate1)
        cur.execute("""INSERT INTO %s (%s, filename, direction)
                        (SELECT %s, '%s', 'sb'
                        FROM %s
                        WHERE confidence IS NOT NULL
                        ORDER BY trace, profile);""" % (transfile, column_header, column_header, col_file, sql_table_name))
        conn.commit()
        cur.execute("""UPDATE %s
                        SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),%s),%s)
                        WHERE geom IS NULL;""" % (transfile, col_x, col_y, old_prj, new_prj))
        conn.commit()
##        cur.execute("""CREATE TABLE %s as
##                        SELECT t.* 
##                        FROM (
##                            SELECT *, row_number() OVER(ORDER BY id ASC) AS row
##                            FROM %s
##                            ) t
##                        WHERE t.row % 20 = 0;""" % (thinned, transfile))
##        conn.commit()
        
#        cur.execute("""UPDATE %s
#                        SET section_label=tyr.section_label, section_chain=tyr.section_chain, priority=tyr.priority
#                        FROM (SELECT file, section_label, section_chain, trace, priority FROM file_tbl) as tyr
#                        WHERE filename = tyr.file AND %s.trace = tyr.trace;""" %(transfile, transfile))
        conn.commit()
        cur.execute("""DROP TABLE %s;""" %(sql_table_name))
        conn.commit()
#cur.execute("""CREATE TABLE %s AS
#                SELECT 
#                g.path[1] as gid, 
#                g.geom::geometry(Polygon, 27700) as geom,
#                g.lane
#                FROM
#                (SELECT pte.lane, (ST_DUMP(ST_UNION(ST_BUFFER(pte.newgeom,0.5,'endcap=flat join=round')))).*
#                FROM (SELECT lane, trace, ST_MakeLine(geom ORDER BY profile) as newgeom FROM %s GROUP BY lane, section_label, trace) as pte GROUP BY pte.lane) as g;
#
#                ALTER TABLE %s ADD COLUMN tid bigserial;""" %(bufferfile, transfile, bufferfile))
#conn.commit()
#cur.execute("""UPDATE %s set overlap = 'overlap', gid = sgrouptrans.pid
#                FROM(
#                        SELECT grouptrans.id, grouptrans.pid
#                        FROM(
#                                SELECT dupetrans.id, max(dupetrans.tid) as pid, count(*)
#                                FROM(
#                                        SELECT %s.id, section_label, section_chain, filename, trace, profile, %s.lane, %s.priority, %s.tid as tid
#                                        FROM %s 
#                                        INNER JOIN %s on st_contains(%s.geom, %s.geom) or st_touches(%s.geom, %s.geom) 
#                                        ORDER BY section_chain, priority, profile, tid, lane, trace
#                                ) as dupetrans
#                                GROUP BY dupetrans.id
#                                HAVING count(*) > 1 
#                                ORDER BY pid, id
#                        ) as grouptrans
#                ) as sgrouptrans WHERE %s.id = sgrouptrans.id;""" %(transfile, transfile, transfile, transfile, bufferfile, bufferfile, transfile, bufferfile, transfile, bufferfile, transfile, transfile))
#conn.commit()
#cur.execute("""UPDATE %s set dont_use = 'yes'
#                FROM(
#                        SELECT gid, max(priority) as maxt FROM %s
#                        GROUP BY overlap, gid
#                        HAVING overlap = 'overlap'
#                        ORDER BY gid
#                ) as tempt
#                WHERE %s.gid = tempt.gid AND %s.priority <> tempt.maxt""" %(transfile, transfile, transfile, transfile))
#conn.commit()
#cur.execute("""UPDATE %s
#                SET cont_profile = subquery.cont_profile
#                FROM(
#                        SELECT id, gid, section_chain, priority, profile, ROW_NUMBER() OVER (PARTITION BY section_chain ORDER BY section_chain, priority, profile)-1 AS cont_profile
#                        FROM %s
#                        WHERE dont_use IS NULL
#                ) AS subquery
#                WHERE %s.id = subquery.id;""" %(transfile, transfile, transfile))
#conn.commit()
#csvfile = open(fpath+'output\\'+ipath+'.csv','w')
#SQLstate = ("""COPY (SELECT filename, section_label, section_chain as trans_chain, direction, lane, trace, profile as orig_profile, cont_profile, ST_X(geom) as osgb_easting,
#                ST_Y(geom) as osgb_northing, pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13, pc14, pc15, pc16, pc17, pc18, pc19, pc20
#                FROM %s
#                WHERE dont_use IS NULL ORDER BY section_label, section_chain, priority, gid, profile) TO STDOUT WITH CSV HEADER;""" % (transfile))
#print(SQLstate)
#cur.copy_expert(SQLstate,csvfile)
cur.close()
conn.close()
