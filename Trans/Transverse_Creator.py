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
ipath = 'j14ssa_nb'
fpath = 'C:\\Users\\Public\\Documents\\'+ipath+'\\'
old_prj = 32630
new_prj = 27700
transfile = 'public.trans_master_'+ ipath
filetblname = 'file_tbl'
cur.execute("""CREATE TABLE %s
                    (
                    id bigserial,
                    filename text,
                    section_label text,
                    section_chain numeric,
                    direction text,
                    lane text,
                    trace integer,
                    profile integer,
                    cont_profile integer,
                    x numeric,
                    y numeric,
                    overlap text,
                    priority integer,
                    dont_use text,
                    gid integer,
                    dist_from numeric,
                    pc01 numeric,
                    pc02 numeric,
                    pc03 numeric,
                    pc04 numeric,
                    pc05 numeric,
                    pc06 numeric,
                    pc07 numeric,
                    pc08 numeric,
                    pc09 numeric,
                    pc10 numeric,
                    pc11 numeric,
                    pc12 numeric,
                    pc13 numeric,
                    pc14 numeric,
                    pc15 numeric,
                    pc16 numeric,
                    pc17 numeric,
                    pc18 numeric,
                    pc19 numeric,
                    pc20 numeric,
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
            if col_dir == 'nb':
                dirsort = 'ASC'
            else:
                dirsort = 'DESC'
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
                                                        print(m.group(1))
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
                                        new_header = '\t'.join(new_header_list)
                                        create_table = ', '.join(create_table_list)
                                        column_header = ', '.join(column_header_list)
                                        print(create_table)
                                        f1.write(new_header)
                                #elif '#Region' in line:
                                #        region_find = re.search('\t(.+?)\n', (line))
                                #        if region_find:
                                #                col_file = region_find.group(1)
                                elif line[0] == '#' or line=='\n':
                                        continue
                                else:
                                        f1.write(line)
        sqlfile = open(nfile)
        bufferfile = 'public.buffer_'+ ipath
        cur.execute("""CREATE TABLE %s (%s);""" %(sql_table_name, create_table))
        conn.commit()
        SQLstate = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '\t' NULL 'NaN';" % (sql_table_name)
        print(SQLstate)
        cur.copy_expert(SQLstate,sqlfile)
        conn.commit()
        SQLstate1 = "INSERT INTO %s (%s, filename, lane, direction) (SELECT %s, '%s', '%s', '%s' FROM %s WHERE trace IN (SELECT trace FROM %s WHERE file = '%s'));" % (transfile, column_header, column_header, col_file, col_lane, col_dir, sql_table_name, filetblname, col_file)
        print(SQLstate1)
        cur.execute("""INSERT INTO %s (%s, filename, lane, direction)
                        (SELECT %s, '%s', '%s', '%s'
                        FROM %s
                        WHERE trace IN
                        (SELECT trace
                        FROM %s
                        WHERE file = '%s'));""" % (transfile, column_header, column_header, col_file, col_lane, col_dir, sql_table_name, filetblname, col_file))
        conn.commit()
        cur.execute("""UPDATE %s
                        SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),%s),%s)
                        WHERE geom IS NULL;""" % (transfile, col_x, col_y, old_prj, new_prj))
        conn.commit()
        cur.execute("""UPDATE %s
                        SET section_label=tyr.section_label, section_chain=tyr.section_chain, priority=tyr.priority
                        FROM (SELECT file, section_label, section_chain, trace, priority FROM %s) as tyr
                        WHERE filename = tyr.file AND %s.trace = tyr.trace;""" %(transfile, filetblname, transfile))
        conn.commit()
        cur.execute("""DROP TABLE %s;""" %(sql_table_name))
        conn.commit()
cur.execute("""CREATE TABLE %s AS
                SELECT
                g.path[1] as gid,
                g.geom::geometry(Polygon, 27700) as geom,
                g.lane
                FROM
                (SELECT pte.lane, (ST_DUMP(ST_UNION(ST_BUFFER(pte.newgeom,0.5,'endcap=flat join=round')))).*
                FROM (SELECT lane, trace, ST_MakeLine(geom ORDER BY profile) as newgeom FROM %s GROUP BY lane, section_label, trace) as pte GROUP BY pte.lane) as g;

                ALTER TABLE %s ADD COLUMN tid bigserial;""" %(bufferfile, transfile, bufferfile))
conn.commit()
cur.execute("""UPDATE %s set overlap = 'overlap', gid = sgrouptrans.pid
                FROM(
                        SELECT grouptrans.id, grouptrans.pid
                        FROM(
                                SELECT dupetrans.id, max(dupetrans.tid) as pid, count(*)
                                FROM(
                                        SELECT %s.id, section_label, section_chain, filename, trace, profile, %s.lane, %s.priority, %s.tid as tid
                                        FROM %s
                                        INNER JOIN %s on st_contains(%s.geom, %s.geom) or st_touches(%s.geom, %s.geom)
                                        ORDER BY section_chain, priority, profile, tid, lane, trace
                                ) as dupetrans
                                GROUP BY dupetrans.id
                                HAVING count(*) > 1
                                ORDER BY pid, id
                        ) as grouptrans
                ) as sgrouptrans WHERE %s.id = sgrouptrans.id;""" %(transfile, transfile, transfile, transfile, bufferfile, bufferfile, transfile, bufferfile, transfile, bufferfile, transfile, transfile))
conn.commit()
cur.execute("""UPDATE %s set dont_use = 'yes'
                FROM(
                        SELECT gid, max(priority) as maxt FROM %s
                        GROUP BY overlap, gid
                        HAVING overlap = 'overlap'
                        ORDER BY gid
                ) as tempt
                WHERE %s.gid = tempt.gid AND %s.priority <> tempt.maxt""" %(transfile, transfile, transfile, transfile))
conn.commit()
cur.execute("""UPDATE %s
                SET cont_profile = subquery.cont_profile
                FROM(
                        SELECT id, gid, section_chain, section_label, priority, profile, ROW_NUMBER()
                        OVER (PARTITION BY section_label, section_chain ORDER BY section_label, section_chain, priority, profile)-1 AS cont_profile
                        FROM %s
                        WHERE dont_use IS NULL
                ) AS subquery
                WHERE %s.id = subquery.id;""" %(transfile, transfile, transfile))
conn.commit()
cur.execute("""UPDATE %s
                SET dist_from = subquery.dist_from
                FROM(
                        SELECT t.id as fid, st_distance(t.point_to, r.point_from) as dist_from
                    FROM((SELECT id, section_chain, section_label, geom as point_to
                         FROM %s
                         WHERE dont_use IS NULL) as t
                         INNER JOIN
                                (SELECT id, section_chain, section_label, geom as point_from
                                FROM %s
                                WHERE cont_profile = 0) as r
                         ON t.section_label = r.section_label AND t.section_chain = r.section_chain)
                    ORDER BY t.section_label, t.section_chain, dist_from) as subquery
                WHERE %s.id = subquery.fid;""" %(transfile, transfile, transfile, transfile))
conn.commit()
cur.execute("""UPDATE %s
                SET cont_profile = subquery.cont_profile
                FROM(
                        SELECT id, gid, section_chain, dist_from, section_label, priority, profile, ROW_NUMBER()
                        OVER (PARTITION BY section_label, section_chain ORDER BY section_label, section_chain, dist_from)-1 AS cont_profile
                        FROM %s
                        WHERE dont_use IS NULL
                ) AS subquery
                WHERE %s.id = subquery.id;""" %(transfile, transfile, transfile))
conn.commit()
cur.execute("""UPDATE %s SET pc15 = t4.eb_depth
                FROM (
                SELECT
                  *
                FROM (
                  SELECT
                    ROW_NUMBER() OVER (PARTITION BY t2.pid ORDER BY t2.dist) AS r,
                    t2.*
                  FROM
                    (SELECT ST_Distance(t.geom,t1.geom) as dist, t1.id as pid, t1.pc15 as eb_depth, t1.*, t.id as fid, t.* FROM %s t
                    JOIN trans_master_edge_beam_hs t1 ON t.section_label = t1.section_label AND t.section_chain = t1.section_chain
                        WHERE t1.pc15 IS NOT NULL AND t.dont_use IS NULL
                        ORDER BY t.geom <-> t1.geom)
                    t2) t3
                WHERE
                  t3.r <= 1) t4
                  WHERE %s.id = t4.fid;""" %(transfile, transfile, transfile))
conn.commit()
csvfile = open(fpath+'output\\'+ipath+'.csv','w')
SQLstate = ("""COPY (SELECT filename, section_label, section_chain as trans_chain, direction, lane, trace, profile as orig_profile, cont_profile, dist_from, ST_X(geom) as osgb_easting,
                ST_Y(geom) as osgb_northing, pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13, pc14, pc15, pc16, pc17, pc18, pc19, pc20
                FROM %s
                WHERE dont_use IS NULL ORDER BY section_label %s, section_chain, cont_profile) TO STDOUT WITH CSV HEADER;""" % (transfile, dirsort))
print(SQLstate)
cur.copy_expert(SQLstate,csvfile)
csvfile.close()
cur.close()
conn.close()
