from psycopg2.sql import SQL, Identifier, Literal
from re import search
from scripts.packages.db_tools import MyDatabase
from os import path


def setup_tables(prj, master_table):

    db = MyDatabase()
    sql = SQL(
        """
            CREATE TABLE survey.{_master_table}
            (
                id bigserial,
                filename text,
                section_chain_from numeric,
                section_chain_to numeric,
                direction text,
                lane text,
                trace integer,
                profile integer,
                x numeric,
                y numeric,
                easting numeric,
                northing numeric,
                l_01 numeric,
                l_02 numeric,
                l_03 numeric,
                l_04 numeric,
                l_05 numeric,
                l_06 numeric,
                l_07 numeric,
                l_08 numeric,
                l_09 numeric,
                l_10 numeric,
                l_11 numeric,
                l_12 numeric,
                l_13 numeric,
                l_14 numeric,
                l_15 numeric,
                l_16 numeric,
                l_17 numeric,
                l_18 numeric,
                l_19 numeric,
                l_20 numeric,
                l_21 numeric,
                l_22 numeric,
                dont_use boolean,
                geom geometry(Point, %(_prj)s)
            );
        """
    ).format(
            _master_table=Identifier(master_table)
        )
    args = {
        '_prj': prj
    }
    db.standard_query(sql, args, False)
    db.close()


def extract_title(raw_file):
    f_name = path.basename(raw_file)
    lane_name = search('[ESNW]B_(.+?)_', (path.basename(raw_file)))
    if lane_name:
        col_lane = lane_name.group(1)
    dir_name = search('(.+?)B_', (path.basename(raw_file)))
    if dir_name:
        col_dir = dir_name.group(1) + 'B'
        if col_dir == 'EB':
            dir_sort = 'ASC'
        else:
            dir_sort = 'DESC'
    chain = search(col_lane + '_(.+?)_', (path.basename(raw_file)))
    if chain:
        col_chain = chain.group(1)
        chain_split = col_chain.split('-')
        chain_from = int(chain_split[0])
        chain_to = int(chain_split[1])
    trace_name = search(str(chain_to) + '_Y(.+?).txt', (path.basename(raw_file)))
    if trace_name:
        col_trace = trace_name.group(1)
    if col_lane == 'L1':
        trace_inc = 0.200953704076476
    elif col_lane == 'HS':
        trace_inc = 0.200778647155883
    elif col_lane == 'L2':
        trace_inc = 0.201027405330988
    elif col_lane == 'L3':
        trace_inc = 0.200988998204387
    elif col_lane == 'HS-A':
        trace_inc = 0.200751233707369
    elif col_lane == 'HS-B':
        trace_inc = 0.200780807774514
    elif col_lane == 'HS-C':
        trace_inc = 0.200751233707369
    elif col_lane == 'HS-D':
        trace_inc = 0.200761371823977
    elif col_lane == 'HS-E':
        trace_inc = 0.200751233707369
    elif col_lane == '-L1':
        trace_inc = 0.200858244515989


    return f_name, col_lane, col_dir, dir_sort, chain_from, chain_to, col_trace, trace_inc


def alter_examiner(ipath, raw_file):
    with open(raw_file, 'r') as read_file:
        proc_file = ipath + 'output\\' + path.basename(raw_file)
        with open(proc_file, 'w') as write_file:
            for read_line in read_file:
                if '#X' in read_line:
                    new_header_list = []
                    create_table_list = []
                    column_header_list = []
                    old_header = read_line.split('\t')
                    val_ignore = 0
                    # t=1
                    for iheader in old_header:
                        print(iheader)
                        if 'Std.dev' in iheader or 'NumValues' in iheader or \
                                'Max' in iheader or 'distribution' in iheader:
                            new_header_list.append('ignore' + str(val_ignore))
                            create_table_list.append('ignore' + str(val_ignore) + ' text')
                            val_ignore += 1
                        elif 'Thickness' in iheader or 'histogram' in iheader or 'Min' in iheader:
                            new_header_list.append('ignore' + str(val_ignore))
                            create_table_list.append('ignore' + str(val_ignore) + ' text')
                            val_ignore += 1
                        elif 'Confidence ((.+?). ' in iheader:
                            new_header_list.append('confidence')
                            create_table_list.append('confidence integer')
                            column_header_list.append('confidence')
                        elif '. ' in iheader:
                            m = search('cm] \((.+?). ', iheader)
                            if m:
                                print(m.group(1))
                                new_header_list.append('l_' + m.group(1))
                                create_table_list.append('l_' + m.group(1) + ' numeric')
                                column_header_list.append('l_' + m.group(1))
                            else:
                                m = search('cm] \((.+?). ', iheader)
                        elif iheader == '#X':
                            new_header_list.append('trace')
                            create_table_list.append('trace integer')
                            column_header_list.append('trace')
                        elif iheader == 'Y':
                            new_header_list.append('profile')
                            create_table_list.append('profile integer')
                            column_header_list.append('profile')
                        elif iheader == 'Northing' or iheader == 'Latitude':
                            new_header_list.append('y')
                            create_table_list.append('y numeric')
                            column_header_list.append('y')
                            col_y = 'y'
                        elif iheader == 'Easting' or iheader == 'Longitude':
                            new_header_list.append('x')
                            create_table_list.append('x numeric')
                            column_header_list.append('x')
                            col_x = 'x'
                        elif iheader == '':
                            new_header_list.append('ignore'+str(val_ignore))
                            create_table_list.append('ignore'+str(val_ignore)+' text')
                            val_ignore += 1
                        elif iheader == '\n' or iheader == '\t':
                            new_header_list.append('ignore'+str(val_ignore)+'\n')
                            create_table_list.append('ignore'+str(val_ignore)+' text')
                            val_ignore += 1
                        elif iheader == '\t':
                            new_header_list.append('ignore'+str(val_ignore))
                            create_table_list.append('ignore'+str(val_ignore)+' text')
                            val_ignore += 1
                        else:
                            new_header_list.append('ignore'+str(val_ignore))
                            create_table_list.append('ignore'+str(val_ignore)+' text')
                            val_ignore += 1
                    new_header = '\t'.join(new_header_list)
                    create_table = ', '.join(create_table_list)
                    column_header = ', '.join(column_header_list)
                    print(create_table)
                    print(column_header_list)
                    write_file.write(new_header)
                elif read_line[0] == '#' or read_line == '\n':
                    continue
                else:
                    write_file.write(read_line)
    return proc_file, create_table, column_header, column_header_list


def import_csv(proc_file, create_table):
    db = MyDatabase()
    sql = "CREATE TABLE survey.csv_holding (" + create_table + ");"
    sql1 = SQL(
        """
        CREATE TABLE survey.{_import_table} ({_create_table});
        """
    ).format(
        _import_table=Identifier('csv_holding'),
        _create_table=SQL(', ').join(Literal(n) for n in create_table)
    )
    args = {
    }
    db.standard_query(sql, args, False)

    sql = SQL(
        """
        COPY survey.{_import_table} FROM STDIN WITH CSV HEADER DELIMITER AS '\t' NULL 'NaN';
        """
    ).format(
        _import_table=Identifier('csv_holding')
    )
    args = open(proc_file)
    db.copy_expert(sql, args)
    db.close()


def update_trace_table(trace_file):
    db = MyDatabase()
    sql = SQL(
        """
        COPY survey.{_import_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',';
        """
    ).format(
        _import_table=Identifier('trans_traces')
    )
    args = open(trace_file)
    db.copy_expert(sql, args)
    db.close()


def update_csv_table(column_header, prj, new_prj, master_table, filename, section_chain_from, section_chain_to,
                     direction, lane):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO survey.{_master_table}
            (
                filename,
                section_chain_from,
                section_chain_to,
                direction,
                lane,
                {_column_header},
                geom,
                easting,
                northing
            )
            (
                SELECT
                    %(_filename)s,
                    %(_section_chain_from)s,
                    %(_section_chain_to)s,
                    %(_direction)s,
                    %(_lane)s,
                    {_column_header},
                    ST_SetSRID(ST_MakePoint(x::float, y::float), %(_prj)s) as geom,
                    ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(x::float, y::float),%(_prj)s),%(_new_prj)s)) as easting,
                    ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(x::float, y::float),%(_prj)s),%(_new_prj)s)) as northing
                FROM survey.csv_holding
            );

        DROP TABLE survey.csv_holding;
        
        UPDATE survey.{_master_table}
        SET dont_use = true
        WHERE
        section_chain_from = %(_section_chain_from)s AND
        l_01 IS NULL AND
        l_02 IS NULL AND
        l_03 IS NULL AND
        l_04 IS NULL AND
        l_05 IS NULL AND
        l_06 IS NULL AND
        l_07 IS NULL AND
        l_08 IS NULL AND
        l_09 IS NULL AND
        l_10 IS NULL AND
        l_11 IS NULL AND
        l_12 IS NULL AND
        l_13 IS NULL AND
        l_14 IS NULL AND
        l_15 IS NULL AND
        l_16 IS NULL AND
        l_17 IS NULL AND
        l_18 IS NULL AND
        l_19 IS NULL AND
        l_20 IS NULL AND
        l_21 IS NULL AND
        l_22 IS NULL;
        
        UPDATE survey.{_master_table}
        SET dont_use = false
        WHERE
        section_chain_from = %(_section_chain_from)s AND
        dont_use IS NULL;
        """
    ).format(
        _master_table=Identifier(master_table),
        _column_header=SQL(', ').join(Identifier(n) for n in column_header)
    )
    args = {
        '_filename': filename, '_section_chain_from': section_chain_from,
        '_section_chain_to': section_chain_to, '_direction': direction,
        '_lane': lane, '_prj': prj, '_new_prj': new_prj
    }
    db.standard_query(sql, args, False)
    db.close()


def export_coord(export_file, master_table, lane, trace_inc, trace_long, section_chain_from):
    db = MyDatabase()
    sql = SQL(
        """
        COPY 
        (
            SELECT DISTINCT ON(chainage)
                profile,
                trace,
                round((trace * {_trace_inc})::numeric, 0) as chainage,
                abs(round((trace * {_trace_inc})::numeric, 2) - round((trace * {_trace_inc})::numeric, 0)) as chainage1,
                round((trace * {_trace_inc})::numeric, 2) as chainage2,
                lane,
                easting,
                northing
            FROM survey.{_master_table} 
            WHERE section_chain_from = {_section_chain_from} AND dont_use = FALSE AND lane = {_lane} AND profile = {_trace_long}
            ORDER BY chainage, chainage1 ASC
        ) 
        TO STDOUT WITH CSV HEADER;
        """
    ).format(
        _master_table=Identifier(master_table),
        _lane=Literal(lane),
        _section_chain_from=Literal(section_chain_from),
        _trace_long=Literal(int(trace_long)),
        _trace_inc=Literal(trace_inc)
    )
    args = export_file
    db.copy_expert(sql, args)
    db.close()


def export_coord_trans(export_file, master_table, lane, trace_inc, trace_long, section_chain_from, section_chain_to, direction):
    db = MyDatabase()
    sql = SQL(
        """
        COPY 
        (
            SELECT
                profile,
                trace,
                round((trace * {_trace_inc})::numeric, 0) as chainage,
                abs(round((trace * {_trace_inc})::numeric, 2) - round((trace * {_trace_inc})::numeric, 0)) as chainage1,
                round((trace * {_trace_inc})::numeric, 2) as chainage2,
                lane,
                easting,
                northing
            FROM survey.{_master_table} 
            WHERE section_chain_from = {_section_chain_from} AND dont_use = FALSE AND lane = {_lane} 
            AND trace IN (SELECT trace FROM survey.trans_traces WHERE lane = {_lane} and direction = {_direction} AND 
            (site_chain BETWEEN LEAST({_section_chain_from},{_section_chain_to}) AND GREATEST({_section_chain_from},{_section_chain_to})))
            ORDER BY chainage, chainage1 ASC
        ) 
        TO STDOUT WITH CSV HEADER;
        """
    ).format(
        _master_table=Identifier(master_table),
        _lane=Literal(lane),
        _section_chain_from=Literal(section_chain_from),
        _section_chain_to=Literal(section_chain_to),
        _trace_long=Literal(int(trace_long)),
        _trace_inc=Literal(trace_inc),
        _direction = Literal(direction)
    )
    args = export_file
    db.copy_expert(sql, args)
    db.close()


def export_table(export_file, master_table, lane, trace_inc, trace_long, section_chain_from):
    db = MyDatabase()
    sql = SQL(
        """
        COPY 
        (
            SELECT DISTINCT ON(chainage)
                profile,
                trace,
                round((trace * {_trace_inc})::numeric, 0) as chainage,
                abs(round((trace * {_trace_inc})::numeric, 2) - round((trace * {_trace_inc})::numeric, 0)) as chainage1,
                round((trace * {_trace_inc})::numeric, 2) as chainage2,
                x,
                y,
                easting,
                northing,
                l_01,
                l_02,
                l_03,
                l_04,
                l_05,
                l_06,
                l_07,
                l_08,
                l_09,
                l_10,
                l_11,
                l_12,
                l_13,
                l_14,
                l_15,
                l_16,
                l_17,
                l_18,
                l_19,
                l_20,
                l_21,
                l_22
            FROM survey.{_master_table} 
            WHERE section_chain_from = {_section_chain_from} AND dont_use = FALSE AND lane = {_lane} AND profile = {_trace_long}
            ORDER BY chainage, chainage1 ASC
        ) 
        TO STDOUT WITH CSV HEADER;
        """
    ).format(
        _master_table=Identifier(master_table),
        _lane=Literal(lane),
        _section_chain_from=Literal(section_chain_from),
        _trace_long=Literal(int(trace_long)),
        _trace_inc=Literal(trace_inc)
    )
    args = export_file
    db.copy_expert(sql, args)
    db.close()


def export_trans(export_file, master_table, lane, trace_inc, trace_long, section_chain_from, section_chain_to, direction):
    db = MyDatabase()
    sql = SQL(
        """
        COPY 
        (
            SELECT
                profile,
                trace,
                round((trace * {_trace_inc})::numeric, 0) as chainage,
                abs(round((trace * {_trace_inc})::numeric, 2) - round((trace * {_trace_inc})::numeric, 0)) as chainage1,
                round((trace * {_trace_inc})::numeric, 2) as chainage2,
                x,
                y,
                easting,
                northing,
                l_01,
                l_02,
                l_03,
                l_04,
                l_05,
                l_06,
                l_07,
                l_08,
                l_09,
                l_10,
                l_11,
                l_12,
                l_13,
                l_14,
                l_15,
                l_16,
                l_17,
                l_18,
                l_19,
                l_20,
                l_21,
                l_22
            FROM survey.{_master_table} 
            WHERE section_chain_from = {_section_chain_from} AND dont_use = FALSE AND lane = {_lane} 
            AND trace IN (SELECT trace FROM survey.trans_traces WHERE lane = {_lane} and direction = {_direction} AND 
            (site_chain BETWEEN LEAST({_section_chain_from},{_section_chain_to}) AND GREATEST({_section_chain_from},{_section_chain_to})))
            ORDER BY chainage, chainage1 ASC
        ) 
        TO STDOUT WITH CSV HEADER;
        """
    ).format(
        _master_table=Identifier(master_table),
        _lane=Literal(lane),
        _section_chain_from=Literal(section_chain_from),
        _section_chain_to=Literal(section_chain_to),
        _trace_long=Literal(int(trace_long)),
        _trace_inc=Literal(trace_inc),
        _direction=Literal(direction)
    )
    args = export_file
    db.copy_expert(sql, args)
    db.close()
