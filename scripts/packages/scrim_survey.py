from psycopg2.sql import SQL, Identifier, Literal, Composed
from scripts.packages.db_tools import MyDatabase
from io import StringIO
from csv import DictReader, DictWriter
from numpy import *
from os import rename, path, makedirs


def shift_create(table_name, survey_type, shift_no, shift_origin, run_id):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO {_survey_type}.{_table_name}
            (
                shift_origin,
                status,
                run_id,
                shift_no
            )
        VALUES
            (
                %(_shift_origin)s,
                'to_be_qc',
                %(_run_id)s,
                %(_shift_no)s
            )
        RETURNING shift_id;
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_shift_origin': shift_origin, '_run_id': run_id,
        '_shift_no': shift_no
    }
    shift_id_key = db.standard_query(sql, args, True)
    for shift in shift_id_key:
        if shift is None:
            break
        shift_id = getattr(shift, 'shift_id')
    db.close()
    return shift_id


def check_shift(table_name, survey_type, shift_origin):
    db = MyDatabase()
    sql = SQL(
        """
        SELECT EXISTS
        (
            SELECT
                shift_id
            FROM {_survey_type}.{_table_name}
            WHERE shift_origin = %(_shift_origin)s
        );
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_shift_origin': shift_origin
    }
    shift_check = db.standard_query(sql, args, True)
    db.close()
    return shift_check


def import_dc_data(gps_file, gps_header, nbp_file, fea_file, vel_file, table_name, survey_type, table_name1):
    gps_output = StringIO(newline='')
    no_rows = True
    with open(gps_file) as csv_file:
        read_file = DictReader(csv_file, fieldnames=gps_header)
        write_file = DictWriter(gps_output, gps_header, delimiter=',', lineterminator='\n')
        write_file.writeheader()

        for csv_row in read_file:
            if csv_row['evt_ident'] == '08':
                print(csv_row.values())
                write_file.writerow(csv_row)
                no_rows = False
        print(gps_output.getvalue())

    # with open('C:\\Users\\adixon\Desktop\\test.csv', 'w') as test_csv:
        # test_csv.write(gps_output.getvalue())

    print(gps_output.getvalue())
    print(gps_output.tell())
    if path.splitext(path.basename(gps_file))[0].split(' ')[2] == '22':
        print('STOP')
    if no_rows is True:
        return False
    gps_output.seek(0)
    db = MyDatabase()
    sql = SQL(
        """
        COPY {_survey_type}.{_table_name} FROM STDIN WITH CSV HEADER;
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = gps_output
    db.copy_expert(sql, args)
    gps_output.close()
    sql = SQL(
        """
        COPY {_survey_type}.{_table_name1} FROM STDIN WITH CSV;
        """
    ).format(
        _table_name1=Identifier(table_name1),
        _survey_type=Identifier(survey_type)
    )
    args = open(nbp_file)
    db.copy_expert(sql, args)

    sql = SQL(
        """
        COPY {_survey_type}.tbl_dc_vel_temp FROM STDIN WITH CSV;
        """
    ).format(
        _survey_type=Identifier(survey_type)
    )
    args = open(vel_file)
    db.copy_expert(sql, args)

    sql = SQL(
        """
        COPY {_survey_type}.tbl_dc_fea_temp FROM STDIN WITH CSV;
        """
    ).format(
        _survey_type=Identifier(survey_type)
    )
    args = open(fea_file)
    db.copy_expert(sql, args)
    db.close()
    return True


def insert_survey_run(table_name, survey_type, shift_id_key, run_origin, run_no, time_offset):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO {_survey_type}.{_table_name}
            (
            shift_id, 
            date_start, 
            date_end, 
            encoder,
            run_origin,
            time_offset,
            run_no
            )
            (
            SELECT 
                %(_shift_id)s as shift_id, 
                a.date_start, 
                a.date_end, 
                b.encoder,
                %(_run_origin)s as run_origin,
                (%(_time_offset)s || ' minutes')::interval as time_offset,
                %(_run_no)s as run_no
            FROM 
                (
                SELECT 
                    MIN(dc_timestamp) as date_start, 
                    MAX(dc_timestamp) as date_end
                FROM {_survey_type}.tbl_temp_gps
                GROUP BY evt_ident 
                HAVING evt_ident = 8
                ) a, 
                (
                SELECT 
                    pulse_count as encoder 
                FROM {_survey_type}.tbl_temp_nbp 
                WHERE evt_ident = 50
                ) b
            )
        RETURNING survey_id
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_shift_id': shift_id_key, '_run_origin': run_origin,
        '_time_offset': time_offset, '_run_no': run_no
    }
    survey_id_key = db.standard_query(sql, args, True)
    for survey in survey_id_key:
        if survey is None:
            break
        survey_id = getattr(survey, 'survey_id')
        print(survey_id)
    db.close()
    return survey_id


def insert_dc_data(table_name, survey_type, survey_id_key):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO {_survey_type}.{_table_name}
            (
            survey_id,
            chain,
            dc_timestamp,
            pulse_count,
            nmea_type,
            time_utc,
            latitude,
            longitude,
            fix_quality,
            no_satellites,
            hdop,
            altitude,
            alt_unit,
            geoid_separation,
            geoid_sep_unit,
            diff_age,
            diff_ref_stn,
            checksum
            )
            (
            SELECT
                %(_survey_id)s as survey_id,
                chain_1,
                dc_timestamp,
                pulse_count,
                nmea_type,
                time_utc,
                CASE lower(n_s)
                    WHEN 'n' THEN
                        (substring(latitude from '\d\d'):: double precision + 
                        substring(latitude from '..\.\d+')::double precision/60)
                    WHEN 's' THEN
                        (-1*(substring(latitude from '\d\d'):: double precision + 
                        substring(latitude from '..\.\d+')::double precision/60))
                END as latitude,
                CASE lower(e_w)
                    WHEN 'e' THEN
                        (substring(longitude from '\d\d\d'):: double precision + 
                        substring(longitude from '..\.\d+')::double precision/60)
                WHEN 'w' THEN
                    (-1*(substring(longitude from '\d\d\d'):: double precision + 
                    substring(longitude from '..\.\d+')::double precision/60))
                END as longitude,
                fix_quality,
                no_satellites,
                hdop,
                altitude,
                alt_unit,
                geoid_separation,
                geoid_sep_unit,
                diff_age,
                split_part(diff_ref_stn, '*', 1) as diff_ref_stn,
                split_part(diff_ref_stn, '*', 2) as checksum
            FROM {_survey_type}.tbl_temp_gps
            );
        
        INSERT INTO {_survey_type}.tbl_nbp
            (
            survey_id,
            evt_ident,
            evt_type,
            section_name,
            chain,
            dc_timestamp,
            pulse_count
            )
            (
            SELECT
                %(_survey_id)s as survey_id,
                evt_ident,
                evt_type,
                section_name,
                chain_1,
                dc_timestamp,
                pulse_count
            FROM {_survey_type}.tbl_temp_nbp
            );      
              
        INSERT INTO {_survey_type}.tbl_dc_fea
            (
            dc_survey_id,
            evt_ident,
            evt_type,
            section_name,
            chain,
            dc_timestamp,
            pulse_count
            )
            (
            SELECT
                %(_survey_id)s as dc_survey_id,
                evt_ident,
                evt_type,
                section_name,
                chain_1,
                dc_timestamp,
                pulse_count
            FROM {_survey_type}.tbl_dc_fea_temp
            );
                
        INSERT INTO {_survey_type}.tbl_dc_vel
            (
            dc_survey_id,
            evt_ident,
            chain,
            speed
            )
            (
            SELECT
                %(_survey_id)s as dc_survey_id,
                evt_ident,
                chain,
                speed
            FROM {_survey_type}.tbl_dc_vel_temp
            );
        
        UPDATE {_survey_type}.{_table_name}
        SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude, altitude, chain),4326),27700);
        
        UPDATE {_survey_type}.tbl_survey b
        SET geom = a.geom
        FROM
            (
            SELECT
                ST_MakeLine(geom ORDER BY time_utc) as geom
            FROM {_survey_type}.tbl_gps
            GROUP BY survey_id
            HAVING survey_id = %(_survey_id)s
            ) as a
        WHERE b.survey_id = %(_survey_id)s;

        DELETE FROM {_survey_type}.tbl_temp_gps;
        
        DELETE FROM {_survey_type}.tbl_temp_nbp;
        
        DELETE FROM {_survey_type}.tbl_dc_fea_temp;        
        
        DELETE FROM {_survey_type}.tbl_dc_vel_temp;
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_survey_id': survey_id_key
    }
    db.standard_query(sql, args, False)
    db.close()
    # db_tidy(survey_type, table_name)
# TODO 3 - Add tidy code into separate def and call, try to make universal


def insert_survey_children(survey_type, survey_id_key, survey_section, survey_part, ei_start, ei_end,
                           chain_type_start, chain_type_end):
    db = MyDatabase()
    sql = Composed(
        [SQL(
            """
            INSERT INTO {_survey_type}.tbl_survey_child
            (
            survey_id,
            survey_section,
            survey_part,
            geom
            )
            (
            SELECT
                s.survey_id,
                %(_survey_section)s as survey_section,
                %(_survey_part)s as survey_part,
                CASE
                    WHEN (end_section - start_section) = 0 THEN
                        Null
                    ELSE
                        (st_dump(st_locatebetween(geom, start_section, end_section))).geom
                    END as geom
            FROM
            (
                (
                SELECT
                    survey_id as ss_id,
            """
            ).format(
                _survey_type=Identifier(survey_type)
            ),
            SQL(chain_type_start),
            SQL(
            """
                (chain) as start_section
            FROM
                {_survey_type}.tbl_nbp
            GROUP BY survey_id, evt_ident
            HAVING survey_id = %(_survey_id)s
                AND evt_ident = %(_ei_start)s
            ) ss
            JOIN
            (
            SELECT
                survey_id as se_id,
            """
            ).format(
                _survey_type=Identifier(survey_type)
            ),
            SQL(chain_type_end),
            SQL(
            """
                    (chain) as end_section
                FROM
                    {_survey_type}.tbl_nbp
                GROUP BY survey_id, evt_ident
                HAVING survey_id = %(_survey_id)s
                    AND evt_ident = %(_ei_end)s
                ) se
                ON
                    ss_id = se_id
            ) si
            JOIN
                {_survey_type}.tbl_survey s
            ON
                si.ss_id = s.survey_id
            )
            """
        ).format(
            _survey_type=Identifier(survey_type)
        )
        ])
    args = {
        '_survey_id': survey_id_key, '_survey_section': survey_section,
        '_survey_part': survey_part, '_ei_start': ei_start,
        '_ei_end': ei_end
    }
    db.standard_query(sql, args, False)
    db.close()


def db_tidy(survey_type, table_name):
    db = MyDatabase()
    sql = SQL(
        """
        --CREATE INDEX {_spatial_key} ON {_survey_type}.{_table_name} USING GIST (geom);
        
        CLUSTER {_survey_type}.{_table_name} USING {_spatial_key};
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type),
        _spatial_key=Identifier(table_name+'gix')
    )
    args = {
    }
    db.standard_query(sql, args, False)
    sql = SQL(
        """
        VACUUM ANALYZE {_survey_type}.{_table_name};
        """
    ).format(
        _table_name=Identifier(table_name),
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_spatial_key': table_name
    }
    db.maintenance_query(sql, args, False)
    db.close()


def survey_matching(survey_type, shift_id):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO {_survey_type}.survey_buffer
        (
        child_id, 
        within_dist, 
        geom
        )
        (
        SELECT
            buffered.child_id,
            buffered.within_dist,
            buffered.geom
        FROM
            ( 
            SELECT 
                child_id,
                5 AS within_dist,
                st_buffer(geom, 2.5::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%' 
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            UNION ALL
            SELECT 
                child_id,
                10 AS within_dist,
                st_buffer(geom, 5::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%' 
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            UNION ALL
            SELECT 
                child_id,
                15 AS within_dist,
                st_buffer(geom, 7.5::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%' 
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            UNION ALL
            SELECT 
                child_id,
                20 AS within_dist,
                st_buffer(geom, 10::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%' 
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            UNION ALL
            SELECT 
                child_id,
                25 AS within_dist,
                st_buffer(geom, 12.5::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%'  
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            UNION ALL
            SELECT 
                child_id,
                30 AS within_dist,
                st_buffer(geom, 15::double precision, 'endcap=round join=round'::text)::geometry(Polygon,27700) AS geom
            FROM {_survey_type}.tbl_survey_child
            WHERE survey_part LIKE 'survey_all%%' 
                AND survey_id IN (SELECT survey_id FROM {_survey_type}.tbl_survey WHERE shift_id IN (%(_shift_id)s))
            ) buffered
        );
        INSERT INTO {_survey_type}.survey_coverage
        (
        client_id,
        section_id,
        child_id,
        one_way,
        direction,
        within_dist,
        covered,
        geom
        )
        (
        SELECT
            DISTINCT ON (coverage.client_id, coverage.section_id, coverage.child_id)
            coverage.client_id,
            coverage.section_id,
            coverage.child_id,
            coverage.one_way,
            coverage.direction,
            coverage.within_dist,
            coverage.covered,
            coverage.geom::geometry(LineStringM,27700) AS geom
        FROM
            (
            SELECT
                network.client_id,
                network.section_id,
                buffer.child_id,
                network.one_way,
                network.direction,
                buffer.within_dist,
                st_covers(buffer.geom, network.geom) AS covered,
                network.geom
            FROM
                (
                SELECT
                    survey_buffer.child_id,
                    survey_buffer.within_dist,
                    survey_buffer.geom
                FROM
                    {_survey_type}.survey_buffer survey_buffer
                WHERE survey_buffer.child_id IN
                    (
                    SELECT
                        child_id
                    FROM
                        {_survey_type}.tbl_survey_child
                    WHERE survey_id IN
                        (
                        SELECT
                            survey_id
                        FROM
                            {_survey_type}.tbl_survey
                        WHERE shift_id IN (%(_shift_id)s)
                        )
                    )
                ) buffer,
                (
                SELECT
                    section.client_id,
                    section.section_id,
                    section.one_way,
                    section.direction,
                    survey_child.child_id,
                    section.geom
                FROM
                    client.tbl_sections section,
                    (
                    SELECT
                        child_id,
                        geom
                    FROM {_survey_type}.tbl_survey_child
                    WHERE survey_id IN
                    (
                        SELECT
                            survey_id
                        FROM {_survey_type}.tbl_survey
                        WHERE shift_id IN (%(_shift_id)s)
                    )
                    ) survey_child
                WHERE st_dwithin(survey_child.geom, section.geom, 300::double precision)
            ) network
            WHERE buffer.child_id = network.child_id
        ) coverage
        WHERE coverage.covered = true
        ORDER BY coverage.client_id, coverage.section_id, coverage.child_id, coverage.within_dist
        );

        INSERT INTO {_survey_type}.survey_points
        (
        client_id,
        child_id,
        section_id,
        within_dist,
        one_way,
        direction,
        node_type,
        length,
        st_distance,
        st_interpolatepoint,
        geom
        )
        (
        SELECT
            lrs_points.client_id,
            lrs_points.child_id,
            lrs_points.section_id,
            lrs_points.within_dist,
            lrs_points.one_way,
            lrs_points.direction,
            lrs_points.node_type,
            lrs_points.length,
            lrs_points.st_distance,
            lrs_points.st_interpolatepoint,
            lrs_points.geom
        FROM
            (
            SELECT
                line.client_id,
                point.child_id,
                point.within_dist,
                point.section_id,
                point.one_way,
                point.direction,
                'start'::text AS node_type,
                mn.length,
                st_distance(line.geom, st_startpoint(point.geom)) AS st_distance,
                st_interpolatepoint(line.geom, st_startpoint(point.geom)) AS st_interpolatepoint,
                (st_dump(st_locatealong(line.geom, st_interpolatepoint(line.geom, st_startpoint(point.geom))))).geom::geometry(PointM,27700) AS geom
            FROM
                client.tbl_sections line
            JOIN
                {_survey_type}.survey_coverage point
            ON
                line.section_id = point.section_id
            JOIN
                master_network mn
            ON
                line.client_id = mn.id
        UNION ALL
        SELECT
            line.client_id,
            point.child_id,
            point.within_dist,
            point.section_id,
            point.one_way,
            point.direction,
            'end'::text AS node_type,
            mn.length,
            st_distance(line.geom, st_endpoint(point.geom)) AS st_distance,
            st_interpolatepoint(line.geom, st_endpoint(point.geom)) AS st_interpolatepoint,
            (st_dump(st_locatealong(line.geom, st_interpolatepoint(line.geom, st_endpoint(point.geom))))).geom::geometry(PointM,27700) AS geom
        FROM
            client.tbl_sections line
        JOIN
            {_survey_type}.survey_coverage point
        ON
            line.section_id = point.section_id
        JOIN
            master_network mn
        ON
            line.client_id = mn.id
        ORDER BY client_id, section_id, child_id, node_type DESC
        ) lrs_points
        WHERE child_id IN
            (
            SELECT
                child_id
            FROM
                {_survey_type}.tbl_survey_child
            WHERE survey_id IN
                (
                SELECT
                    survey_id
                FROM
                    {_survey_type}.tbl_survey
                WHERE shift_id IN (%(_shift_id)s)
                )
            )
        );
        
        INSERT INTO {_survey_type}.tbl_qc_route
        (
        child_id,
        section_id,
        within_dist,
        survey_length,
        section_length,
        hausdorff_dist,
        survey_az,
        section_az,
        geom
        )
        (
        SELECT
            t5.child_id,
            t5.section_id,
            t5.within_dist,
            t5.survey_length,
            t5.section_length,
            st_hausdorffdistance(network.geom, t5.geom) as hausdorff_dist,
            degrees(st_azimuth(st_startpoint(t5.geom), st_endpoint(t5.geom))) as survey_az,
            degrees(st_azimuth(st_startpoint(network.geom), st_endpoint(network.geom))) as section_az,
            t5.geom
        FROM
            (
            SELECT
                *,
                st_length(geom) as survey_length
            FROM
                (
                SELECT
                    sections.client_id,
                    sections.child_id,
                    sections.section_id,
                    sections.within_dist,
                    sections.one_way,
                    sections.direction,
                    sections.length as section_length,
                    sections.start_node,
                    sections.end_node,
                    ST_SETSRID((st_dump(st_locatebetween(a.geom, sections.start_node, sections.end_node))).geom,27700)::geometry(LineStringZM,27700) as geom
                FROM
                    {_survey_type}.tbl_survey_child a,
                    (
                    SELECT
                        sect_node.client_id,
                        sect_node.child_id,
                        sect_node.section_id,
                        sect_node.within_dist,
                        sect_node.one_way,
                        sect_node.direction,
                        sect_node.length,
                        st_interpolatepoint(survey.geom, sect_node.s_geom) as start_node,
                        st_interpolatepoint(survey.geom, sect_node.e_geom) as end_node
                    FROM
                        (
                            (
                            SELECT
                                client_id,
                                child_id,
                                section_id,
                                within_dist,
                                one_way,
                                direction,
                                length,
                                geom as s_geom
                            FROM
                                {_survey_type}.survey_points
                            WHERE node_type = 'start'
                            ) as s_point
                        JOIN
                            (
                            SELECT
                                client_id as e_client_id,
                                child_id as e_child_id,
                                section_id as e_section_id,
                                geom as e_geom
                            FROM
                                {_survey_type}.survey_points
                            WHERE node_type = 'end'
                            ) as e_point
                        ON
                            s_point.client_id = e_point.e_client_id AND
                            s_point.child_id = e_point.e_child_id AND
                            s_point.section_id = e_point.e_section_id
                        ) sect_node
                    JOIN
                        (
                        SELECT
                            child_id,
                            geom
                        FROM
                            {_survey_type}.tbl_survey_child
                        WHERE survey_id IN
                            (
                            SELECT
                                survey_id
                            FROM
                                {_survey_type}.tbl_survey
                            WHERE shift_id IN (%(_shift_id)s)
                            )
                        ) survey
                    ON
                        sect_node.child_id = survey.child_id
                    ORDER BY sect_node.client_id, sect_node.child_id
                    ) as sections
                WHERE a.child_id = sections.child_id AND (sections.start_node != sections.end_node)
                ) as t4
            ) as t5
            JOIN
                client.tbl_sections network
            ON
                network.section_id = t5.section_id
            ORDER BY t5.client_id, t5.section_id, t5.child_id
        );

        UPDATE {_survey_type}.tbl_qc_route
        SET
            abs_bearing = abs(round((1 - abs(section_az - survey_az )/180)::numeric * 100,2))
        WHERE abs_bearing IS NULL;
        """
    ).format(
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_shift_id': shift_id
    }
    db.standard_query(sql, args, False)
    db.close()


def survey_matching1(survey_type, shift_id):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO client.tbl_sections_child
        (
            section_id,
            surv_child_id,
            geom
        )
        (
        SELECT
            section_id,
            surv_child_id,
            ST_AddMeasure(geom, ST_InterpolatePoint(section_geom , ST_StartPoint(c.geom)), 
                ST_InterpolatePoint(section_geom, ST_EndPoint(c.geom)))::geometry(LineStringM, 27700) as geom
        FROM
            (
            SELECT 
                surv_child_id, 
                section_id,
                (ST_Dump(ST_Intersection(a.geom, (ring_geom).geom))).geom::geometry(LineString, 27700) as geom, 
                a.geom as section_geom 
            FROM 
                client.tbl_sections a 
            JOIN 
                (
                SELECT 
                    child_id as surv_child_id, 
                    geom as survey_geom, 
                    (ST_DumpRings((ST_Dump(ST_Buffer(geom, 20::double precision, 
                        'endcap=flat join=round'::text))).geom)) AS ring_geom 
                FROM 
                    scrim.tbl_survey_child 
                WHERE 
                    survey_section = 'survey' AND
                    survey_part != 0 AND
                    survey_id IN 
                        (
                        SELECT 
                            survey_id 
                        FROM 
                            scrim.tbl_survey 
                        WHERE shift_id IN (%(_shift_id)s)
                        )
                ) b
            ON 
                ST_DWithin(a.geom, survey_geom, 50)
            WHERE
                (ring_geom).path[1] = 0
            ) c
            ORDER BY section_id    
        );
        
        WITH survey_select AS 
            (
            SELECT
                sect_child_id,
                surv_child_id, 
                b1.section_id,
                ST_M(ST_EndPoint(b1.geom)) - ST_M(ST_StartPoint(b1.geom)) as section_length,
                ST_M(ST_EndPoint(b2.geom)) - ST_M(ST_StartPoint(b2.geom)) as original_section_length, 
                b1.geom as section_geom, 
                ST_Area((ST_Dump(ST_Buffer(b1.geom, 15::double precision, 
                    'endcap=flat join=round'::text))).geom) as area,
                (ST_Dump(ST_Buffer(b1.geom, 15::double precision, 
                    'endcap=flat join=round'::text))).geom::geometry(Polygon,27700) AS geom 
            FROM 
                client.tbl_sections_child b1
            JOIN 
                client.tbl_sections b2
            ON
                b1.section_id = b2.section_id
            WHERE surv_child_id IN 
                (
                SELECT 
                    child_id 
                FROM 
                    scrim.tbl_survey_child 
                WHERE survey_id IN 
                    (
                    SELECT 
                        survey_id 
                    FROM 
                        scrim.tbl_survey 
                    WHERE 
                        shift_id IN (%(_shift_id)s)
                    )
                )
            )
        
        INSERT INTO {_survey_type}.tbl_qc_route
        (
            child_id,
            section_id,
            sect_child_id,
            within_dist,
            survey_length,
            section_length,
            original_section_length,
            hausdorff_dist,
            survey_az,
            section_az,
            abs_bearing,
            geom
        )
        (
        SELECT
            child_id,
            section_id,
            sect_child_id,
            distance,
            (en - st) as survey_length,
            section_length,
            original_section_length,
            hausdorff_dist,
            survey_az,
            section_az,
            abs_bearing,
            (ST_Dump(ST_LocateBetween(f.geom, st, en))).geom::geometry(LineStringZM, 27700) as geom
        FROM
            (
            SELECT
                child_id,
                section_id,
                sect_child_id,
                distance,
                section_length,
                original_section_length,
                hausdorff_dist,
                survey_az,
                section_az,
                abs_bearing,
                e.geom,
                ST_InterpolatePoint(e.geom, ST_StartPoint(section_geom)) as st, 
                ST_InterpolatePoint(e.geom, ST_EndPoint(section_geom)) as en
            FROM
                (
                SELECT
                    child_id,
                    section_id,
                    sect_child_id,
                    distance,
                    section_length,
                    original_section_length,
                    hausdorff_dist,
                    survey_az,
                    section_az,
                    abs(round((1 - abs(section_az - survey_az )/180)::numeric * 100,2)) as abs_bearing,
                    (ST_Dump(ST_LocateBetween(survey_geom, surv_new_start, 
                        surv_new_end))).geom::geometry(LineStringZM, 27700) as geom,
                    section_geom
                FROM
                    (
                    SELECT 
                        child_id,
                        sect_child_id,
                        section_id, 
                        section_length,
                        original_section_length,
                        ST_Length(geom) as geom_length,
                        ST_InterpolatePoint(survey_geom, ST_StartPoint(c.geom)) as surv_new_start, 
                        ST_InterpolatePoint(survey_geom, ST_EndPoint(c.geom)) as surv_new_end,
                        ST_HausdorffDistance(geom, section_geom) as hausdorff_dist,
                        degrees(ST_Azimuth(ST_StartPoint(section_geom), ST_EndPoint(section_geom))) as section_az,
                        degrees(ST_Azimuth(ST_StartPoint(c.geom), ST_EndPoint(c.geom))) as survey_az,
                        ST_Distance(geom, section_geom) as distance,
                        section_geom,
                        survey_geom,
                        c.geom
                    FROM
                        (
                        SELECT 
                            a.child_id, 
                            sect_child_id,
                            section_id,
                            section_length,
                            original_section_length,
                            (ST_Dump(ST_Intersection(ST_Simplify(a.geom,0.03), 
                                b.geom))).geom::geometry(LineStringZ, 27700) as geom, 
                            a.geom as survey_geom,
                            section_geom
                        FROM
                            scrim.tbl_survey_child a
                        JOIN
                            (
                            SELECT 
                                a1.* 
                            FROM 
                                survey_select a1
                            JOIN 						
                                (
                                SELECT 
                                    sect_child_id, 
                                    surv_child_id, 
                                    section_id, 
                                    max(area) as area 
                                FROM 
                                    survey_select
                                GROUP BY 
                                    sect_child_id, 
                                    surv_child_id, 
                                    section_id
                                ) b1
                                ON 
                                    a1.sect_child_id = b1.sect_child_id AND 
                                    a1.surv_child_id = b1.surv_child_id AND 
                                    a1.section_id = b1.section_id AND 
                                    a1.area = b1.area
                            ) b
                            ON 
                                b.surv_child_id = a.child_id
                        ) c
                    ) d
                    WHERE 
                        (surv_new_end - surv_new_start) > 0
                ) e
            ) f
            WHERE 
                st != en
            ORDER BY 
                child_id ASC
            );
        """
    ).format(
        _survey_type=Identifier(survey_type)
    )
    args = {
        '_shift_id': shift_id
    }
    db.standard_query(sql, args, False)
    db.close()


def survey_qc(survey_type):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO {_survey_type}.tbl_qc_status
        (
        qc_id, 
        status_ts, 
        status_note,
        status_code
        )
        (
        SELECT
            main.qc_id,
            CURRENT_TIMESTAMP,
            CASE
                WHEN
                    (	
                        (main.abs_bearing BETWEEN 70 AND 90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                        OR 
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist > 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                        OR
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)>5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)>10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)>0.1)
                        END = True) 
                    OR
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist > 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)>5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)>10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)>0.1)
                        END = True) 
                    )
                THEN 
                    'manual_check'
                WHEN 
                    (
                        (main.abs_bearing <70)
                    )
                THEN 
                    'auto_failed'
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) <0.2) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN 'partial_failed'
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) BETWEEN 0.2 AND 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN 'partial_recollect'
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) Between 0.8 AND 0.99) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN 'partial_passed'
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2)=1) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN 'auto_passed'
            END as status_note,
            CASE
                WHEN
                    (	
                        (main.abs_bearing BETWEEN 70 AND 90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    OR 
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist > 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    OR
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)>5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)>10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)>0.1)
                        END = True) 
                    OR
                        (main.abs_bearing >70) AND 
                        (main.hausdorff_dist > 20) AND
                        (round((main.section_length / main.original_section_length)::numeric, 2) > 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)>5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)>10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)>0.1)
                        END = True) 
                    )
                THEN 
                    20
                WHEN 
                    (
                        (main.abs_bearing <70)
                    )
                THEN 
                    30
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric,2) <0.2) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN
                    19
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric,2) BETWEEN 0.2 AND 0.79) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN
                    18
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric,2) BETWEEN 0.8 AND 0.99) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN
                    17
                WHEN 
                    (
                        (main.abs_bearing >90) AND 
                        (main.hausdorff_dist < 20) AND
                        (round((main.section_length / main.original_section_length)::numeric,2) =1) AND
                        (CASE
                            WHEN section_length BETWEEN 0 AND 20 THEN (abs(survey_length-section_length)<5)
                            WHEN section_length BETWEEN 20 AND 100 THEN (abs(survey_length-section_length)<10)
                            WHEN section_length > 100 THEN (abs((survey_length-section_length)/section_length)<0.1)
                        END = True) 
                    )
                THEN 10
            END as status_code
        FROM  
            {_survey_type}.tbl_qc_route main
        WHERE main.qc_id NOT IN 
            (
            SELECT 
                qc_id
            FROM
                {_survey_type}.tbl_qc_status b 
            )
        ORDER BY qc_id
        )
        """
    ).format(
        _survey_type=Identifier(survey_type)
    )
    args = {
    }
    db.standard_query(sql, args, False)
    db.close()


def import_lni_data(pathname, sr_origin, shift_id):
    lni_file = open(pathname, "r")
    lni_list = lni_file.readlines()
    lni_file.close()
    sql_mark = []
    for idx, item in enumerate(lni_list):
        if idx == 0:
            sr_ts_begin = item.split(',')[1].strip()
            sr_version = item.split(',')[0].strip()
        elif idx == len(lni_list)-1:
            sr_ts_end = item.strip()
        else:
            # TODO add to an array, loop through array in sql insert
            mark_list = {'mark_code': item.split(',')[0],
                         'mark_distance': item.split(',')[1],
                         'mark_no': item.split(',')[2].strip()}
            sql_mark.append(mark_list)

    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO scrim.tbl_sr_survey
        (
            shift_id,
            date_start,
            date_end,
            sr_origin,
            sr_version
        )
        VALUES
        (
            %(_shift_id)s,
            to_timestamp(%(_sr_ts_begin)s, 'YYMMDDHH24MISS'),
            to_timestamp(%(_sr_ts_end)s, 'YYMMDDHH24MISS'),
            %(_sr_origin)s,
            %(_sr_version)s
        )
        RETURNING sr_survey_id;
        """
    ).format(
    )
    args = {
        '_sr_ts_begin': sr_ts_begin, '_sr_ts_end': sr_ts_end,
        '_sr_origin': sr_origin, '_shift_id': shift_id,
        '_sr_version': sr_version
    }
    sr_id_key = db.standard_query(sql, args, True)
    for sr_key in sr_id_key:
        if sr_key is None:
            break
        sr_survey_id = getattr(sr_key, 'sr_survey_id')

    sql = \
        """
        INSERT INTO 
            scrim.tbl_sr_lni
        (
            sr_survey_id, 
            mark_code, 
            mark_distance, 
            mark_no
        )
        VALUES
            %s
        """
    args = sql_mark
    template = "(" + str(sr_survey_id) + ", %(mark_code)s, %(mark_distance)s, %(mark_no)s)"
    db.execute_lots(sql, args, template, False)


def match_sr_dc(shift_id):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO
            scrim.tbl_sr_dc_match
        (
        sr_survey_id,
        dc_survey_id
        )
        (
        SELECT
            sr_survey_id,
            survey_id as dc_survey_id
        FROM
            sr_dc_matching
        GROUP BY
            shift_id,
            survey_id,
            sr_survey_id,
            matching
        HAVING
            matching = 'pass' AND
            shift_id = %(_shift_id)s
        ORDER BY
            survey_id
        )

        RETURNING sr_dc_match_id;
        """
    ).format(
    )
    args = {
        '_shift_id': shift_id
    }
    sr_dc_match_pass = db.standard_query(sql, args, True)
    print('number of sections matched = ' + str(len(sr_dc_match_pass)))

    sql = SQL(
        """
        SELECT
            sr_survey_id,
            survey_id as dc_survey_id
        FROM
            sr_dc_matching
        GROUP BY
            shift_id,
            survey_id,
            sr_survey_id,
            matching
        HAVING
            matching = 'fail' AND
            shift_id = %(_shift_id)s
        ORDER BY
            survey_id
        """
    ).format(
    )
    args = {
        '_shift_id': shift_id
    }
    sr_dc_match_fail = db.standard_query(sql, args, True)
    print('number of sections failed = ' + str(len(sr_dc_match_fail)))


def sr_lni_check(pathname, shift_id):
    db = MyDatabase()
    sql = SQL(
        """
        UPDATE 
            scrim.tbl_sr_survey e
        SET 
            process = False
        FROM
            (
            SELECT 
                d.sr_origin, c.* 
            FROM
                (
                SELECT 
                    sr_survey_id 
                FROM
                    (
                    SELECT 
                        a.* 
                    FROM 
                        scrim.tbl_sr_lni a
                    WHERE 
                        a.sr_survey_id NOT IN
                            (
                            SELECT 
                                sr_survey_id 
                            FROM 
                                scrim.tbl_sr_lni 
                            GROUP BY 
                                sr_survey_id, 
                                mark_code 
                            HAVING 
                                mark_code = 'm' 
                            ORDER BY 
                                sr_survey_id
                            )
                    ) b
                GROUP BY sr_survey_id
                ) c
            JOIN scrim.tbl_sr_survey d
            ON c.sr_survey_id = d.sr_survey_id
            WHERE shift_id = %(_shift_id)s 
            ) f
        WHERE e.sr_survey_id = f.sr_survey_id
        
        RETURNING e.sr_survey_id;
        """
    ).format(
    )
    args = {
        '_shift_id': shift_id
    }
    sr_lni_edit = db.standard_query(sql, args, True)
    print('number of files changed = ' + str(len(sr_lni_edit)))

    sql = SQL(
        """
        UPDATE
            scrim.tbl_sr_survey a
        SET
            lni_offset = d.lni_offset
        FROM
            (
            SELECT 
                c.sr_origin, 
                b.*, 
                50 - b.mark_distance as lni_offset
            FROM
                scrim.tbl_sr_lni b
            JOIN
                scrim.tbl_sr_survey c
            ON 
                b.sr_survey_id = c.sr_survey_id
            WHERE
                mark_code = 'm' AND 
                mark_no = 0 AND 
                mark_distance <50 AND
                shift_id = %(_shift_id)s 
            ) d
        WHERE
            a.sr_survey_id = d.sr_survey_id

        RETURNING a.sr_survey_id;
        """
    ).format(
    )
    args = {
        '_shift_id': shift_id
    }
    sr_lni_edit = db.standard_query(sql, args, True)
    print('number of files changed = ' + str(len(sr_lni_edit)))
    for lni_edit in sr_lni_edit:
        if lni_edit is None:
            break
        sr_survey_id = getattr(lni_edit, 'sr_survey_id')

        sql = SQL(
            """
            SELECT 
                sr_lni_id,
                a.sr_survey_id,
                mark_no,
                mark_code,
                CASE mark_code
                    WHEN 'e' THEN mark_distance + (lni_offset * 1000)
                    ELSE mark_distance + lni_offset
                END as mark_distance,
                to_char(date_start, 'YYMMDDHH24MISS') as date_start,
                to_char(date_end, 'YYMMDDHH24MISS') as date_end,
                lni_offset,
                sr_origin,
                sr_version,
                process 
            FROM 
                scrim.tbl_sr_lni a
            JOIN
                scrim.tbl_sr_survey b
            ON 
                a.sr_survey_id = b.sr_survey_id
            WHERE
                a.sr_survey_id = %(_sr_survey_id)s AND
                process IS NULL AND
                lni_offset IS NOT NULL
            ORDER BY sr_lni_id
            """
        ).format(
        )
        args = {
            '_sr_survey_id': sr_survey_id
        }
        sr_lni_write = db.standard_query(sql, args, True)
        print(sr_lni_write)
        print(getattr(sr_lni_write[0], 'sr_version'))
        old_path = pathname
        new_path = pathname + '\\old_lni'
        if not path.exists(new_path):
            makedirs(new_path)
        rename(old_path + '\\' + getattr(sr_lni_write[0], 'sr_origin') + '.lni', new_path + '\\' + getattr(sr_lni_write[0], 'sr_origin') + '.lni')
        with open(old_path + '\\' + getattr(sr_lni_write[0], 'sr_origin') + '.lni', mode='w') as the_file:
            the_file.write(getattr(sr_lni_write[0], 'sr_version') + ',' + getattr(sr_lni_write[0], 'date_start') + '\n')
            for lni_write in sr_lni_write:
                if lni_write is None:
                    break
                the_file.write(getattr(lni_write, 'mark_code') + ',' + str(getattr(lni_write, 'mark_distance')) + ',' + str(getattr(lni_write, 'mark_no')) + '\n')
            the_file.write(getattr(lni_write, 'date_end') + '\n')


def import_sr_data(s01_file, mkd_file, mkd_header):
    i = 0
    mkd_output = StringIO(newline='')
    with open(mkd_file) as mkd_file:
        read_file = DictReader(mkd_file, fieldnames=mkd_header)
        write_file = DictWriter(mkd_output, mkd_header, delimiter=',', lineterminator='\n')
        write_file.writeheader()
        for mkd_row in read_file:
            if mkd_row['lni_distance'] == 'original distance':
                i = 1
                continue
            if mkd_row['lni_distance'] == 'route file section lengths':
                break
            if i >= 1:
                if 'SKIP' in mkd_row['lni_distance']:
                    continue
                write_file.writerow(mkd_row)
                continue
            else:
                continue

        print(mkd_output.getvalue())
        mkd_output.seek(0)
        db = MyDatabase()
        sql = SQL(
            """
            COPY scrim.tbl_sr_mkd_temp FROM STDIN WITH CSV HEADER;
            """
        ).format(
        )
        args = mkd_output
        db.copy_expert(sql, args)
        mkd_output.close()

    db = MyDatabase()
    sql = SQL(
        """
        COPY scrim.tbl_sr_s01_temp FROM STDIN WITH CSV;
        """
    ).format(
    )
    args = open(s01_file)
    db.copy_expert(sql, args)


def insert_sr_data(sr_join_id):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO scrim.tbl_sr_s01
            (
            sr_survey_id,
            survey_distance,
            survey_speed,
            status,
            lhs_skid,
            rhs_skid,
            air_temp,
            surface_temp
            )
            (
            SELECT
                sr_survey_id,
                survey_distance,
                survey_speed,
                status,
                lhs_skid,
                rhs_skid,
                air_temp,
                surface_temp
            FROM 
                (
                SELECT 
                    *, 
                    %(_sr_join_id)s::text as sr_origin 
                FROM 
                    scrim.tbl_sr_s01_temp
                ) a
            JOIN
                scrim.tbl_sr_survey b
            ON
                a.sr_origin = b.sr_origin
            );

        INSERT INTO scrim.tbl_sr_mkd
            (
            sr_survey_id,
            lni_distance,
            loc_distance,
            marker_no,
            marker_type
            )
            (
            SELECT
                sr_survey_id,
                lni_distance,
                loc_distance,
                marker_no,
                marker_type
            FROM 
                (
                SELECT 
                    *, 
                    %(_sr_join_id)s::text as sr_origin 
                FROM 
                    scrim.tbl_sr_mkd_temp
                ) a
            JOIN
                scrim.tbl_sr_survey b
            ON
                a.sr_origin = b.sr_origin
            );

        DELETE FROM scrim.tbl_sr_s01_temp;

        DELETE FROM scrim.tbl_sr_mkd_temp;
        """
    ).format(
    )
    args = {
        '_sr_join_id': sr_join_id
    }
    db.standard_query(sql, args, False)
    db.close()
