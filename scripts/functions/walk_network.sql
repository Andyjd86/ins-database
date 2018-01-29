CREATE OR REPLACE FUNCTION client.walk_network(_dir_key int,
                                              _sect_start varchar(30),
                                              _sect_funct int default 1,
                                              _sect_end varchar(30) default null,
                                              _op_code int default null,
                                              _road int default null
                                              )
    RETURNS VOID AS
$func$
BEGIN

EXECUTE format
    (
    'DROP TABLE client.network_walk;

    -- Create am empty table with auto_ID ready for the data

    CREATE TABLE client.network_walk
    (
        gid int,
        section_label character varying(30),
        dist_from_last double precision,
        fid_trail bigint[]
    );

    -- Starting with a particular section the recursive query looks at the endpoint of that segment and the nearest start point of the next segment
    -- within 1.00m. it continues in this fashion until it reaches the end of the table or a gap bigger than 1.00m
    -- filters can include direction, carriageway type and even an end segment if you want to stop it part way through the table.

    WITH RECURSIVE walk_network(geom, gid, section_label, dist_from_last) AS (
        SELECT geom, fid as gid, section_label, 0.00::float AS dist_from_last, ARRAY[fid] AS trail
            FROM client.hapms_master
            WHERE section_label = $2
        UNION ALL
        SELECT n.geom, n.fid as gid, n.section_label, ST_Distance(ST_EndPoint(w.geom), ST_StartPoint(n.geom)) AS dist_from_last, trail || fid
            FROM client.hapms_master n, walk_network w
            WHERE ST_DWithin(ST_EndPoint(w.geom), ST_StartPoint(n.geom), 6.50)
            AND ($4 is null OR w.section_label != $4)
            AND direction_key = $1
            AND section_function = $3
            AND ($5 is null OR operational_area != $5)
            AND ($6 is null OR road != $6)
    )
    INSERT INTO client.network_walk (gid, section_label, dist_from_last, fid_trail)
        SELECT gid, section_label, dist_from_last, trail::bigint[] AS fid_trail
            FROM walk_network
            ORDER BY trail') USING _dir_key, _sect_start, _sect_funct, _sect_end, _op_code, _road
    ;
END
$func$ LANGUAGE plpgsql;