CREATE OR REPLACE FUNCTION client.walk_network(_dir_key int,
                                              _sect_start varchar(30),
                                              _sect_funct int default null,
                                              _sect_end varchar(30) default null,
                                              _op_code int default null,
                                              _road int default null
                                              )
    RETURNS VOID AS
$func$
BEGIN

EXECUTE format
    (
    '
    -- Create a temporary table with auto_ID ready for the data

    CREATE TEMP TABLE client.network_walk
    (
        fid int,
        section_label character varying(30),
        dist_from_last double precision,
        client_id_trail bigint[]
    );

    -- Starting with a particular section the recursive query looks at the endpoint of that segment and the nearest start point of the next segment
    -- within 1.00m. it continues in this fashion until it reaches the end of the table or a gap bigger than 1.00m
    -- filters can include direction, carriageway type and even an end segment if you want to stop it part way through the table.

    WITH RECURSIVE walk_network(geom, fid, section_label, dist_from_last) AS (
        SELECT geom, client_id as fid, section_label, 0.00::float AS dist_from_last, ARRAY[client_id] AS trail
            FROM client.master_network
            WHERE section_label = $2
        UNION ALL
        SELECT n.geom, n.client_id as fid, n.section_label, ST_Distance(ST_EndPoint(w.geom), ST_StartPoint(n.geom)) AS dist_from_last, trail || client_id
            FROM client.master_network n, walk_network w
            WHERE ST_DWithin(ST_EndPoint(w.geom), ST_StartPoint(n.geom), 6.50)
            AND ($4 is null OR w.section_label != $4)
            AND direction_key = $1
            AND ($3 is null OR section_function = $3)
            AND ($5 is null OR operational_area != $5)
            AND ($6 is null OR road = $6)
    )
    INSERT INTO client.network_walk(fid, section_label, dist_from_last, client_id_trail)
        SELECT fid, section_label, dist_from_last, trail::bigint[] AS client_id_trail
            FROM walk_network
            ORDER BY trail'
    ) USING _dir_key, _sect_start, _sect_funct, _sect_end, _op_code, _road
    ;
END
$func$ LANGUAGE plpgsql;