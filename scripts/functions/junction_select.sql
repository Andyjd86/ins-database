CREATE OR REPLACE FUNCTION client.junction_select(
    _route_table varchar(30),
    _vertex_table varchar(30),
    _junct_road varchar(12),
    _junct_list int[],
    _search_rad int,
    _sect_funct int
)
RETURNS TABLE(
    fid bigint,
    direction_key int
)
AS
$func$
BEGIN

EXECUTE format (
    'WITH ordered_nearest AS (
        SELECT
            route.*,
            vertex.*,
            route.geom_m AS route_geom,
            vertex.geom AS vertex_geom,
            ST_Distance(route.geom_m, vertex.geom) AS distance
        FROM client.%I route
        JOIN (
            SELECT *
            FROM %I
            WHERE road = $1 AND junct_no = ANY ($2)) as vertex
        ON ST_DWithin(route.geom_m, vertex.geom, $3)
        ORDER BY vertex.id, distance ASC
    )

    -- We use the distinct on PostgreSQL feature to get the first route (the nearest) for each unique vertex gid and route node. We can then
    -- pass that one route into ST_InterpolatePoint along with its candidate vertex to calculate the measure of the route.

    SELECT fid, direction_key
    FROM (
        SELECT
            ordered_nearest.*,
            ROW_NUMBER() OVER (PARTITION BY id, direction_key ORDER BY distance DESC) AS extents,
            ST_InterpolatePoint(route_geom, vertex_geom) AS measure,
            ST_LocateAlong(route_geom,ST_InterpolatePoint(route_geom, vertex_geom)) AS geom
        FROM ordered_nearest
        WHERE section_function = $4
        ORDER BY id, direction_key, distance desc) x
    WHERE x.extents <=1', _route_table, _vertex_table) USING _junct_road, _junct_list, _search_rad, _sect_funct
    ;
END
$func$ LANGUAGE plpgsql;