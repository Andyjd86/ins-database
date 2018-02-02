from shapely.geometry import point, linestring, multipoint
from shapely import wkb
import psycopg2
import psycopg2.extras
from psycopg2.sql import SQL, Identifier

# Can you wrap connections into a module??
conn = None
try:
    # Password is contained in a file so that it isn't exposed here.
    conn = psycopg2.connect("dbname = 'ins_data_dev' user = 'andydixon' host = 'localhost'")
except:
    print("I am unable to connect the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
curr = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
curt = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
def junction_select(route_table, vertex_table, junct_road, junct_list, search_rad, sect_funct):
    cur.execute(
        SQL(
            """
            WITH ordered_nearest AS (
                SELECT
                    route.*,
                    vertex.*,
                    route.geom_m AS route_geom,
                    vertex.geom AS vertex_geom,
                    ST_Distance(route.geom_m, vertex.geom) AS distance
                FROM client.{route} route
                JOIN (
                    SELECT *
                    FROM {vertex}
                    WHERE road = %(junct_road)s AND junct_no = ANY (%(junct_list)s)) as vertex
                ON ST_DWithin(route.geom_m, vertex.geom, %(search_rad)s)
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
                WHERE section_function = %(sect_funct)s
                ORDER BY id, direction_key, distance desc) x
            WHERE x.extents <=1;
            """
        ).format(route=Identifier(route_table), vertex=Identifier(vertex_table)),
        {'junct_road': junct_road, 'junct_list': junct_list, 'search_rad': search_rad, 'sect_funct': sect_funct}
    )
    r = cur.fetchall()
    print(r)
    return r


def interpolate_point(schema, line_table, point_geom, line_field, line_filter):
    print(point_geom)
    cur.execute(
        SQL(
            """
            SELECT
                ST_InterpolatePoint(line.geom_m, (ST_DUMP((%(point_geom)s))).geom)
            FROM {schema}.{line_table} line
            WHERE {line_field} = %(line_filter)s;
            """
        ).format(schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)),
        {'point_geom': point_geom, 'line_filter': line_filter}
    )
    conn.commit()
    for return_geom in cur.fetchall():
        return return_geom[0]


def locate_along(schema, line_table, line_measure, line_field, line_filter):
    cur.execute(
        SQL(
            """
            SELECT
                ST_LocateAlong(line.geom_m, %(line_measure)s)
            FROM {schema}.{line_table} line
            WHERE (%(line_filter)s is null OR {line_field} = %(line_filter)s);
            """
        ).format(schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)),
        {'line_measure': line_measure, 'line_filter': line_filter}
    )
    conn.commit()
    for r in cur.fetchall():
        rt = r[0]
        # return_geom = wkb.loads(rt,hex=True)
        print(rt)
        # print(return_geom)
        # return return_geom
        return rt


def locate_between(schema, line_table, line_measure_s, line_measure_e, line_field, line_filter):
    cur.execute(
        SQL(
            """
            SELECT
                ST_LocateBetween(line.geom_m, %(line_measure_s)s, %(line_measure_e)s)
            FROM {schema}.{line_table} line
            WHERE {line_field} = %(line_filter)s;
            """
        ).format(schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)),
        {'line_measure_s': line_measure_s, 'line_measure_e': line_measure_e, 'line_filter': line_filter}
    )
    conn.commit()
    # for return_geom in cur.fetchall():
    # return return_geom[0]


def add_measure(schema, line_table, line_field, line_filter):
    cur.execute(
        SQL(
            """
            UPDATE {schema}.{line_table}
            SET geom_m = add_m.geom_m
            FROM
                (SELECT
                    {line_field},
                    ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS geom_m
                FROM {schema}.{line_table} line
                WHERE (%(line_filter)s is null OR {line_field} = %(line_filter)s)
                ) as add_m
            WHERE {line_table}.{line_field} = add_m.{line_field};
            """
        ).format(schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)),
        {'line_filter': line_filter}
    )
    conn.commit()


def create_routes(schema, route_table, collection_table, route_field, collection_field):
    cur.execute(
        SQL(
            """
            WITH ordered_nearest AS (
            --Query to select the start points of the collection linestring and the closest point along the closest route.
                SELECT
                    ST_GeometryN(route.geom_m,1) AS route_geom,
                    route.{route_field} AS route_gid,
                    route.start_chainage AS route_from,
                    route.end_chainage AS route_to,
                    'start' AS vertex_node,
                    ST_StartPoint(vertex.geom_m) AS vertex_geom,
                    vertex.{collection_field} AS vertex_gid,
                    ST_Distance(route.geom_m, ST_StartPoint(vertex.geom_m)) AS distance
                FROM {schema}.{route_table} route
                JOIN {schema}.{collection_table} vertex
                ON ST_DWithin(route.geom_m, ST_StartPoint(vertex.geom_m), 100)
                UNION ALL
            --Query to select the end points of the collection linestring and the closest point along the closest route.
                SELECT
                    ST_GeometryN(route.geom_m,1) AS route_geom,
                    route.{route_field} AS route_gid,
                    route.start_chainage AS route_from,
                    route.end_chainage AS route_to,
                    'end' AS vertex_node,
                    ST_EndPoint(vertex.geom_m) AS vertex_geom,
                    vertex.{collection_field} AS vertex_gid,
                    ST_Distance(route.geom_m, ST_EndPoint(vertex.geom_m)) AS distance
                FROM {schema}.{route_table} route
                JOIN {schema}.{collection_table} vertex
                ON ST_DWithin(route.geom_m, ST_EndPoint(vertex.geom_m), 100)
            ORDER BY vertex_gid, distance ASC
            )
            -- We use the 'distinct on' PostgreSQL feature to get the first route (the nearest) for each unique vertex gid and route node. We can then
            -- pass that one route into ST_InterpolatePoint along with its candidate vertex to calculate the measure of the route.
    
            SELECT
                DISTINCT ON (vertex_gid, vertex_node)
                vertex_gid,
                route_gid,
                vertex_node,
                route_from,
                route_to,
                ST_InterpolatePoint(route_geom, vertex_geom) AS measure,
                ST_LocateAlong(route_geom,ST_InterpolatePoint(route_geom, vertex_geom)) AS geom,
                distance
            FROM ordered_nearest
            ORDER BY vertex_gid, vertex_node desc, distance;
            """
            ).format(schema=Identifier(schema), route_table=Identifier(route_table),
                     collection_table=Identifier(collection_table),
                     route_field=Identifier(route_field), collection_field=Identifier(collection_field))
    )
    conn.commit()
    while True:
        row = cur.fetchone()
        if row == None:
            break
        print(row)
        route_id = getattr(row, 'route_gid')
        collection_id = getattr(row, 'vertex_gid')
        if getattr(row, 'vertex_node') == 'start':
            route_s = getattr(row, 'measure')
            route_e = getattr(row, 'route_to')
        else:
            route_s = getattr(row, 'route_from')
            route_e = getattr(row, 'measure')
        insert_routes('dfg', collection_id, route_id, route_s, route_e)


def insert_routes(schema, survey_id, client_id, route_start, route_end):
    curr.execute(
        SQL(
            """
            INSERT INTO {_schema}.routes
                (
                    path_id,
                    route_order, 
                    survey_id, 
                    client_id, 
                    route_start, 
                    route_end)
                (
                    SELECT
                        path_id,
                        path_order,
                        %(_survey_id)s, 
                        %(_client_id)s,
                        %(_route_start)s, 
                        %(_route_end)s
                    FROM client.project_path
                    WHERE fid = %(_client_id)s
                );
            """
        ).format(_schema=Identifier(schema)),
        {'_survey_id': survey_id, '_client_id': client_id, '_route_start': route_start, '_route_end': route_end}
    )
    conn.commit()


def select_route_from_path(schema):
    curr.execute(
        SQL(
            """
            SELECT DISTINCT
                survey_id,
                path_id,
                first_value(route_order) OVER w AS path_start,
                last_value(route_order)  OVER w AS path_end
            FROM {_schema}.routes 
            WINDOW w AS (PARTITION BY survey_id ORDER BY route_order, path_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            ORDER BY 1;
            """
        ).format(_schema=Identifier(schema))
    )
    conn.commit()
    while True:
        row = curr.fetchone()
        if row == None:
            break
        survey_id = getattr(row, 'survey_id')
        path_id = getattr(row, 'path_id')
        path_start = getattr(row, 'path_start')
        path_end = getattr(row, 'path_end')

        curt.execute(
            SQL(
                """
                INSERT INTO {_schema}.routes
                    (
                    survey_id,
                    client_id,
                    route_start,
                    route_end,
                    route_order,
                    path_id
                    )
                    (SELECT
                        %(_survey_id)s as survey_id,
                        path.fid as client_id,
                        route.start_chainage as route_start,
                        route.end_chainage as route_end,
                        path.path_order as route_order,
                        path.path_id
                    FROM client.project_path path
                    JOIN client.hapms_master route
                    ON path.fid = route.fid
                    WHERE
                        path_id = %(_path_id)s AND
                        path_order > %(_path_start)s AND
                        path_order < %(_path_end)s
                    );
                """
            ).format(_schema=Identifier(schema)),
            {'_survey_id': survey_id, '_path_id': path_id, '_path_start': path_start, '_path_end': path_end}
        )
        conn.commit()