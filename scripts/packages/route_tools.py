import datetime
from operator import itemgetter
from jinja2 import Environment, FileSystemLoader
from psycopg2.sql import SQL, Identifier
from scripts.packages.db_tools import MyDatabase


def walk_the_network(sect_start, sect_end=None, dir_key=1, sect_function=None, op_code=None, road=None):
    db = MyDatabase()
    db.call_proc("client.walk_network", [dir_key, sect_start, sect_function, sect_end, op_code, road])


def junction_select(route_table, vertex_table, junction_road, junction_list, search_rad, sect_function):
    db = MyDatabase()
    sql = SQL(
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
                WHERE road = %(junction_road)s AND junction_no = ANY (%(junction_list)s)) as vertex
            ON ST_DWithin(route.geom_m, vertex.geom, %(search_rad)s)
            ORDER BY vertex.id, distance ASC
        )

        -- We use the distinct on PostgreSQL feature to get the first route (the nearest) for each unique vertex gid 
        -- and route node. We can then pass that one route into ST_InterpolatePoint along with its candidate 
        -- vertex to calculate the measure of the route.

        SELECT fid, direction_key
        FROM (
            SELECT
                ordered_nearest.*,
                ROW_NUMBER() OVER (PARTITION BY id, direction_key ORDER BY distance DESC) AS extents,
                ST_InterpolatePoint(route_geom, vertex_geom) AS measure,
                ST_LocateAlong(route_geom,ST_InterpolatePoint(route_geom, vertex_geom)) AS geom
            FROM ordered_nearest
            WHERE section_function = %(sect_function)s
            ORDER BY id, direction_key, distance desc) x
        WHERE x.extents <=1;
        """
    ).format(
        route=Identifier(route_table), vertex=Identifier(vertex_table)
    )
    args = {
        'junction_road': junction_road, 'junction_list': junction_list,
        'search_rad': search_rad, 'sect_function': sect_function
    }
    rs = db.query(sql, args, True)
    # print(rs)
    db.close()
    return rs


def create_routes(route_schema, route_table, survey_schema, collection_table, route_field, collection_field):
    db = MyDatabase()
    sql = SQL(
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
            FROM {route_schema}.{route_table} route
            JOIN {survey_schema}.{collection_table} vertex
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
            FROM {route_schema}.{route_table} route
            JOIN {survey_schema}.{collection_table} vertex
            ON ST_DWithin(route.geom_m, ST_EndPoint(vertex.geom_m), 100)
        ORDER BY vertex_gid, distance ASC
        )
        -- We use the 'distinct on' PostgreSQL feature to get the first route (the nearest) for each unique 
        -- vertex gid and route node. We can then pass that one route into ST_InterpolatePoint along with its
        -- candidate vertex to calculate the measure of the route.
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
    ).format(
        route_schema=Identifier(route_schema), route_table=Identifier(route_table),
        survey_schema=Identifier(survey_schema), collection_table=Identifier(collection_table),
        route_field=Identifier(route_field), collection_field=Identifier(collection_field)
    )
    args = None
    rs_routes = db.query(sql, args, True)
    for route in rs_routes:
        if route is None:
            break
        # print(route)
        route_id = getattr(route, 'route_gid')
        collection_id = getattr(route, 'vertex_gid')
        if getattr(route, 'vertex_node') == 'start':
            route_s = getattr(route, 'measure')
            route_e = getattr(route, 'route_to')
        else:
            route_s = getattr(route, 'route_from')
            route_e = getattr(route, 'measure')
        insert_routes('dfg', collection_id, route_id, route_s, route_e)
    db.close()


def insert_routes(schema, survey_id, client_id, route_start, route_end):
    db = MyDatabase()
    sql = SQL(
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
    ).format(
        _schema=Identifier(schema)
    )
    args = {
        '_survey_id': survey_id, '_client_id': client_id,
        '_route_start': route_start, '_route_end': route_end
    }
    db.query(sql, args, False)
    db.close()


def select_route_from_path(schema):
    db = MyDatabase()
    sql_path = SQL(
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
    ).format(
        _schema=Identifier(schema)
    )
    args_path = None
    rs_path = db.query(sql_path, args_path, True)
    for path in rs_path:
        if path is None:
            break
        survey_id = getattr(path, 'survey_id')
        path_id = getattr(path, 'path_id')
        path_start = getattr(path, 'path_start')
        path_end = getattr(path, 'path_end')

        sql_survey = SQL(
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
                JOIN client.master_network route
                ON path.fid = route.client_id
                WHERE
                    path_id = %(_path_id)s AND
                    path_order > %(_path_start)s AND
                    path_order < %(_path_end)s
                );
            """
        ).format(
            _schema=Identifier(schema)
        )
        args_survey = {
            '_survey_id': survey_id, '_path_id': path_id,
            '_path_start': path_start, '_path_end': path_end
        }
        db.query(sql_survey, args_survey, False)
    db.close()


def build_route_file(survey_list):
    db = MyDatabase()
    sql_layout = SQL(
        """
            SELECT 
                g.survey_id,
                g.num_sections,
                r.road,
                r.direction,
                r.lane,
                r.end_x,
                r.end_y,
                'End' as end_ref_marker
            FROM
                (SELECT 
                    survey_id,
                    COUNT(survey_id) AS num_sections,
                    MAX(route_order) AS order_match
                FROM dfg.routes
                GROUP BY survey_id 
                HAVING survey_id = ANY (%(_survey_list)s)
                ) AS g
            JOIN
                (SELECT
                    route.survey_id,
                    hapms.road_number as road,
                    hapms.direction_code as direction,
                    collection.lane as lane,
                    route.route_order,
                    ST_X((ST_Dump(ST_LocateAlong(hapms.geom_m,route.route_end))).geom) as end_x, 
                    ST_Y((ST_Dump(ST_LocateAlong(hapms.geom_m,route.route_end))).geom) as end_y
                FROM dfg.routes route
                INNER JOIN client.master_network hapms ON route.client_id = hapms.client_id
                INNER JOIN dfg.survey collection ON route.survey_id = collection.collect_id
                ORDER BY route.survey_id, route.route_order
                ) as r
            ON g.survey_id = r.survey_id AND g.order_match = r.route_order;
        """
    ).format()
    args_layout = {
        '_survey_list': survey_list
    }
    rs_layout = db.query(sql_layout, args_layout, True)
    for layout in rs_layout:
        if layout is None:
            break
        # print(layout)
        sql_template = SQL(
            """
                SELECT
                    route.route_id,
                    route.survey_id,
                    hapms.section_label,
                    hapms.section_start_date,
                    Null as section_end_date,
                    hapms.section_function_code,
                    hapms.start_chainage,
                    hapms.end_chainage,
                    hapms.direction_code,
                    collection.lane,
                    route.route_start,
                    route.route_end,
                    ST_X((ST_Dump(ST_LocateAlong(route.geom_c, route.route_start))).geom) as start_x,
                    ST_Y((ST_Dump(ST_LocateAlong(route.geom_c, route.route_start))).geom) as start_y,
                    ST_X((ST_Dump(ST_LocateAlong(route.geom_c, route.route_end))).geom) as end_x, 
                    ST_Y((ST_Dump(ST_LocateAlong(route.geom_c, route.route_end))).geom) as end_y
                FROM dfg.routes route
                INNER JOIN client.master_network hapms ON route.client_id = hapms.client_id
                INNER JOIN dfg.survey collection ON route.survey_id = collection.collect_id
                WHERE route.survey_id = %(_survey_id)s
                ORDER BY route.survey_id, route.route_order;
            """
        ).format()
        args_template = {
            '_survey_id': getattr(layout, 'survey_id')
        }
        rs_template = db.query(sql_template, args_template, True)
        template = rs_template
        if template is None:
            break
        # print(template)
        print_rte(layout, template)
    db.close()


def print_rte(layout, template):
    filename = (
            str(datetime.date.today()) + " " + getattr(layout, 'road') + " " +
            str(getattr(layout, 'direction')) + " " + "L" + str(getattr(layout, 'lane')))
    sections = sorted(template, key=itemgetter(2))
    # print(sections)
    temp_env = Environment(
        loader=FileSystemLoader("C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\text"),
        trim_blocks=True)
    temp_out = temp_env.get_template('route_template.rte')
    temp_out.stream(
            file_version='V1',
            route_identifier=filename,
            survey_count=getattr(layout, 'num_sections'),
            survey_lanes=template,
            end_ref_marker='End',
            end_x=getattr(layout, 'end_x'),
            end_y=getattr(layout, 'end_y'),
            sections=sections
    ).dump("C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\text\\" + filename + ".rte")


def update_geometry(schema, srid):
    db = MyDatabase()
    sql = SQL(
        """
        UPDATE {_schema}.routes
        SET geom_s = ST_SetSRID((ST_Dump(create_between.geom_s)).geom,%(_srid)s)
        FROM
            (SELECT
                collect.id,
                locate_dfg.route_id,
                ST_LocateBetween(collect.geom_m, locate_dfg.start_measure, locate_dfg.end_measure) as geom_s
            FROM {_schema}.survey collect
            JOIN
                (SELECT
                    collects.id,
                    locate_hapms.route_id,
                    locate_hapms.survey_id,
                    ST_InterpolatePoint(collects.geom_m, (ST_DUMP(locate_hapms.start_point)).geom) as start_measure,
                    ST_InterpolatePoint(collects.geom_m, (ST_DUMP(locate_hapms.end_point)).geom) as end_measure
                FROM {_schema}.survey collects
                JOIN
                    (SELECT
                        route_id,
                        survey_id,
                        route_order,
                        ST_LocateAlong(line.geom_m, route.route_start) as start_point,
                        ST_LocateAlong(line.geom_m, route.route_end) as end_point
                    FROM client.master_network line
                    JOIN {_schema}.routes route
                    ON line.client_id = route.client_id
                    ) AS locate_hapms
                ON collects.collect_id = locate_hapms.survey_id
                ORDER BY survey_id, route_order
                ) AS locate_dfg
            ON collect.collect_id = locate_dfg.id
            ) AS create_between
        WHERE {_schema}.routes.route_id = create_between.route_id;

        UPDATE {_schema}.routes
        SET geom_c = ST_SetSRID(ST_AddMeasure(geom_s,route_start,route_end),%(_srid)s);
        """
    ).format(
        _schema=Identifier(schema)
    )
    args = {
        '_srid': srid
    }
    db.query(sql, args, False)
    db.close()
