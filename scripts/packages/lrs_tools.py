from psycopg2.sql import SQL, Identifier
from scripts.packages.db_tools import MyDatabase


def interpolate_point(schema, geometry, line_table, point_geom, line_field, line_filter):
    db = MyDatabase()
    sql = SQL(
        """
        SELECT
            client_id,
            ST_Distance(line.{_geometry}, (ST_DUMP((%(_point_geom)s))).geom),
            round(ST_InterpolatePoint(line.{_geometry}, (ST_DUMP((%(_point_geom)s))).geom)::numeric,3)
        FROM {_schema}.{_line_table} line
        WHERE {_line_field} = %(_line_filter)s;
        """
    ).format(
        _schema=Identifier(schema), _geometry=Identifier(geometry),
        _line_table=Identifier(line_table), _line_field=Identifier(line_field)
    )
    args = {
        '_point_geom': point_geom, '_line_filter': line_filter
    }
    rs_geom = db.standard_query(sql, args, True)
    db.close()
    return rs_geom


def locate_along(schema, geometry, line_table, line_measure, line_field, line_filter):
    db = MyDatabase()
    sql = SQL(
        """
        SELECT
            route_id,
            ST_LocateAlong(line.{_geometry}, %(_line_measure)s) AS point_geom
        FROM {_schema}.{_line_table} line
        WHERE {_line_field} = %(_line_filter)s
        AND %(_line_measure)s 
            BETWEEN 
                ST_InterpolatePoint({_geometry}, ST_StartPoint({_geometry})) 
                AND 
                ST_InterpolatePoint({_geometry}, ST_EndPoint({_geometry}));
        """
    ).format(
        _schema=Identifier(schema), _geometry=Identifier(geometry),
        _line_table=Identifier(line_table), _line_field=Identifier(line_field)
    )
    args = {
        '_line_measure': line_measure, '_line_filter': line_filter
    }
    rs_geom = db.standard_query(sql, args, True)
    db.close()
    return rs_geom


def locate_between(schema, line_table, line_measure_s, line_measure_e, line_field, line_filter):
    db = MyDatabase()
    sql = SQL(
        """
        SELECT
            ST_LocateBetween(line.geom_m, %(line_measure_s)s, %(line_measure_e)s)
        FROM {schema}.{line_table} line
        WHERE {line_field} = %(line_filter)s;
        """
    ).format(
        schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)
    )
    args = {
        'line_measure_s': line_measure_s, 'line_measure_e': line_measure_e, 'line_filter': line_filter
    }
    rs_geom = db.standard_query(sql, args, True)
    db.close()
    return rs_geom


def add_measure(schema, line_table, line_field, line_filter):
    db = MyDatabase()
    sql = SQL(
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
    ).format(
        schema=Identifier(schema), line_table=Identifier(line_table), line_field=Identifier(line_field)
    )
    args = {
        'line_filter': line_filter
    }
    db.standard_query(sql, args, False)
    db.close()
