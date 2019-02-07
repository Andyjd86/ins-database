from psycopg2.sql import SQL, Identifier
from scripts.packages.db_tools import MyDatabase


def create_schema(schema, db_user, schema_comment):
    db = MyDatabase()
    sql = SQL(
        """
        CREATE SCHEMA {_schema}
            AUTHORIZATION {_db_user};
        
        COMMENT ON SCHEMA {_schema}
            IS %(_schema_comment)s;
        """
    ).format(
        _schema=Identifier(schema),
        _db_user=Identifier(db_user)
    )
    args = {
        '_schema_comment': schema_comment
    }
    db.standard_query(sql, args, False)
    db.close()


def create_network_table(network_table):
    db = MyDatabase()
    sql = SQL(
        """
        CREATE TABLE client.{_network_table} 
            (
            road_class integer,
            road_class_code character varying(12),
            road_class_name character varying(12),
            road_class_sort integer,
            road integer,
            road_number character varying(12),
            road_name character varying(72),
            road_sort bigint,
            section bigint,
            section_label character varying(24),
            section_start_date date,
            section_end_date date,
            section_length numeric(12, 3),
            section_function integer,
            section_function_code character varying(24),
            section_function_name character varying(72),
            section_function_sort bigint,
            operational_area integer,
            operational_area_code character varying(12),
            operational_area_name character varying(48),
            operational_area_sort bigint,
            data_key integer,
            direction_key integer,
            direction_code character varying(12),
            direction_name character varying(48),
            direction_sort bigint,
            permanent_lanes numeric(12, 3),
            single_or_dual integer,
            single_or_dual_code character varying(24),
            single_or_dual_name character varying(72),
            single_or_dual_sort bigint,
            environment integer,
            environment_code character varying(12),
            environment_name character varying(48),
            environment_sort bigint,
            local_authority integer,
            local_authority_code character varying(24),
            local_authority_name character varying(72),
            local_authority_sort bigint,
            plan_reference character varying(48),
            start_chainage numeric(12, 3),
            end_chainage numeric(12, 3)
            )
        WITH (
            OIDS = FALSE
        );
        
        ALTER TABLE client.{_network_table}
            OWNER to andydixon;
        """
    ).format(
        _network_table=Identifier(network_table)
    )
    args = {
        None
    }
    db.standard_query(sql, args, False)
    db.close()


def update_network_table(network_table, shape_table):
    db = MyDatabase()
    sql = SQL(
        """
        ALTER TABLE client.{_network_table}
            ADD COLUMN client_id bigserial,
            ADD COLUMN geom geometry,
            ADD COLUMN geom_m geometry;
    
        UPDATE client.{_network_table}
            SET geom = geometry, geom_m = geom_measure
            FROM
                (SELECT
                    geom as geometry,
                    ST_AddMeasure(geom, dsegrefsd, dsegrefed) as geom_measure,
                    sect_label
                FROM client.{_shape_table}
                ) as hapms_transfer
            WHERE section_label = sect_label;
    
        DROP TABLE client.{_shape_table};
        """
    ).format(
        _network_table=Identifier(network_table),
        _shape_table=Identifier(shape_table)
    )
    args = {
        None
    }
    db.standard_query(sql, args, False)
    db.close()


def create_path_tables():
    db = MyDatabase()
    sql = SQL(
        """
        CREATE TABLE client.path_master
        (
            path_id bigserial,
            fid_trail bigint[],
            fid_last bigint,
            geom geometry,
            new_path boolean DEFAULT true
        )
        WITH (
            OIDS = FALSE
        );
    
        ALTER TABLE client.path_master
            OWNER to andydixon;
    
        CREATE TABLE client.project_path
        (
            pk_id bigserial,
            path_id bigint,
            fid bigint,
            path_order bigint
        )
        WITH (
            OIDS = FALSE
        );
    
        ALTER TABLE client.project_path
            OWNER to andydixon;
        """
    ).format(
    )
    args = {
        None
    }
    db.standard_query(sql, args, False)
    db.close()


def create_survey_tables(schema):
    db = MyDatabase()
    sql = SQL(
        """
        CREATE TABLE {_schema}.routes
        (
            route_id bigserial,
            survey_id bigint,
            client_id bigint,
            route_start numeric,
            route_end numeric,
            route_order bigint,
            path_id bigint,
            geom_s geometry(LineStringM,27700),
            geom_c geometry(LineStringM,27700),
            PRIMARY KEY (route_id)
        )
        WITH (
            OIDS = FALSE
        );
        
        ALTER TABLE {_schema}.routes
            OWNER to andydixon;
        """
    ).format(
        _schema=Identifier(schema)
    )
    args = {
        None
    }
    db.standard_query(sql, args, False)
    db.close()


def add_geom(schema, geom_table, geom_name, geom_type, srid):
    db = MyDatabase()
    sql = SQL(
        """
        ALTER TABLE {_schema}.{_geom_table}
            ADD COLUMN {_geom_name} geometry;
        """
    ).format(
        _schema=Identifier(schema),
        _geom_table=Identifier(geom_table),
        _geom_name=Identifier(geom_name)
    )
    args = {
        '_geom_type': geom_type,
        '_srid': srid
    }
    db.standard_query(sql, args, False)
    db.close()
