from psycopg2.sql import SQL, Identifier
import re
from scripts.packages.db_tools import MyDatabase
import os
import glob
import sys


def setup_tables(old_prj, new_prj, master_table):
    db = MyDatabase()
    sql = SQL(
        """
        CREATE TABLE survey.{_mastertable}
            (
                id bigserial,
                filename text,
                survey_id integer,
                survey_date date,
                survey_chain integer,
                survey_dir text,
                trans_dir text,
                file_id text,
                gpr_channel integer,
                trace integer,
                x numeric,
                y numeric,
                height numeric,
                marker text,
                marker_group integer,
                dist_to_line numeric,
                gps_time numeric,
                geom geometry(Point, %(_new_prj)s),
                geom_old geometry(Point, %(_old_prj)s),
                PRIMARY KEY (id)
            )
            WITH (
                OIDS = FALSE
            );
        """
    ).format(
        _mastertable=Identifier(master_table)
    )
    args = {
        '_old_prj': old_prj, '_new_prj': new_prj
    }
    db.standard_query(sql, args, False)
    db.close()


def import_csv(old_prj, new_prj, fpath, master_table, filename, survey_id, survey_chain, survey_dir, trans_dir, file_id, gpr_channel, survey_date):
    db = MyDatabase()
    sql = SQL(
        """
        CREATE TABLE csv_holding
            (
                trace integer,
                distance numeric,
                x numeric,
                y numeric,
                height numeric,
                marker integer,
                gps_time numeric
            );
        """
    ).format(
    )
    args = {
    }
    db.standard_query(sql, args, False)

    sql = SQL(
        """
        COPY {_import_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',';
        """
    ).format(
        _import_table=Identifier('csv_holding')
    )
    args = open(fpath)
    # print(sql)
    db.copy_expert(sql, args)
    update_csv_table(old_prj, new_prj, master_table, filename, survey_id, survey_chain, survey_dir, trans_dir, file_id, gpr_channel, survey_date)
    db.close()


def update_csv_table(old_prj, new_prj, master_table, filename, survey_id, survey_chain, survey_dir, trans_dir, file_id, gpr_channel, survey_date):
    db = MyDatabase()
    sql = SQL(
        """
        INSERT INTO survey.{_master_table}
            (
                filename,
                survey_id,
                survey_date,
                survey_chain,
                survey_dir,
                trans_dir,
                file_id,
                gpr_channel,
                trace,
                x,
                y,
                height,
                marker,
                gps_time,
                geom,
                geom_old
            )
            (
                SELECT
                    %(_filename)s,
                    %(_survey_id)s,
                    %(_survey_date)s,
                    %(_survey_chain)s,
                    %(_survey_dir)s,
                    %(_trans_dir)s,
                    %(_file_id)s,
                    %(_gpr_channel)s,
                    trace,
                    x,
                    y,
                    height,
                    marker,
                    gps_time,
                    ST_SetSRID(ST_MakePoint(x::float, y::float), %(_old_prj)s) as geometry,
                    ST_Transform(ST_SetSRID(ST_MakePoint(x::float, y::float),%(_old_prj)s),%(_new_prj)s) as geom_old
                FROM csv_holding
            );

        DROP TABLE csv_holding;
        """
    ).format(
        _master_table=Identifier(master_table)
    )
    args = {
        '_filename': filename, '_survey_id': survey_id,
        '_survey_chain': survey_chain, '_survey_dir': survey_dir,
        '_trans_dir': trans_dir, '_file_id': file_id,
        '_gpr_channel': gpr_channel, '_survey_date': survey_date,
        '_old_prj': old_prj, '_new_prj': new_prj
    }
    db.standard_query(sql, args, False)
    db.close()
