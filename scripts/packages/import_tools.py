import os
import subprocess
from psycopg2.sql import SQL, Identifier
from scripts.packages.db_tools import MyDatabase


def shp2psql(base_dir, srid, shp_import):
    # Choose your PostgreSQL version here
    # TODO store in database field?
    os.environ['PATH'] += r';C:\Program Files\PostgreSQL\9.6\bin'
    # http://www.postgresql.org/docs/current/static/libpq-envars.html
    # Password is contained in a file so that it isn't exposed here.
    os.environ['PGHOST'] = 'localhost'
    os.environ['PGPORT'] = '5432'
    os.environ['PGUSER'] = 'andydixon'
    os.environ['PGPASSFILE'] = r'C:\Users\adixon\AppData\Roaming\postgresql\pgpass.conf'
    os.environ['PGDATABASE'] = 'ins_data_dev'

    # Choose base directory to search in, anticipated that this will be fixed and managed by admin.
    # base_dir = r"C:\Users\adixon\Desktop\Projects\INS Database\ins-database\data\survey"
    # srid = '27700'
    # shp_import = 'dfg.survey'
    full_dir = os.walk(base_dir)
    shapefile_list = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp':
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
    # Future development adds all the files to a form to
    # allow tick box selection and addition of table name and SRID values
    # to populate the variables above.
    # Calls the command line function shp2pgsql with
    # several arguments including one to mute the output. Future development
    # should put this output to a log file for error catching.
    for shape_path in shapefile_list:
        cmds = 'shp2pgsql -c -S -I -s "' + srid + '" "' + shape_path + '" "' + shp_import + '" | psql '
        subprocess.run(cmds, shell=True, stdout=subprocess.DEVNULL)


def create_client_tables(schema, import_table, shp_import_table, fpath):
    db = MyDatabase()
    db.call_proc("client.create_table_hapms", [import_table])
    sql = SQL(
        """
        COPY {_schema}.{_import_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',';
        """
    ).format(
        _import_table=Identifier(import_table),
        _schema=Identifier(schema)
    )
    args = open(fpath)
    # print(sql)
    db.copy_expert(sql, args)
    db.call_proc("client.update_geom", [import_table, shp_import_table])
    db.close()
