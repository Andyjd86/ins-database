import os
import subprocess

# Choose your PostgreSQL version here
os.environ['PATH'] += r';C:\Program Files\PostgreSQL\9.6\bin'
# http://www.postgresql.org/docs/current/static/libpq-envars.html
# Password is contained in a file so that it isn't exposed here.
os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGUSER'] = 'andydixon'
os.environ['PGPASSFILE'] = r'C:\Users\adixon\AppData\Roaming\postgresql\pgpass.conf'
os.environ['PGDATABASE'] = 'ins_data_dev'

# Choose base directory to search in, anticipated that this will be fixed and managed by admin.
base_dir = r"C:\Users\adixon\Desktop\Projects\INS Database\ins-database\data\spatial"
srid = '27700'
shp_import = 'client.hapms_temp'
full_dir = os.walk(base_dir)
shapefile_list = []
for source, dirs, files in full_dir:
    for file_ in files:
        if file_[-3:] == 'shp':
            shapefile_path = os.path.join(base_dir, file_)
            shapefile_list.append(shapefile_path)
# Future development adds all the files to a form to allow tick box selection and addition of table name and SRID values
# to populate the variables above.
# Calls the command line function shp2pgsql with several arguments including one to mute the output. Future development
# should put this output to a log file for error catching.
for shape_path in shapefile_list:
    cmds = 'shp2pgsql -S -I -s "' + srid + '" "' + shape_path + '" "' + shp_import + '" | psql '
    subprocess.run(cmds, shell=True, stdout=subprocess.DEVNULL)