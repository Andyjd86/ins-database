import os, subprocess

# Choose your PostgreSQL version here
os.environ['PATH'] += r';C:\Program Files\PostgreSQL\9.6\bin'
# http://www.postgresql.org/docs/current/static/libpq-envars.html
os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGUSER'] = 'andydixon'
os.environ['PGPASSFILE'] = r'C:\Users\adixon\AppData\Roaming\postgresql\pgpass.conf'
os.environ['PGDATABASE'] = 'ins_data_dev'

base_dir = r"C:\Users\adixon\Desktop\Projects\INS Database\ins-database\data\shp"
srid = '27700'
shp_import = 'hapms_temp'
full_dir = os.walk(base_dir)
shapefile_list = []
for source, dirs, files in full_dir:
    for file_ in files:
        if file_[-3:] == 'shp':
            shapefile_path = os.path.join(base_dir, file_)
            shapefile_list.append(shapefile_path)
for shape_path in shapefile_list:
    cmds = 'shp2pgsql -d -I -s "' + srid + '" "' + shape_path + '" "' + shp_import + '" | psql '
    subprocess.call(cmds, shell=True)