from scripts.packages.import_tools import *
from scripts.packages.route_tools import *
from scripts.packages.lrs_tools import *


# shp2psql(
#     "C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\survey",
#     '27700',
#     'dfg.survey'
# )

# shp2psql(
#     "C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\spatial",
#     '27700',
#     'client.temp_network'
# )

# create_client_tables('master_network', 'temp_network', 'C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\text\\TRL - Section data_M6_M27_A47.csv')

# directions = [(1, '2400M6/9'), (2, '0900M6/624')]
# for walk_dir in directions:
#     walk_the_network(walk_dir[1], None, walk_dir[0], 1, None, 42)

# TODO
# TODO routes.sql to insert into path_master.
# TODO createpath.sql to update path_master geometry.
# TODO unpack.sql to insert into project_path.
# TODO table creation scripts for all standard tables.

# add_measure('dfg', 'survey', 'collect_id', None)

# create_routes('client', 'master_network', 'dfg', 'survey', 'client_id', 'collect_id')

# select_route_from_path('dfg')

# update_geometry('dfg', 27700)

# build_route_file([1, 2, 3, 4])
