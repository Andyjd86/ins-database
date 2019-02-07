from scripts.packages.import_tools import *
from scripts.packages.route_tools import *
from scripts.packages.lrs_tools import *
from scripts.packages.table_tools import *
from datetime import datetime, date

time_start = datetime.now().time()

# create_schema('client', 'andydixon', 'Holds all the information related to the client network')

# create_path_tables()

create_schema('gpr', 'andydixon', 'Holds all the information related to ground penetrating radar')

create_survey_tables('gpr')

shp2psql(
    "C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\survey",
    '27700',
    'gpr.survey'
)

# shp2psql(
#     "C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\spatial",
#     '27700',
#     'client.temp_network'
# )
#
# create_client_tables(
#     'client',
#     'master_network',
#     'temp_network',
#     'C:\\Users\\adixon\\Desktop\\Projects\\INS Database\\ins-database\\data\\text\\TRL - Section data_M6_M27_A47.csv'
# )

# directions = [(1, '2400M6/9'), (2, '0900M6/624')]
# for walk_dir in directions:
#     walk_the_network(
#         walk_dir[1],
#         None,
#         walk_dir[0],
#         1,
#         None,
#         42
#     )
#
# create_path_geometry()
#
# unpack_paths([1, 3])

add_geom('gpr', 'survey', 'geom_m', 'LineString', 27700)

add_measure(
    'gpr',
    'survey',
    'collect_id',
    None
)

create_routes(
    'client',
    'master_network',
    'gpr',
    'survey',
    'client_id',
    'collect_id'
)

select_route_from_path(
    'gpr'
)

update_route_geometry(
    'gpr',
    27700
)

build_route_file([1, 2, 3, 4])

time_end = datetime.now().time()

total_time = datetime.combine(date.today(), time_end) - datetime.combine(date.today(), time_start)
print(total_time)
