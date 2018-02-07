#import ListSections

sect_start = '0900M6/624'
sect_end = None
dir_key = 2
sect_funct = 1
op_code = None
road = 42
#ListSections.walk_the_network(sect_start=sect_start, sect_end=sect_end, op_code=op_code, road=road, dir_key=dir_key, sect_funct=sect_funct)

from scripts.packages.select_by_junction import *
import pprint
schema = 'client'
route_table = 'hapms_master'
vertex_table = 'motorway_junctions'
junct_road = "M6"
sect_funct = 1
junct_list = [13, 15]
search_rad = 100

#P = junction_select(route_table=route_table, vertex_table=vertex_table, junct_road=junct_road, junct_list=junct_list, search_rad=search_rad, sect_funct=sect_funct)
#print(P)
#P = locate_along('client', 'collection', 90, 'collect_id', None)
#print(P)
#T = round(interpolate_point('client', 'hapms_master', P, 'section_label', '3400M6/119'),3)
#print(T)
#T = locate_between('client', 'hapms_master', 90, 210, 'section_label', '3400M6/119')
#print(T)
add_measure('dfg', 'collection', 'collect_id', 4)
#print(P)
#pprint=pprint.PrettyPrinter(indent=4)
create_routes('client', 'hapms_master', 'dfg', 'collection', 'fid', 'collect_id')
select_route_from_path('dfg')
#for t in P:
    #print(t[0,1,2,5])
#print(P)