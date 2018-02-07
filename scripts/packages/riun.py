from scripts.packages import listsections

route_table = 'hapms_master'
vertex_table = 'motorway_junctions'
junct_road = 'M6'
sect_funct = 1
junct_list = [13, 15]
search_rad = 100
t = listsections.junction_select(route_table=route_table, vertex_table=vertex_table, junct_road=junct_road, junct_list=junct_list, search_rad=search_rad, sect_funct=sect_funct)
print(t)