from scripts.packages.examiner2pgsql import *
from glob import glob
fpath = 'Reporting'
ipath = 'C:\\Users\\adixon\\Desktop\\Projects\\M27 Survey\\' + fpath + '\\'
prj = 4326
new_prj = 4258
trace_file = ipath + 'eb_trans_updater6.csv'
update_trace_table(trace_file)
master_table = 'long_master_eb'
# dir_name = 'EB'
# lane_name = 'L1'
# trace_inc = 0.20092153947742009
# trace_long = 17
# setup_tables(prj, master_table)
for raw_file in glob(path.join(ipath + "input\\", '*.txt')):
    proc_data, create_table, column_header, column_header_list = alter_examiner(ipath, raw_file)
    file_name, lane_name, dir_name, dir_sort, chain_from, chain_to, trace_long, trace_inc = extract_title(raw_file)
    print(create_table)
    import_csv(proc_data, create_table)
    update_csv_table(column_header_list, prj, new_prj, master_table, file_name, chain_from, chain_to, dir_name, lane_name)
    csv_file = open(ipath + 'output\\' + dir_name + '_' + lane_name + '.csv', 'w')
    export_table(csv_file, master_table, lane_name, trace_inc, trace_long, chain_from)
    csv_file.close()
    csv_file_trans = open(ipath + 'output\\' + dir_name + '_' + lane_name + '_trans.csv', 'w')
    export_trans(csv_file_trans, master_table, lane_name, trace_inc, trace_long, chain_from, chain_to, dir_name.lower())
    csv_file_trans.close()
    csv_file_coord = open(ipath + 'output\\' + dir_name + '_' + lane_name + '_coord.csv', 'w')
    export_coord(csv_file_coord, master_table, lane_name, trace_inc, trace_long, chain_from)
    csv_file_coord.close()
    csv_file_coord_trans = open(ipath + 'output\\' + dir_name + '_' + lane_name + '_trans_coord.csv', 'w')
    export_coord_trans(csv_file_coord_trans, master_table, lane_name, trace_inc, trace_long, chain_from, chain_to, dir_name.lower())
    csv_file_coord_trans.close()
# Todo 1 - Export all coordinates after insert
# Todo 2 - Import GridQuest output to new lookup table
# Todo 3 - Function to store coordinates and match to outputs
# Todo 4 - Move export functions to new code call
