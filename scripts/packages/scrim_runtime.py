from scripts.packages.scrim_survey import *
from glob import *
from os import stat


src1 = "//trllimited/data/24850_HS2SCRIMSurveys/1_RawData/1_Inbox"
src = "D:\\HS2 SCRIM\\1_RawData\\1_Inbox"
gps_header = ['evt_ident', 'fixed_val', 'evt_type', 'section_name', 'blank_1', 'blank_2', 'blank_3',
               'chain_1', 'chain_2', 'chain_3', 'chain_4', 'dc_timestamp', 'pulse_count', 'nmea_type',
               'time_utc', 'latitude', 'n_s', 'longitude', 'e_w', 'fix_quality', 'no_satellites',
               'hdop', 'altitude', 'alt_unit', 'geoid_separation', 'geoid_sep_unit', 'diff_age', 'diff_ref_stn']
for shift in sorted(glob(src + '/*')):
    # TODO 5 - remember to update shift dates later from imported data, grab run number from dates in run table
    proj_run_no = path.basename(shift).split('_')[1]
    shift_no = path.basename(shift).split('_')[3]
    shift_check = check_shift('tbl_shift', 'scrim', path.basename(shift))
    print(shift_check[0])
    print(getattr(shift_check[0], 'exists'))
    if getattr(shift_check[0], 'exists') == True:
        continue
    ret_shift_id = shift_create('tbl_shift', 'scrim', shift_no, path.basename(shift), proj_run_no)
# Loop through all DC files and import
# TODO 3 - Alter to match new DC folder structure
# TODO 4 - Add variable tables to an array structure
    for dc_filename in glob(src + '/' + path.basename(shift) + '/DC/**/*.gps', recursive=True):
        teststat = stat(dc_filename)
        print(teststat.st_size)
        if stat(dc_filename).st_size == 0:
            continue
        gps_file = dc_filename
        nbp_file = path.splitext(dc_filename)[0] + '.nbp'
        fea_file = path.splitext(dc_filename)[0] + '.fea'
        vel_file = path.splitext(dc_filename)[0] + '.vel'
        outcome = import_dc_data(gps_file, gps_header, nbp_file, fea_file, vel_file, 'tbl_temp_gps', 'scrim', 'tbl_temp_nbp')
        if outcome is False:
            continue
        print(ret_shift_id)
        run_origin = path.splitext(path.basename(dc_filename))[0]
# TODO 6 - Split by space, a bit prescribed but should be ok for now, with 2 being the run number at the end.
        run_no = run_origin.split(' ')[2]
# Time offset - how many minutes ahead or behind is the DC (if behind use a - symbol)
        time_offset = 0
        ret_survey_id = insert_survey_run('tbl_survey', 'scrim',
                                          ret_shift_id, run_origin, run_no, time_offset)
        insert_dc_data('tbl_gps', 'scrim', ret_survey_id)
        # survey_section = ['gps', 'survey', 'survey', 'survey', 'survey']
        # survey_part = ['gps_li', 'survey_li', 'survey_p1', 'survey_lo', 'survey_all']
        # ei_start = [50, 1, 5, 5, 1]
        # ei_end = [1, 5, 5, 2, 2]
        # chain_type_start = ['min', 'min', 'min', 'max', 'min']
        # chain_type_end = ['min', 'min', 'max', 'min', 'min']
        survey_section = ['gps', 'survey']
        survey_part = [1, 1]
        ei_start = [50, 1]
        ei_end = [1, 2]
        chain_type_start = ['min', 'min']
        chain_type_end = ['min', 'min']
        for x in range(len(survey_section)):
            insert_survey_children('scrim', ret_survey_id, survey_section[x], survey_part[x], ei_start[x], ei_end[x],
                                   chain_type_start[x], chain_type_end[x])
# Loop through all SR files and import
# shift = 'R_3_Shift_08'
# ret_shift_id = 180
# TODO 1 - Alter to match SR folder structure
# TODO 2 - Create processes in scrim_survey.py
    for sr_filename in glob(src + '/' + path.basename(shift) + '/SR/*.lni', recursive=True):
        if stat(sr_filename).st_size == 0:
            continue
        import_lni_data(sr_filename, path.splitext(path.basename(sr_filename))[0], ret_shift_id)
# TODO 7 - Match the data, automatic only for now then manual after
    match_sr_dc(ret_shift_id)
# TODO 8 - Identify lni files that need changing or have 0 mark
    sr_lni_check(src + '/' + path.basename(shift) + '/SR', ret_shift_id)
# TODO 9 - Write new lni files and record offset against record
# TODO 10 - code to import processed SR files
    print('Waiting for enter')
    pause = input('')  # This will wait until you press enter before it continues with the program
    print('You pressed enter!')
    for sr_filename in glob(src + '/' + path.basename(shift) + '/SR/**/*.s01', recursive=True):
        if stat(sr_filename).st_size == 0:
            continue
        s01_file = sr_filename
        mkd_file = path.splitext(sr_filename)[0] + '.mkd'
        mkd_header = ['lni_distance', 'loc_distance', 'marker_no', 'marker_type']
# TODO Urgent - Split off the processed name to give just the s20 name
        sr_join = path.splitext(path.basename(sr_filename))[0]
        sr_join_id = sr_join.split('processed_')[1]
        import_sr_data(s01_file, mkd_file, mkd_header)
        insert_sr_data(sr_join_id)
