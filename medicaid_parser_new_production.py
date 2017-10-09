import cx_Oracle
import mysql.connector
import datetime
import csv
import sys
import re
import string
import itertools
import os
import time


# class for custom exceptions
class MyException(Exception):
    """Custom exception for intuitive error messages"""
    pass


# Extract CLI arguments
args = sys.argv
invoice_file = args[1]
file = args[1].replace('\\', '\\\\')
key = args[2]
inv_qtr = args[3]
labeler = args[4]

# MySQL connection
con = mysql.connector.connect(user='root', password='ncsuball', host='127.0.0.1',
                              database='medicaid')
cursor = con.cursor()

# Quarter Conversion map for period_type : 'MM/DD/YY' & 'MM/DD/YYYY'
qmap = {'01': '1', '02': '1', '03': '1', '04': '2', '05': '2', '06': '2', '07': '3', '08': '3', '09': '3', '10': '4',
        '11': '4', '12': '4'}
# Quarter Conversion map for period_type : '# QUARTER YYYY'
textqmap = {'FIRST QUARTER': 'Q1', 'SECOND QUARTER': 'Q2', 'THIRD QUARTER': 'Q3', 'FOURTH QUARTER': 'Q4'}

# print items

# global initializations
row_count = 0
error_count = 0
unknown_error_count = 0
total_error_count = 0
total_error_percent = 0.0
detail_row_count = 0
garbage = ['$', ',', '*', ',', '(', ',', ')', ',', '\'']
ptype = ''
inv_num = ''
util_qtr = ''
unknown_errors = []
errors = []
human = []
cms = []
mco_lookup = ['MCO', 'Managed']
ffs_lookup = ['FFS']
cmsd = {}
logic = {}
ocr_details_list = []
ocr_error_list = []
year = ''
quarter = ''
prior_qtr_flag = 'n'

querystring = (
                  "SELECT * FROM invoice_logic il LEFT JOIN invoice_types it ON il.invoice_type = it.id WHERE il.`key` = '%s'") % key
cursor.execute(querystring)
field_names = [d[0].lower() for d in cursor.description]
rows = cursor.fetchmany()

for row in rows:
    logic.update(dict(itertools.izip(field_names, row)))

lines = open(file).read().splitlines()

# retrieve and initialize global invoice logic
try:
    state = str(logic['state'])
    type = str(logic['type'])
    std_type = str(logic['std_type'])
    rtype = std_type
except KeyError:
    with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\log.txt', 'ab') as output_file:
        print str(key + ' not yet added to OCR tool, please contact brian.coleman@cumberlandcg.com')
        output_file.write(str(
            datetime.datetime.now()) + ' ' + key + ' not yet added to OCR tool, please contact brian.coleman@cumberlandcg.com' + '\r\n')

for row in lines:
    ocr_details = {
        "row_number": None,
        "detail_row_number": None,
        "ndc": None,
        "ura": None,
        "units": None,
        "claimed": None,
        "scripts": None,
        "medi_reimbursed": None,
        "non_medi_reimbursed": None,
        "raw_detail_row": None,
        "raw_info_list": None,
        "invoice_qtr": None,
        "utilization_qtr": None,
        "invoice_number": None,
        "error_type": None,
        "error_message": None
    }
    ocr_errors = {}
    row_count += 1
    row_clean = ''  # every row of raw text after performing a cleaning action and uppercased
    info = ''  # intermediate string(row) to extract specific details
    info_list = ''  # row_clean split into a list
    print 'checkpoint 1'
    try:
        # garbage removal and converting to upper case
        row_upper = row.upper()
        row_clean = row_upper
        for ch in garbage:
            row_clean = row_clean.replace(ch, '')

        # Identify Invoice number
        if not inv_num:
            if logic['inv_num'] in row_clean:
                info = row_clean.split(logic['inv_num'])
                inv_num = info[1].strip(' ')

        # identification of invoice type if differs from type derived from file_name
        for x in mco_lookup:
            if x in row_clean:
                ptype = 'MCOU'
        for x in ffs_lookup:
            if x in row_clean:
                ptype = 'FFSU'
        if ptype != rtype and ptype != '':
            rtype = ptype

        # identification of billing quarter
        for x in logic['period_covered'].split('|'):
            if x in row_clean:
                # ~ print row, x
                if logic['period_type'] == '#QYYYY':
                    info = row_clean.split(str(logic['period_covered']) + ': ')
                    util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                elif logic['period_type'] == 'YYYY#':
                    info = row_clean.split(str(logic['period_covered']) + ' ')
                    util_qtr = info[1][4:5].replace('L', '1') + info[1][:4]
                elif logic['period_type'] == '#/YYYY':
                    info = row_clean.split(str(logic['period_covered']) + ': ')
                    util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                elif logic['period_type'] == '#-YYYY':
                    info = row_clean.split(str(logic['period_covered']) + ': ')
                    util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                elif logic['period_type'] == '#YYYY':
                    info = row_clean.split(str(logic['period_covered']) + ': ')
                    util_qtr = info[1][:1].replace('L', '1') + info[1][1:5]
                # DN - Added for CA - New  YYYY/Q
                elif logic['period_type'] == 'YYYY/#':
                    info = row_clean.split(str(logic['period_covered']) + ' ')
                    util_qtr = info[1][5:6].replace('L', '1') + info[1][:4]
                elif logic['period_type'] == 'MM/DD/YY':
                    info = row_clean.split(str(logic['period_covered']) + ': ')
                    util_qtr = qmap[info[1][:2]] + '20' + info[1][6:8]
                elif logic['period_type'] == 'YYYYbreakQ':
                    info = row_clean.split(str(x) + ' ')
                    if x == 'YEAR':
                        year = info[1][:4]
                    elif x == 'QTR':
                        quarter = info[1][:1]
                elif logic['period_type'] == '# QUARTER YYYY':
                    quarter = textqmap[x]
                    info = row_clean.split(str(x))
                    year = info[1].strip(' ')
                    util_qtr = quarter[1:2] + year[:4]
                elif logic['period_type'] == 'MM/DD/YYYY':
                    info = row_clean.split(str(logic['period_covered']) + ' ')
                    month = info[1][:2]
                    year = info[1][6:10]
                    quarter = qmap[str(month)]
                    util_qtr = quarter + year[:4]
        print 'checkpoint 2'
        if logic['period_type'] == 'YYYYbreakQ' and year != '' and quarter != '':
            util_qtr = quarter + year

        # if billing quarter is not listed in the file, default to invoice quarter
        if not util_qtr:
            util_qtr = inv_qtr[5:6] + inv_qtr[:4]

        # determine if we have passed into prior quarter pages (from submission/current quarter)
        # if prior quarter id field is present, this means the state/program has a different format for prior quarter detail and needs to be parsed with a different format
        if logic['prior_quarter_id']:
            if logic['prior_quarter_id'] in row_clean:
                prior_qtr_flag = 'y'

        # Checking URA/pq_URA and NDC/pq_NDC existence to determine if row_clean is a detail row or not
        if prior_qtr_flag == 'n':
            if logic['ura_length']:
                length = int(logic['ura_length'])
                pattern = r"\d+\.\d{%d}" % length
                ura_string = re.findall(pattern, row_clean)
                ura_pattern = r"\d*[.,]\d{%d}" % length
                ura_detail_check = re.findall(ura_pattern, row_clean)
                ndc_no_hyphen = row_clean.replace('-', '').replace('O', '0')
                ndc_pattern = r"^\d{11}"
                ndc_detail_check = re.findall(ndc_pattern, ndc_no_hyphen)
                # print str(ndc_detail_check)

        else:
            if logic['pq_ura_length']:
                length = int(logic['pq_ura_length'])
                pattern = r"\d+\.\d{%d}" % length
                pq_ura_string = re.findall(pattern, row_clean)
                ura_pattern = r"\d*[.,]\d{%d}" % length
                ura_detail_check = re.findall(ura_pattern, row_clean)
                ndc_no_hyphen = row_clean.replace('-', '').replace('O', '0')
                ndc_pattern = r"^\d{11}"
                ndc_detail_check = re.findall(ndc_pattern, ndc_no_hyphen)

        detail_row_flag = 'n'

        # Get utilization quarter if not provided in the header section of the invoice
        # set placeholder value for util qtr if it trails the detail lines in the files
        if logic['pq_pc_trailing'] == 'Y' and prior_qtr_flag == 'y':
            util_qtr = '11900'

        # if util qtr trails detail lines - pick it up when found and use that value to replace the placeholder in the list
        if logic['pq_pc_trailing'] == 'Y' and prior_qtr_flag == 'y':
            if str(logic['pq_period_covered']) in row_clean:
                util_qtr_o = row_clean.split(str(logic['pq_period_covered']))[0].strip().replace('-', '').replace('O',
                                                                                                                  '0')
                if str(logic['pq_period_type']) == '#QYYYY':
                    util_qtr = str(str(util_qtr_o)[:1] + str(util_qtr_o)[2:6])
                    if len(str(util_qtr).strip()) != 5:
                        raise MyException(
                            'utilization quarter did not translate properly, correct and re-run as this will impact any related lines')
                else:
                    util_qtr = util_qtr

                for idx, item in enumerate(cms):
                    if '11900' in item:
                        cms[idx] = item.replace('11900', str(util_qtr))

        # check if current row is a detail_row & set the detail_flag
        if ura_detail_check or ndc_detail_check:
            detail_row_flag = 'y'
        print 'checkpoint 3'
        # Extract all information if current row is a detail row
        if detail_row_flag == 'y':
            detail_row_count += 1
            print 'checkpoint 4'
            # setting ocr_details table info
            ocr_details["row_number"] = row_count
            ocr_details["raw_detail_row"] = str(row_upper)  # upper cased raw detail row
            ocr_details["detail_row_number"] = detail_row_count
            ocr_details["invoice_number"] = str(inv_num).replace(': ', '')
            # process inv_qtr into date format before insert
            if 'Q' in inv_qtr:
                split_inv_qtr = str(inv_qtr).split('Q')
            elif 'q' in inv_qtr:
                split_inv_qtr = str(inv_qtr).split('q')
            else:
                raise MyException('invoice quarter in filename is of incorrect format')

            temp_qtr_date = ''
            print 'checkpoint 9'
            if split_inv_qtr[1] == '1':
                temp_qtr_date = split_inv_qtr[0] + '/01/01'
            elif split_inv_qtr[1] == '2':
                temp_qtr_date = split_inv_qtr[0] + '/04/01'
            elif split_inv_qtr[1] == '3':
                temp_qtr_date = split_inv_qtr[0] + '/07/01'
            elif split_inv_qtr[1] == '4':
                temp_qtr_date = split_inv_qtr[0] + '/10/01'

            ocr_details["invoice_qtr"] = str(temp_qtr_date)

            # Get utilization quarter from the line item info
            if prior_qtr_flag == 'y':
                # extract period when prior quarter period is a part of the detail row
                if logic['detail_qtr']:
                    pre_info = row_clean.split(' ')
                    if logic['period_type'] == 'YYYYQ#':
                        util_qtr_o = str(pre_info[int(logic['detail_qtr'])])
                        util_qtr = util_qtr_o[5:6] + util_qtr_o[:4]
                    if logic['period_type'] == '#/YYYY':
                        util_qtr_o = str(pre_info[int(logic['detail_qtr'])])
                        util_qtr = util_qtr_o[:1] + util_qtr_o[2:6]
                    row_clean = row_clean.replace(util_qtr_o, '')

            # process util_qtr into date format before insert
            temp_util_date = ''
            temp_qtr = util_qtr[0]
            temp_year = util_qtr[-4:]
            if temp_qtr == '1':
                temp_util_date = temp_year + '/01/01'
            elif temp_qtr == '2':
                temp_util_date = temp_year + '/04/01'
            elif temp_qtr == '3':
                temp_util_date = temp_year + '/07/01'
            elif temp_qtr == '4':
                temp_util_date = temp_year + '/10/01'
            ocr_details["utilization_qtr"] = str(temp_util_date)
            # add labeler code for invoices that only include product code and size - requires ndc to be the first field
            if str(logic['ndc']) == '0':
                checkr = re.sub("[^0-9\d.\-\s]", "", row_clean)
                if len(str(checkr)) >= 5:
                    if not checkr[:5].strip().isdigit():
                        row_clean = labeler + '-' + row_clean

            # TODO check for dashes and add if necessary

            # remove spaces and hyphens in ndc area - requires ndc to be first field
            ndc_area = ''
            if logic['invoice_type'] == '1' or logic['invoice_type'] == '3':
                ndc_area = row_clean[:13]
            if logic['invoice_type'] == '2':
                ndc_area = row_clean[:11]

            # TODO: optimize conditional statements
            if prior_qtr_flag == 'n':
                if str(logic['ndc']) == '0':
                    row_clean = row_clean.replace(ndc_area, ndc_area.replace(' ', ''))
            else:
                if str(logic['pq_ndc']) == '0':
                    row_clean = row_clean.replace(ndc_area, ndc_area.replace(' ', ''))
            # Remove product description
            # If ura is present after product description, use ura to determine product description end position and remove product description
            if prior_qtr_flag == 'n':
                if logic['prod_desc'] != 'n':
                    # identifies product name start position with first space
                    start = row_clean.index(' ')
                    # identifies product end position as the index before start of the ura_string
                    end = row_clean.index(str(ura_string[0])) - 1
                    row_clean = row_clean.replace(row_clean[start:end], '')
            else:
                if logic['pq_prod_desc'] != 'n':
                    # identifies product name start position with first space
                    start = row_clean.index(' ')
                    end = row_clean.index(str(pq_ura_string[0])) - 1
                    row_clean = row_clean.replace(row_clean[start:end], '')
            print 'checkpoint 5'
            # Additional cleanup of row
            # DN - replace any single dot, i.e. led and followed by space with just space, double space -> single space
            row_clean = re.sub("[^0-9\d.\-\s]", "", row_clean)
            row_clean = row_clean.replace('  ', ' ').replace('  ', ' ').replace(' . ', ' ')

            # ~ if logic['invoice_type'] == 1:
            # ~ get_ndc = row_clean[:5]+row_clean[6:10]+row_clean[11:13]
            # ~ else:
            # ~ get_ndc = row_clean[:11]

            # Split out detail lines into a list of elements
            info_list = row_clean.split(' ')

            # # remove del_col columns
            # '''if length of the elements is longer than expected,
            # remove the delete columns(del_col) from the list
            # (only for invoices where current and prior format is the same)'''
            # if prior_qtr_flag == 'n':
            #     if logic['del_col'] and logic['exp_col']:
            #         if len(info_list) > int(logic['exp_col']):
            #             for x in logic['del_col'].split('|'):
            #                 del info_list[int(x) - 1:int(x)]
            print 'checkpoint 6'
            # Get NDC
            if prior_qtr_flag == 'n':
                # ~ ndc = get_ndc.replace('O','0').replace('()','0')
                ndc = info_list[int(logic['ndc'])].replace('-', '').replace('O', '0').replace('()', '0')
            else:
                ndc = info_list[int(logic['pq_ndc'])].replace('-', '').replace('O', '0').replace('()', '0')

            # quality check - ndc field
            if len(str(ndc).strip()) != 11:
                raise MyException('ndc is not 11 characters, possible OCR misread, please review')
            if not str(ndc).strip().replace('-', '').isdigit():
                raise MyException('ndc has non-numeric characters, possible OCR misread, please review')

            ocr_details["ndc"] = str(ndc)

            # Extract labeler, prod and size from ndc(11)
            labeler = ndc[:5]
            prod = ndc[5:9]
            size = ndc[9:11]
            print 'checkpoint 7'
            # Default Name to blank with correct spacing
            name = str("          ")

            # clean up multiple decimal points when tesseract misinterprets commas, also throw error if tesseract misinterprates decimal for a comma
            for idx, item in enumerate(info_list):
                item = str(item).replace('l', '1').replace('O', '0').replace('()', '0')
                info_list[idx] = item
                if str(item).count(".") > 1:
                    item = str(item).replace('.', '', 1)
                    info_list[idx] = item
                if str(item).count(",") > 1:
                    raise MyException('tesseract misread decimal for a comma - please fix manually')

            # handling for columns in sections that are only sometimes populated
            if prior_qtr_flag == 'n':
                if logic['exp_col'] and logic['part_col']:
                    if len(info_list) < int(logic['exp_col']) and logic['part_col']:
                        info_list = info_list[:int(logic['part_col'])] + ['0'] + info_list[int(logic['part_col']):]
            else:
                if logic['pq_exp_col'] and logic['pq_part_col']:
                    if len(info_list) < int(logic['pq_exp_col']) and logic['pq_part_col']:
                        info_list = info_list[:int(logic['pq_part_col'])] + ['0'] + info_list[
                                                                                    int(logic['pq_part_col']):]
            ocr_details["raw_info_list"] = "|".join(info_list)
            # Quality check for total info fields
            if prior_qtr_flag == 'n':
                if len(info_list) != int(logic['exp_col']):
                    print info_list, row, util_qtr, info_list, ura, units, scripts, claimed
                    raise MyException('length is not correct - possible ocr mis-read resulting in missing/extra fields')
            else:
                if len(info_list) != int(logic['pq_exp_col']):
                    print info_list, row, util_qtr, info_list, ura, units, scripts, claimed
                    raise MyException('length is not correct - possible ocr mis-read resulting in missing/extra fields')

            # Get URA
            if prior_qtr_flag == 'n':
                if logic['ura']:
                    ura = info_list[int(logic['ura'])]
                    if '.' not in ura:
                        print ura
                        raise Exception('ura missing decimal, please review')
                    ura = str("%012.6f" % float(ura))
                else:
                    ura = str("%012.6f" % 0)
            else:
                if logic['pq_ura']:
                    ura = info_list[int(logic['pq_ura'])]
                    if '.' not in ura:
                        print ura
                        raise MyException('ura missing decimal, please review')
                    ura = str("%012.6f" % float(ura))
                else:
                    ura = str("%012.6f" % 0)
            ocr_details["ura"] = float(ura)

            # Get Units
            if prior_qtr_flag == 'n':
                if logic['units']:
                    units = info_list[int(logic['units'])]
                    if '.' not in units:
                        print units
                        raise MyException('units missing decimal, please review')
                    units = str("%015.3f" % float(units))
                else:
                    str("%015.3f" % 0)
            else:
                if logic['pq_units']:
                    units = info_list[int(logic['pq_units'])]
                    if 'covenant' not in str(logic['notes']):
                        if '.' not in units:
                            if state == 'MT' and type == 'MEDIJC' or type == 'MEDIEXP':
                                print 'MT_MEDIJC pq_units decimal absent'
                            else:
                                print units
                                raise MyException('units missing decimal, please review')
                    units = str('%015.3f' % float(units))
                else:
                    str("%015.3f" % 0)
            ocr_details["units"] = float(units)
            # print row_clean, units

            # Get Amount Claimed
            if prior_qtr_flag == 'n':
                if logic['claimed']:
                    claimed = info_list[int(logic['claimed'])]
                    if '.' not in claimed:
                        print claimed
                        raise MyException('amt claimed missing decimal, please review')
                    claimed = str("%012.2f" % float(claimed))
                else:
                    claimed = str("%012.2f" % 0)
            else:
                if logic['pq_claimed']:
                    claimed = info_list[int(logic['pq_claimed'])]
                    if '.' not in claimed:
                        print claimed
                        raise MyException('amt claimed missing decimal, please review')
                    claimed = str("%012.2f" % float(claimed))
                else:
                    claimed = str("%012.2f" % 0)
            ocr_details["claimed"] = float(claimed)
            # print row_clean, claimed

            # Scripts
            if prior_qtr_flag == 'n':
                if logic['scripts']:
                    scripts = info_list[int(logic['scripts'])]
                    scripts = str("%08.0f" % float(scripts))
                else:
                    scripts = str("%08.0f" % 0)
            else:
                if logic['pq_scripts']:
                    scripts = info_list[int(logic['pq_scripts'])]
                    scripts = str("%08.0f" % float(scripts))
                else:
                    scripts = str("%08.0f" % 0)
            ocr_details["scripts"] = int(scripts)

            # ignore unimportant lines
            if float(units) == 0 and float(ura) == 0 and float(scripts) == 0:
                detail_row_count -= 1
                continue

            # Medicaid Reimbursement
            if prior_qtr_flag == 'n':
                if logic['medi_reimb']:
                    medi_reimb = str("%013.2f" % float(info_list[int(logic['medi_reimb'])]))
                else:
                    medi_reimb = str("%013.2f" % 0)
            else:
                if logic['pq_medi_reimb']:
                    medi_reimb = str("%013.2f" % float(info_list[int(logic['pq_medi_reimb'])]))
                else:
                    medi_reimb = str("%013.2f" % 0)
            ocr_details["medi_reimbursed"] = float(medi_reimb)
            # print row_clean, scripts

            # Non-Medicaid Reimbursement
            if prior_qtr_flag == 'n':
                if logic['non_medi_reimb']:
                    non_medi_reimb = str("%013.2f" % float(info_list[int(logic['non_medi_reimb'])]))
                else:
                    non_medi_reimb = str("%013.2f" % 0)
            else:
                if logic['pq_non_medi_reimb']:
                    non_medi_reimb = str("%013.2f" % float(info_list[int(logic['pq_non_medi_reimb'])]))
                else:
                    non_medi_reimb = str("%013.2f" % 0)
            ocr_details["non_medi_reimbursed"] = float(non_medi_reimb)
            # print row_clean, non_medi_reimb

            # Total Reimbursement
            if prior_qtr_flag == 'n':
                if logic['total_reimb']:
                    total_reimb = str("%014.2f" % float(info_list[int(logic['total_reimb'])]))
                else:
                    total_reimb = str("%014.2f" % 0)
            else:
                if logic['pq_total_reimb']:
                    total_reimb = str("%014.2f" % float(info_list[int(logic['pq_total_reimb'])]))
                else:
                    total_reimb = str("%014.2f" % 0)

            # Correction flag
            if prior_qtr_flag == 'n':
                if logic['corr_flag']:
                    corr_flag = info_list[int(logic['corr_flag'])][:1]
                else:
                    corr_flag = str(0)
            else:
                if logic['pq_corr_flag']:
                    corr_flag = info_list[int(logic['pq_corr_flag'])][:1]
                else:
                    corr_flag = str(0)
            print 'checkpoint 8'
            human.append(
                {'type': type, 'state': state, 'labeler': labeler, 'prod': prod, 'size': size,
                 'inv_qtr': inv_qtr,
                 'util_qtr': util_qtr, 'inv_num': inv_num, 'ndc': ndc, 'name': name, 'ura': ura, 'units': units,
                 'claimed': claimed, 'scripts': scripts, 'medi_reimb': medi_reimb,
                 'non_medi_reimb': non_medi_reimb,
                 'total_reimb': total_reimb, 'corr_flag': corr_flag})

            if len(rtype + state + labeler + prod + size + util_qtr +
                           name + ura + units + claimed + scripts + medi_reimb + non_medi_reimb + total_reimb + corr_flag) != 120:
                print str(len(
                    rtype + state + labeler + prod + size + util_qtr + name + ura + units + claimed + scripts + medi_reimb + non_medi_reimb + total_reimb + corr_flag)), row_clean
                print rtype, state, labeler, prod, size, util_qtr, name, ura, units, claimed, scripts, medi_reimb, non_medi_reimb, total_reimb, corr_flag
                raise MyException('length is not correct')

                # ~ split out acella into a file for each quarter
                # ~ if labeler == '42192':
                # ~ if util_qtr in cmsd:
                # ~ cmsd[util_qtr].append(rtype+state+labeler+prod+size+util_qtr+name+ura+units+claimed+scripts+medi_reimb+non_medi_reimb+total_reimb+corr_flag)
                # ~ else:
                # ~ cmsd[util_qtr] = [rtype+state+labeler+prod+size+util_qtr+name+ura+units+claimed+scripts+medi_reimb+non_medi_reimb+total_reimb+corr_flag]

                # ~ else:
                # ~ cms.append(rtype+state+labeler+prod+size+util_qtr+name+ura+units+claimed+scripts+medi_reimb+non_medi_reimb+total_reimb+corr_flag)
            cms.append(
                rtype + state + labeler + prod + size + util_qtr + name + ura + units + claimed + scripts + medi_reimb + non_medi_reimb + total_reimb + corr_flag)

            # print '--------------------'
            # for item in info_list:
            #     print str(item) + " "

    except MyException as e:
        print row_clean
        print str(e)
        error_count += 1
        errors.append('row #: ' + str(row_count) + ' error: ' + str(e) + ' ' + row_clean)
        ocr_details["error_type"] = "User Error"
        ocr_details["error_message"] = str(e.message)

    except Exception as e:
        print row_clean
        print str(e.message)
        unknown_error_count += 1
        unknown_errors.append('row #: ' + str(row_count) + ' error: ' + str(e) + ' ' + row_clean)
        ocr_details["error_type"] = "Technical Error"
        ocr_details["error_message"] = str(e.message)

    if ocr_details["row_number"] is not None:
        ocr_details_list.append(ocr_details)

# print performance and accuracy stats in console
print error_count, unknown_error_count, detail_row_count

total_error_count = error_count
total_error_percent = round(float(total_error_count) / float(detail_row_count) * 100, 2)

print labeler, state, type, rtype, util_qtr, inv_qtr, ' - Total rows: ' + str(row_count) + ', Detail rows: ' + str(
    detail_row_count) + ', Error rows: ' + str(error_count) + ', Error percentage: ' + str(
    total_error_percent) + '%'

# Log OCR result stats
# with open('log.txt', 'ab') as output_file:
with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\log.txt', 'ab') as output_file:
    output_file.write(
        str(datetime.datetime.now()) + ' ' + state + ' ' + type + ' ' + rtype + ' ' + util_qtr + ' - Total rows: ' +
        str(row_count) + ', Detail rows: ' + str(detail_row_count) + ', Error rows: ' + str(error_count)
        + 'Technical Errors:' + str(unknown_error_count) + ', Total Error percentage: '
        + str(total_error_percent) + '%' + '\r\n')

# create file for logging errors
tech_error_file = 'F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_TECHNICAL_ERRORS.txt' % (
    inv_qtr, labeler, state, type)
# tech_error_file = '%s_%s_%s_%s_TECHNICAL_ERRORS.txt' % (inv_qtr, labeler, state, type)
error_file = 'F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_ERRORS.txt' % (
    inv_qtr, labeler, state, type)
# error_file = '%s_%s_%s_%s_ERRORS.txt' % (inv_qtr, labeler, state, type)

# remove existing error files for this invoice
try:
    os.remove(tech_error_file)
except OSError:
    pass

try:
    os.remove(error_file)
except OSError:
    pass

# write the errors to file only if they exist
if unknown_errors:
    with open(tech_error_file, 'wb') as output_file:
        for r in unknown_errors:
            output_file.write(r + '\r\n')

if errors:
    # DN 07/24 - using Util Qtr in filename, being consistent with CMS format filename below
    with open(error_file, 'wb') as output_file:
        for r in errors:
            output_file.write(r + '\r\n')

# write all details into csv file
inv_header = ['type', 'state', 'labeler', 'prod', 'size', 'inv_qtr', 'util_qtr', 'inv_num', 'ndc', 'name', 'ura',
              'units', 'claimed', 'scripts', 'medi_reimb', 'non_medi_reimb', 'total_reimb', 'corr_flag']

# with open('%s_%s_%s_%s_csv.txt' % (inv_qtr, labeler, state, type), 'wb') as output_file:
with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_csv.txt' % (
inv_qtr, labeler, state, type), 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, inv_header)
    dict_writer.writeheader()
    dict_writer.writerows(human)

# write all details into cms file
# with open('%s_%s_%s_%s_cms.txt' % (inv_qtr, labeler, state, type), 'wb') as output_file:
with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_cms.txt' % (
        inv_qtr, labeler, state, type), 'wb') as output_file:
    for r in cms:
        output_file.write(r + '\r\n')

# capture attempt # from filename
if 'attempt' in invoice_file:
    attempt = invoice_file[str(invoice_file).rindex('_') + 1:str(invoice_file).rindex('.')]
else:
    attempt = '1'

file_name = str(inv_qtr) + '_' + str(labeler) + '_' + state + '_' + str(type) + '_ATTEMPT_' + attempt

# insert into ocr_header table
ocr_header_insert = "INSERT INTO OCR_HEADER VALUES (id,'%s',%s,'%s','%s','%s','%s','%s', NOW())" % (
    str(file_name), str(attempt), str(row_count), str(detail_row_count), str(error_count), str(unknown_error_count),
    str(total_error_percent))
cursor.execute(ocr_header_insert)
con.commit()

# get id reference of this invoice for ocr_details & ocr_errors
# get_id_query = "SELECT id FROM OCR_HEADER WHERE invoice_file='%s' ORDER BY timestamp DESC LIMIT 1" % (str(file_name))

# query string for SQL Server
get_id_query = "SELECT id FROM OCR_HEADER WHERE timestamp =  ( SELECT MAX( timestamp ) FROM OCR_HEADER WHERE invoice_file = '%s' )" % (
    str(file_name))

cursor.execute(get_id_query)
ocr_header_id = cursor.fetchone()[0]

# add header_id into every map of details and errors list
for x in ocr_details_list:
    x["ocr_header_id"] = int(ocr_header_id)

# formulate query for insertion into ocr_details
ocr_details_insert = "INSERT INTO OCR_DETAILS VALUES(%(ocr_header_id)s, %(row_number)s, %(detail_row_number)s, %(ndc)s, %(ura)s, %(units)s, %(claimed)s, %(scripts)s, %(medi_reimbursed)s, %(non_medi_reimbursed)s, %(raw_detail_row)s, %(raw_info_list)s, %(invoice_qtr)s, %(utilization_qtr)s, %(invoice_number)s, %(error_type)s, %(error_message)s)"
cursor.executemany(ocr_details_insert, ocr_details_list)
con.commit()

cursor.close()
con.close

print datetime.datetime.now()
