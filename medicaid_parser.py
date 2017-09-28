import cx_Oracle
import mysql.connector
import datetime
import csv
import sys
import re
import string
import itertools
import os

class MyException(Exception):
    """Custom exception for intuitive error messages"""
    pass

args = sys.argv

file = args[1].replace('\\', '\\\\')
key = args[2]
inv_qtr = args[3]
labeler = args[4]

print datetime.datetime.now()

print file, key, inv_qtr, labeler

con = mysql.connector.connect(user='root', password='ncsuball', host='127.0.0.1', database='medicaid')
cursor = con.cursor()

ptype = ''
inv_num = ''
util_qtr = ''

qmap = {'01': '1', '02': '1', '03': '1', '04': '2', '05': '2', '06': '2', '07': '3', '08': '3', '09': '3', '10': '4',
        '11': '4', '12': '4'}
textqmap = {'FIRST QUARTER': 'Q1', 'SECOND QUARTER': 'Q2', 'THIRD QUARTER': 'Q3', 'FOURTH QUARTER': 'Q4'}

items = {file: key}

for k, v in items.iteritems():
    unknown_errors = []
    errors = []
    human = []
    cms = []
    mco_lookup = ['MCO', 'Managed']
    ffs_lookup = ['FFS']
    cmsd = {}
    logic = {}
    file = k
    key = v
    year = ''
    quarter = ''
    prior_qtr = 'n'

    # ~ length = 5
    # ~ pattern= r"\D(\d{%d})\D" % length
    # ~ labeler_extract = re.findall(pattern, file)
    # ~ file_labeler = labeler_extract[0]
    # ~ labeler = file_labeler

    # DN - using  os.path to get base filename.  From there, can get Inv Quarter, Program key (to be done later)
    base_filename = os.path.basename(file)

    querystring = (
                  "SELECT * FROM invoice_logic il LEFT JOIN invoice_types it ON il.invoice_type = it.id WHERE il.`key` = '%s'") % key
    cursor.execute(querystring)

    field_names = [d[0].lower() for d in cursor.description]
    rows = cursor.fetchmany()
    for row in rows:
        logic.update(dict(itertools.izip(field_names, row)))

    lines = open(file).read().splitlines()

    row = 0
    ecount = 0
    unknown_ecount = 0
    detail_row = 0

    try:
        state = str(logic['state'])
        type = str(logic['type'])
        std_type = str(logic['std_type'])
        rtype = std_type
    except:
        with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\log.txt',
                  'ab') as output_file:
            print str(key + ' not yet added to OCR tool, please contact brian.coleman@cumberlandcg.com')
            output_file.write(str(
                datetime.datetime.now()) + ' ' + key + ' not yet added to OCR tool, please contact brian.coleman@cumberlandcg.com' + '\r\n')
        continue

    for r in lines:

        row += 1
        # ~ print r
        try:
            # row cleanup and garbage removal
            r = r.upper()
            for x in logic['garbage'].split('|'):
                r = r.replace(str(x), '')

            # identification of invoice number
            if not inv_num:
                if logic['inv_num'] in r:
                    info = r.split(logic['inv_num'])
                    inv_num = info[1].strip(' ')

            # identification of invoice type if differs from type derived from file_name
            for x in mco_lookup:
                if x in r:
                    ptype = 'MCOU'
            for x in ffs_lookup:
                if x in r:
                    ptype = 'FFSU'
            if ptype <> rtype and ptype <> '':
                rtype = ptype

                # identification of submission/invoice quarter
                # ~ if r.startswith(logic['inv_qtr']):
                # ~ info = r.split(':')
                # ~ info = info[1].strip().split('/')
                # ~ inv_qtr = info[0]+info[1]
                # ~ qtr = info[1]+"Q"+info[0]
                # ~ util_qtr = inv_qtr

            # identification of billing quarter
            for x in logic['period_covered'].split('|'):
                if x in r:
                    # ~ print row, x
                    if logic['period_type'] == '#QYYYY':
                        info = r.split(str(logic['period_covered']) + ': ')
                        util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                    elif logic['period_type'] == 'YYYY#':
                        info = r.split(str(logic['period_covered']) + ' ')
                        util_qtr = info[1][4:5].replace('L', '1') + info[1][:4]
                    elif logic['period_type'] == '#/YYYY':
                        info = r.split(str(logic['period_covered']) + ': ')
                        util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                    elif logic['period_type'] == '#-YYYY':
                        info = r.split(str(logic['period_covered']) + ': ')
                        util_qtr = info[1][:1].replace('L', '1') + info[1][2:6]
                    elif logic['period_type'] == '#YYYY':
                        info = r.split(str(logic['period_covered']) + ': ')
                        util_qtr = info[1][:1].replace('L', '1') + info[1][1:5]
                    # DN - Added for CA - New  YYYY/Q
                    elif logic['period_type'] == 'YYYY/#':
                        info = r.split(str(logic['period_covered']) + ' ')
                        util_qtr = info[1][5:6].replace('L', '1') + info[1][:4]
                    elif logic['period_type'] == 'MM/DD/YY':
                        info = r.split(str(logic['period_covered']) + ': ')
                        util_qtr = qmap[info[1][:2]] + '20' + info[1][6:8]
                    elif logic['period_type'] == 'YYYYbreakQ':
                        info = r.split(str(x) + ' ')
                        if x == 'YEAR':
                            year = info[1][:4]
                        elif x == 'QTR':
                            quarter = info[1][:1]
                    elif logic['period_type'] == '# QUARTER YYYY':
                        quarter = textqmap[x]
                        info = r.split(str(x))
                        year = info[1].strip(' ')
                        util_qtr = quarter[1:2] + year[:4]
                    elif logic['period_type'] == 'MM/DD/YYYY':
                        info = r.split(str(logic['period_covered']) + ' ')
                        month = info[1][:2]
                        year = info[1][6:10]
                        quarter = qmap[str(month)]
                        util_qtr = quarter + year[:4]

            if (logic['period_type'] == 'YYYYbreakQ' and year != '' and quarter != ''):
                util_qtr = quarter + year

             # if billing quarter is not listed in the file, default to invoice quarter
            if not util_qtr:
                util_qtr = inv_qtr[5:6] + inv_qtr[:4]

            # determine if we have passed into prior quarter pages (from submission/current quarter)
            # if prior quarter id field is present, this means the state/program has a different format for prior quarter detail and needs to be parsed with a different format
            if logic['prior_quarter_id']:
                if logic['prior_quarter_id'] in r:
                    prior_qtr = 'y'
                    # ~ print 'prior quarter'

            if prior_qtr == 'n':
                if logic['ura_length']:
                    length = int(logic['ura_length'])
                    pattern = r"\d+\.\d{%d}" % length
                    ura_string = re.findall(pattern, r)
                    # ~ checkr = r.replace(' ','')
                    #~ pattern = logic['detail_check']
                    pattern = r"\d*[.,]\d{%d}" % length
                    #~ pattern = r"\d+\.\d{%d}" % length
                    inv_detail_check = re.findall(pattern, r)
            else:
                if logic['pq_ura_length']:
                    length = int(logic['pq_ura_length'])
                    pattern = r"\d+\.\d{%d}" % length
                    pq_ura_string = re.findall(pattern, r)
                    # ~ checkr = r.replace(' ','')
                    #~ pattern = logic['pq_detail_check']
                    pattern = r"\d*[.,]\d{%d}" % length
                    #~ pattern = r"\d+\.\d{%d}" % length
                    inv_detail_check = re.findall(pattern, r)

            detail = 'n'

            # Get utilization quarter if not provided in the header section of the invoice
            # set placeholder value for util qtr if it trails the detail lines in the files
            if logic['pq_pc_trailing'] == 'Y' and prior_qtr == 'y':
                util_qtr = '11900'

            # if util qtr trails detail lines - pick it up when found and use that value to replace the placeholder in the list
            if logic['pq_pc_trailing'] == 'Y' and prior_qtr == 'y':
                if str(logic['pq_period_covered']) in r:
                    util_qtr_o = r.split(str(logic['pq_period_covered']))[0].strip().replace('-', '').replace('O', '0')
                    if str(logic['pq_period_type']) == '#QYYYY':
                        util_qtr = str(str(util_qtr_o)[:1] + str(util_qtr_o)[2:6])
                        if len(str(util_qtr).strip()) <> 5:
                            raise MyException(
                                'utilization quarter did not translate properly, correct and re-run as this will impact any related lines')
                    else:
                        util_qtr = util_qtr

                    for idx, item in enumerate(cms):
                        if '11900' in item:
                            cms[idx] = item.replace('11900', str(util_qtr))

            # dynamically inserts python logic to identify invoice line item detail based on the invoice type which is defined in the medicaid->invoice_types table
            #~ exec (logic['code_line_id'])
            if inv_detail_check:
                detail = 'y'

            if detail == 'y':
                # ~ print 'detail'

                detail_row += 1

                # Get utilization quarter from the line item info
                if prior_qtr == 'y':
                    if logic['detail_qtr']:
                        pre_info = r.split(' ')
                        if logic['period_type'] == 'YYYYQ#':
                            util_qtr_o = str(pre_info[int(logic['detail_qtr'])])
                            util_qtr = util_qtr_o[5:6] + util_qtr_o[:4]
                        if logic['period_type'] == '#/YYYY':
                            util_qtr_o = str(pre_info[int(logic['detail_qtr'])])
                            util_qtr = util_qtr_o[:1] + util_qtr_o[2:6]
                        r = r.replace(util_qtr_o, '')

                # add labeler code for invoices that only include product code and size - requires ndc to be the first field
                if str(logic['ndc']) == '0':
                    checkr = re.sub("[^0-9\d.\-\s]", "", r)
                    if len(str(checkr)) >= 5:
                        if not checkr[:5].strip().isdigit():
                            r = labeler + '-' + r

                # check for dashes and add if necesssary

                # remove spaces and hyphens in ndc area - requires ndc to be first field
                if prior_qtr == 'n':
                    if str(logic['ndc']) == '0':
                        exec (logic['code_ndc_area'])
                        r = r.replace(ndc_area, ndc_area.replace(' ', ''))
                else:
                    if str(logic['pq_ndc']) == '0':
                        exec (logic['code_ndc_area'])
                        r = r.replace(ndc_area, ndc_area.replace(' ', ''))

                # If ura is present after product description, use ura to determine product description end position and remove product description
                if prior_qtr == 'n':
                    if logic['prod_desc'] <> 'n':
                        # identifies product name start position with first space
                        start = r.index(' ')

                        end = r.index(str(ura_string[0])) - 1

                        r = r.replace(r[start:end], '')
                else:
                    if logic['pq_prod_desc'] <> 'n':
                        # identifies product name start position with first space
                        start = r.index(' ')

                        end = r.index(str(pq_ura_string[0])) - 1

                        r = r.replace(r[start:end], '')

                r = re.sub("[^0-9\d.\-\s]", "", r)
                r = r.replace('  ', ' ')
                r = r.replace('  ', ' ')
                # DN - replace any single dot, i.e. led and followed by space with just space
                r = r.replace(' . ', ' ')

                # ~ if logic['invoice_type'] == 1:
                # ~ get_ndc = r[:5]+r[6:10]+r[11:13]
                # ~ else:
                # ~ get_ndc = r[:11]

                # Split out detail lines into a list of elements
                info = r.split(' ')

                # if length of the elements is longer than expected, remove the delete columns from the list (only for invoices where current and prior format is the same)
                if prior_qtr == 'n':
                    if logic['del_col'] and logic['exp_col']:
                        if len(info) > int(logic['exp_col']):
                            for x in logic['del_col'].split('|'):
                                del info[int(x) - 1:int(x)]

                # Get NDC
                if prior_qtr == 'n':
                    # ~ ndc = get_ndc.replace('O','0').replace('()','0')
                    ndc = info[int(logic['ndc'])].replace('-', '').replace('O', '0').replace('()', '0')
                else:
                    ndc = info[int(logic['pq_ndc'])].replace('-', '').replace('O', '0').replace('()', '0')

                # quality check on ndc field
                if len(str(ndc).strip()) <> 11:
                    raise MyException('ndc is not 11 characters, possible OCR misread, please review')
                if not str(ndc).strip().replace('-', '').isdigit():
                    raise MyException('ndc has non-numeric characters, possible OCR misread, please review')

                    # If detail quarter field is filled in overwrite utilization quarter from the detail row - moved further up
                    # ~ if logic['detail_qtr']:
                    # ~ if logic['period_type'] == 'YYYYQ#':
                    # ~ util_qtr = str(info[int(logic['detail_qtr'])])
                    # ~ util_qtr = util_qtr[5:6]+util_qtr[:4]

                # Create labeler, prod and size from ndc11 field
                labeler = ndc[:5]
                prod = ndc[5:9]
                size = ndc[9:11]

                # Default Name to blank with correct spacing
                name = str("          ")

                # clean up multiple decimal points when tesseract misinterprets commas, also throw error if tesseract misinterprates decimal for a comma
                for idx, item in enumerate(info):
                    item = str(item).replace('l', '1').replace('O', '0').replace('()', '0')
                    info[idx] = item
                    if str(item).count(".") > 1:
                        item = str(item).replace('.', '', 1)
                        info[idx] = item
                    if str(item).count(",") > 1:
                        raise MyException('tesseract misread decimal for a comma - please fix manually')

                # handling for columns in sections that are only sometimes populated
                if prior_qtr == 'n':
                    if logic['exp_col'] and logic['part_col']:
                        if len(info) < int(logic['exp_col']) and logic['part_col']:
                            info = info[:int(logic['part_col'])] + ['0'] + info[int(logic['part_col']):]
                else:
                    if logic['pq_exp_col'] and logic['pq_part_col']:
                        if len(info) < int(logic['pq_exp_col']) and logic['pq_part_col']:
                            info = info[:int(logic['pq_part_col'])] + ['0'] + info[int(logic['pq_part_col']):]

                # URA
                if prior_qtr == 'n':
                    if logic['ura']:
                        ura = info[int(logic['ura'])]
                        if '.' not in ura:
                            print ura
                            raise Exception('ura missing decimal, please review')
                        ura = str("%012.6f" % float(ura))
                    else:
                        ura = str("%012.6f" % 0)
                else:
                    if logic['pq_ura']:
                        ura = info[int(logic['pq_ura'])]
                        if '.' not in ura:
                            print ura
                            raise MyException('ura missing decimal, please review')
                        ura = str("%012.6f" % float(ura))
                    else:
                        ura = str("%012.6f" % 0)

                # Units
                if prior_qtr == 'n':
                    if logic['units']:
                        units = info[int(logic['units'])]
                        if '.' not in units:
                            print units
                            raise MyException('units missing decimal, please review')
                        units = str("%015.3f" % float(units))
                    else:
                        str("%015.3f" % 0)
                else:
                    if logic['pq_units']:
                        units = info[int(logic['pq_units'])]
                        if '.' not in units:
                            if state == 'MT' and type == 'MEDIJC' or type == 'MEDIEXP':
                                print 'MT_MEDIJC pq_units decimal absent'
                            else:
                                print units
                                raise MyException('units missing decimal, please review')
                        units = str('%015.3f' % float(units))
                    else:
                        str("%015.3f" % 0)
                        # ~ print row, units

                # Amount Claimed
                if prior_qtr == 'n':
                    if logic['claimed']:
                        claimed = info[int(logic['claimed'])]
                        if '.' not in claimed:
                            print claimed
                            raise MyException('amt claimed missing decimal, please review')
                        claimed = str("%012.2f" % float(claimed))
                    else:
                        claimed = str("%012.2f" % 0)
                else:
                    if logic['pq_claimed']:
                        claimed = info[int(logic['pq_claimed'])]
                        if '.' not in claimed:
                            print claimed
                            raise MyException('amt claimed missing decimal, please review')
                        claimed = str("%012.2f" % float(claimed))
                    else:
                        claimed = str("%012.2f" % 0)
                        # ~ print row, claimed

                # Scripts
                if prior_qtr == 'n':
                    if logic['scripts']:
                        scripts = info[int(logic['scripts'])]
                        scripts = str("%08.0f" % float(scripts))
                    else:
                        scripts = str("%08.0f" % 0)
                else:
                    if logic['pq_scripts']:
                        scripts = info[int(logic['pq_scripts'])]
                        scripts = str("%08.0f" % float(scripts))
                    else:
                        scripts = str("%08.0f" % 0)

                if prior_qtr == 'n':
                    if len(info) <> int(logic['exp_col']):
                        print info, row, util_qtr, info, ura, units, scripts, claimed
                        raise MyException(
                            'length is not correct - possible ocr mis-read resulting in missing/extra fields')
                else:
                    if len(info) <> int(logic['pq_exp_col']):
                        print info, row, util_qtr, info, ura, units, scripts, claimed
                        raise MyException(
                            'length is not correct - possible ocr mis-read resulting in missing/extra fields')

                # ignore unimportant lines
                if float(units) == 0 and float(ura) == 0 and float(scripts) == 0:
                    detail_row -= 1
                    continue

                # Medicaid Reimbursement
                if prior_qtr == 'n':
                    if logic['medi_reimb']:
                        medi_reimb = str("%013.2f" % float(info[int(logic['medi_reimb'])]))
                    else:
                        medi_reimb = str("%013.2f" % 0)
                else:
                    if logic['pq_medi_reimb']:
                        medi_reimb = str("%013.2f" % float(info[int(logic['pq_medi_reimb'])]))
                    else:
                        medi_reimb = str("%013.2f" % 0)

                # Non-Medicaid Reimbursement
                if prior_qtr == 'n':
                    if logic['non_medi_reimb']:
                        non_medi_reimb = str("%013.2f" % float(info[int(logic['non_medi_reimb'])]))
                    else:
                        non_medi_reimb = str("%013.2f" % 0)
                else:
                    if logic['pq_non_medi_reimb']:
                        non_medi_reimb = str("%013.2f" % float(info[int(logic['pq_non_medi_reimb'])]))
                    else:
                        non_medi_reimb = str("%013.2f" % 0)

                # Total Reimbursement
                if prior_qtr == 'n':
                    if logic['total_reimb']:
                        total_reimb = str("%014.2f" % float(info[int(logic['total_reimb'])]))
                    else:
                        total_reimb = str("%014.2f" % 0)
                else:
                    if logic['pq_total_reimb']:
                        total_reimb = str("%014.2f" % float(info[int(logic['pq_total_reimb'])]))
                    else:
                        total_reimb = str("%014.2f" % 0)

                # Correction flag
                if prior_qtr == 'n':
                    if logic['corr_flag']:
                        corr_flag = info[int(logic['corr_flag'])][:1]
                    else:
                        corr_flag = str(0)
                else:
                    if logic['pq_corr_flag']:
                        corr_flag = info[int(logic['pq_corr_flag'])][:1]
                    else:
                        corr_flag = str(0)

                human.append(
                    {'type': type, 'state': state, 'labeler': labeler, 'prod': prod, 'size': size, 'inv_qtr': inv_qtr,
                     'util_qtr': util_qtr, 'inv_num': inv_num, 'ndc': ndc, 'name': name, 'ura': ura, 'units': units,
                     'claimed': claimed, 'scripts': scripts, 'medi_reimb': medi_reimb, 'non_medi_reimb': non_medi_reimb,
                     'total_reimb': total_reimb, 'corr_flag': corr_flag})

                if len(
                                                                                                                                        rtype + state + labeler + prod + size + util_qtr + name + ura + units + claimed + scripts + medi_reimb + non_medi_reimb + total_reimb + corr_flag) != 120:
                    print str(len(
                        rtype + state + labeler + prod + size + util_qtr + name + ura + units + claimed + scripts + medi_reimb + non_medi_reimb + total_reimb + corr_flag)), r
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

        except MyException as e:
            print r
            print str(e)
            ecount += 1
            errors.append('row #: ' + str(row) + ' error: ' + str(e) + ' ' + r)
            # ~ errors.append('detail row #: '+str(detail_row)+' error: '+str(e)+' '+r)
        except Exception as e:
            print r
            print str(e)
            unknown_ecount += 1
            unknown_errors.append('row #: ' + str(row) + ' error: ' + str(e) + ' ' + r)


    # ~ print errors

    # ~ print state, type, rtype, inv_qtr, util_qtr, str(round(float(ecount)/float(row)*100,2))+'%'
    print ecount, unknown_ecount, detail_row
    print labeler, state, type, rtype, util_qtr, inv_qtr, ' - Total rows: ' + str(row) + ', Detail rows: ' + str(
        detail_row) + ', Error rows: ' + str(ecount) + ', Error percentage: ' + str(
        round(float(ecount) / float(detail_row) * 100, 2)) + '%'
    with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\log.txt', 'ab') as output_file:
        output_file.write(str(
            datetime.datetime.now()) + ' ' + state + ' ' + type + ' ' + rtype + ' ' + util_qtr + ' - Total rows: ' + str(
            row) + ', Detail rows: ' + str(detail_row) + ', Error rows: ' + str(ecount) + ', Error percentage: ' + str(
            round(float(ecount) / float(detail_row) * 100, 2)) + '%' + '\r\n')



    # ~ Printing for errors + unknown_errors
    print labeler, state, type, rtype, util_qtr, inv_qtr, ' - Total rows: ' + str(row) + ', Detail rows: ' + str(
        detail_row) + ', Error rows: ' + str(ecount+unknown_ecount) + ', Error percentage: ' + str(
        round(float(ecount+unknown_ecount) / float(detail_row) * 100, 2)) + '%'

    tech_error_file = 'F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_TECHNICAL_ERRORS.txt' % (
        inv_qtr, labeler, state, type)
    try:
        os.remove(tech_error_file)
    except OSError:
        pass

    if unknown_errors:
        # DN 07/24 - using Util Qtr in filename, being consistent with CMS format filename below
        # ~ with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_ERRORS.txt'%(inv_qtr,labeler,state,type), 'wb') as output_file:
        with open(tech_error_file, 'wb') as output_file:
            for r in unknown_errors:
                output_file.write(r + '\r\n')



    error_file = 'F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_ERRORS.txt' % (
    inv_qtr, labeler, state, type)
    try:
        os.remove(error_file)
    except OSError:
        pass

    if errors:
        # DN 07/24 - using Util Qtr in filename, being consistent with CMS format filename below
        # ~ with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_ERRORS.txt'%(inv_qtr,labeler,state,type), 'wb') as output_file:
        with open(error_file, 'wb') as output_file:
            for r in errors:
                output_file.write(r + '\r\n')

    inv_header = ['type', 'state', 'labeler', 'prod', 'size', 'inv_qtr', 'util_qtr', 'inv_num', 'ndc', 'name', 'ura',
                  'units', 'claimed', 'scripts', 'medi_reimb', 'non_medi_reimb', 'total_reimb', 'corr_flag']
    # ~ with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_csv.txt'%(inv_qtr,labeler,state,type), 'wb') as output_file:
    with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_csv.txt' % (
            inv_qtr, labeler, state, type), 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, inv_header)
        dict_writer.writeheader()
        dict_writer.writerows(human)

        # Split out Acella into separate quarters
        # ~ if labeler == '42192':
        # ~ for k,v in cmsd.iteritems():
        # ~ qtr = k[1:5]+"Q"+k[:1]
        # ~ with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_cms.txt'%(qtr,labeler,state,type), 'wb') as output_file:
        # ~ for r in v:
        # ~ output_file.write(r + '\r\n')
        # ~ else:
        # ~ with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_cms.txt'%(inv_qtr,labeler,state,type), 'wb') as output_file:
        # ~ for r in cms:
        # ~ output_file.write(r + '\r\n')

    with open('F:\\sharefile\\Shared Folders\\mft\\out\\medicaid\\4 - cms_format\\%s_%s_%s_%s_cms.txt' % (
            inv_qtr, labeler, state, type), 'wb') as output_file:
        for r in cms:
            output_file.write(r + '\r\n')

    print datetime.datetime.now()
