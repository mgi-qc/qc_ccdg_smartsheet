#!/usr/bin/python3.5
import os
import sys
import csv
import glob
import datetime
from time import sleep
import smartsheet

"""
Upload QC complete ccdg sample information to Smartsheet
Usage:
qc_ccdg_smartsheet <infile>

ex:
qc_ccdg_smartsheet qc.process.121219.1728.tsv
"""


infile = sys.argv[1]
if not infile:
    sys.exit('{} file not found.'.format(infile))


def get_report_info(report):
    """Get pass/fail samples and failed metrics"""

    report_results = dict.fromkeys(['Pass', 'Fail', 'FreemixFail', 'CovFail', 'OtherFail (discordant/interchromosomal)']
                                   , 0)
    with open(report, 'r') as f:
        failed_samples = False
        for line in f:
            if 'Samples That Meet QC Criteria' in line:
                report_results['Pass'] = int(line.split('=')[1])
            if 'Samples that Fail QC Criteria' in line:
                report_results['Fail'] = int(line.split('=')[1])
            if 'Failed Samples' in line:
                failed_samples = True
            if 'FREEMIX' in line and failed_samples:
                report_results['FreemixFail'] = int(line.split(':')[1])
            if 'HAPLOID_COVERAGE' in line and failed_samples:
                report_results['CovFail'] = int(line.split(':')[1])
            if ('INTERCHROMOSOMAL_RATE' in line or 'DISCORDANT_RATE' in line) and failed_samples:
                report_results['OtherFail (discordant/interchromosomal)'] += int(line.split(':')[1])
            if 'Summary Statistics' in line:
                return report_results

    return report_results


# connect to smartsheet
api_key = os.environ.get('SMRT_API')
if api_key is None:
    sys.exit('Api key not found')

smart_sheet_client = smartsheet.Smartsheet(api_key)

# get sheet column headers and id's
sheet_columns = smart_sheet_client.Sheets.get_columns(1355593198921604)
sheet_column_id_dict = {}
for col in sheet_columns.data:
    sheet_column_id_dict[col.title] = col.id


with open(infile, 'r') as f:

    fh = csv.DictReader(f, delimiter='\t')

    for line in fh:

        print('\nAdding {} QC results to CCDG tracking sheet.'.format(line['WOID']))

        # create new row
        new_row = smart_sheet_client.models.Row()
        new_row.to_bottom = True

        # add cells to row
        for field in line:

            if field == 'QC Directory':

                # qc files to append to row, get metrics for pass/fail samples.
                qc_files = glob.glob(line[field] + "/attachments/*")
                report_info = get_report_info([x for x in qc_files if 'report' in x][0])

                # add report metrics to new cells
                for r in report_info:
                    new_row.cells.append({'column_id': sheet_column_id_dict[r], 'value': report_info[r]})

            # correct date format/add to cell
            if field == 'QC Date':
                date = datetime.datetime.strptime(line['QC Date'], '%m-%d-%y').strftime('%Y-%m-%d')
                new_row.cells.append({'column_id': sheet_column_id_dict[field], 'value': date})
                continue

            # add remaining fields to new cells
            new_row.cells.append({'column_id': sheet_column_id_dict[field], 'value': line[field]})

        # write row to smartsheet
        print('\nApending: {} QC Row'.format(line['WOID']))
        response = smart_sheet_client.Sheets.add_rows(1355593198921604, [new_row]).data

        # get new row id to append attachments
        for item in response:
            new_row_id = item.id

        # chdir to attachments dir to avoid using full file name as attachment name
        cwd = os.getcwd()
        os.chdir(os.path.dirname(qc_files[0]))

        # append attachments to row
        for file_path in qc_files:
            sleep(10)
            file = os.path.basename(file_path)
            print('Appending: {}'.format(file))
            if 'report' in file:
                smart_sheet_client.Attachments.attach_file_to_row(1355593198921604, new_row_id,
                                                                  (file, open(file, 'rb'), 'application/Word'))
                continue

            smart_sheet_client.Attachments.attach_file_to_row(1355593198921604, new_row_id,
                                                              (file, open(file, 'rb'), 'application/Excel'))

        os.chdir(cwd)

print('\nSmartsheet sample update complete.')
