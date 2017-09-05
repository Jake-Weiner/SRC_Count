import pyodbc
import os
import csv
import pandas as pd
from datetime import datetime
from Queue import Queue
from threading import Thread
import time
import sys

##NOTE - If there are significant deiscrepancies between Row_Count in the Upload Data and SQL tables, there may be unreadable Unicode Characters in the Upload data that Python can't handle. To remedy this
##problem, find the row that python stops counting on and investigate what the character is on notepad++. Replace all such characters in all files with '' and re run this script for accurate results.

# Two options for program running
# 1) data recon on both SQL side and L:drive side
# 2) data recon on L:drive side
# Set run option to either to run as required

option_num = input('For SQL table summaries enter 1 \nFor Source Data Summaries enter 2 \nFor both enter 3 \n ')

# location of source data in L:Drive
upload_folder = r'xx'

def progress_bar(in_q):
    while True:

        if not in_q.empty():
            process = in_q.get()

            if process == 1:
                sys.stdout.write('[**        ]')
                sys.stdout.flush()
            elif process == 2:
                sys.stdout.write('\r[****      ]')
                sys.stdout.flush()
            elif process == 3:
                sys.stdout.write('\r[******    ]')
                sys.stdout.flush()
            elif process == 4:
                sys.stdout.write('\r[********  ]')
                sys.stdout.flush()
            elif process == 5:
                sys.stdout.write('\r[**********]')
                sys.stdout.flush()
                quit()


def sql_query():
    # Update the DB_name in SQL
    DB_name = "xxx"

    # Location of SQL Stored Proc to Count all SRC_Table rows
    sql_query_file_path = r'xxx'

    # Establish the Connection to SQL. Makesure the change the DATABASE= <<Your DB NAME>>
    connection = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=xxx;DATABASE=xxx;Trusted_Connection=Yes')

    # SQL Commands executed and results are given
    cur = connection.cursor()
    query_file = open(sql_query_file_path, 'rU')
    query_string = query_file.read()
    cur = cur.execute(str(query_string))
    cur.commit()
    cur = cur.execute("Select * from #Output")
    results = cur.fetchall()

    cur = cur.execute("DROP TABLE #Output")
    cur.close()
    del cur
    connection.close()
    return results


def table_calcs(results):
    table_results = []
    for table in results:
        table_results.append((table[0], table[2]))
    table_results = sorted(table_results, key=lambda table: table[1])
    return table_results


def source_data_calcs():
    filenames = os.listdir(upload_folder)
    Upload_row_count = 0
    file_info_list = []
    for root, dirs, files in os.walk(upload_folder):
        for input_file in files:
            count = 0
            current_file = open(os.path.join(root, input_file), 'rU')
            for line in current_file:
                count += 1

            current_file.close()
            file_time = os.path.getctime(os.path.join(root, input_file))
            file_time = datetime.fromtimestamp(file_time).strftime('%Y-%m-%d')
            file_info_list.append((input_file, root + '\\' + input_file, count - 1, file_time))

    # Sort the file by row count - sort by the key which is the function applied to the list

    file_info_list = sorted(file_info_list, key=lambda file_info: file_info[2])

    return file_info_list


def pad_data(table_results, file_info_list):
    # Writing to the Output CSV file

    largest_size = -1

    if len(table_results) > len(file_info_list):
        uneven_length = True
        largest_size = 1

    elif len(table_results) < len(file_info_list):
        uneven_length = True
    else:
        uneven_length = False

    # pad out lists to get symmetry
    if uneven_length:
        if largest_size == 1:
            file_info_list = ([('', '', '', '')] * (len(table_results) - len(file_info_list))) + file_info_list
        else:
            table_results = ([('', '')] * (len(file_info_list) - len(table_results))) + table_results
    return table_results, file_info_list


def write_results(table_results, file_info_list):
    df_data = {'SQL Table Name': [table[0] for table in table_results],
               'Table Row Count': [table[1] for table in table_results],
               'Source File Name': [file[0] for file in file_info_list],
               'Source File Path': [file[1] for file in file_info_list],
               'Source Row Count': [file[2] for file in file_info_list],
               'Source File Modified Time': [file[3] for file in file_info_list]
               }

    df = pd.DataFrame(df_data, columns=['SQL Table Name', 'Table Row Count', 'Source File Name', 'Source File Path',
                                        'Source Row Count', 'Source File Modified Time'])

    # Change the location of the output CSV results file
    df.to_csv(r'xxx')


def run_all(out_q):
    if option_num == 1 or option_num == 3:
        sql_results = sql_query()
        out_q.put(1)
        table_results = table_calcs(sql_results)
        out_q.put(2)
    else:
        out_q.put(1)
        out_q.put(2)
        table_results = [('', '')]

    if option_num == 2 or option_num == 3:
        file_info_list = source_data_calcs()
        out_q.put(3)
    else:
        out_q.put(3)
        file_info_list = [('', '', '', '')]
    table_results, file_info_list = pad_data(table_results, file_info_list)
    out_q.put(4)
    write_results(table_results, file_info_list)
    out_q.put(5)


if __name__ == '__main__':
    q = Queue()
    run_thread = Thread(target=run_all, args=(q,))
    progress_thread = Thread(target=progress_bar, args=(q,))
    run_thread.start()
    progress_thread.start()





