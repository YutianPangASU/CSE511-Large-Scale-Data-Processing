#!/usr/bin/python2.7
#
# Assignment5 Interface
#

import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    """
    Implement a Python function ParallelSort() that takes as input: (1) InputTable stored in a PostgreSQL database,
    (2) SortingColumnName the name of the column used to order the tuples by. ParallelSort() then sorts all tuples
    (using five parallelized threads) and stores the sorted tuples for in a table named OutputTable (the output table
    name is passed to the function). The OutputTable contains all the tuple present in InputTable sorted in ascending order.
    :param InputTable: Name of the table on which sorting needs to be done.
    :param SortingColumnName: Name of the column on which sorting needs to be done, would be either of type integer or
    real or float. Basically Numeric format. Will be Sorted in Ascending order.
    :param OutputTable: Name of the table where the output needs to be stored.
    :param openconnection: connection to the database.
    :return:
    """
    # num of threads to use
    num_of_threads = 5
    cur = openconnection.cursor()

    # get the interval value of the given sorting column (float)
    cur.execute("SELECT MAX({}) FROM {}".format(SortingColumnName, InputTable))
    sorting_col_max = cur.fetchone()[0]
    cur.execute("SELECT MIN({}) FROM {}".format(SortingColumnName, InputTable))
    sorting_col_min = cur.fetchone()[0]
    interval = (sorting_col_max - sorting_col_min)/num_of_threads

    # use multiple threads
    threads = []
    for thread_num in range(num_of_threads):

        cur.execute("DROP TABLE IF EXISTS SortPart{}".format(thread_num))
        cur.execute("CREATE TABLE SortPart{} (LIKE {})".format(thread_num, InputTable))

        lower, upper = sorting_col_min+thread_num*interval, sorting_col_min+(thread_num+1)*interval
        t = threading.Thread(target=Sortfunction,
                             args=(openconnection, thread_num, InputTable, SortingColumnName, lower, upper))
        t.start()
        threads.append(t)

    # join threads
    [thread.join() for thread in threads]

    # save result into OutputTable
    cur.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    cur.execute("CREATE TABLE {} (LIKE {} )".format(OutputTable, InputTable))
    for thread_num in range(num_of_threads):
        cur.execute("INSERT INTO {} SELECT * FROM SortPart{}".format(OutputTable, thread_num))
        cur.execute("DROP TABLE IF EXISTS SortPart{}".format(thread_num))

    cur.close()
    openconnection.commit()


def Sortfunction(openconnection, thread_num, InputTable, SortingColumnName, lower, upper):
    """
    This function is used for threading. Basically sort and partition the data based on the given Sorting column name
    and insert into referenced meta tables.
    :param openconnection:
    :param thread_num:
    :param InputTable:
    :param SortingColumnName:
    :param lower:
    :param upper:
    :return:
    """
    cur = openconnection.cursor()
    if thread_num == 0:
        cur.execute("INSERT INTO SortPart{0} SELECT * FROM {1} WHERE {2}>={3} AND {2}<={4} ORDER BY {2} ASC"
                    .format(thread_num, InputTable, SortingColumnName, lower, upper))
    else:
        cur.execute("INSERT INTO SortPart{0} SELECT * FROM {1} WHERE {2}>{3} AND {2}<={4} ORDER BY {2} ASC"
                    .format(thread_num, InputTable, SortingColumnName, lower, upper))
    cur.close()
    openconnection.commit()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    """
    Implement a Python function ParallelJoin() that takes as input: (1) InputTable1 and InputTable2 table stored in a
    PostgreSQL database, (2) Table1JoinColumn and Table2JoinColumn that represent the join key in each input table
    respectively. ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized threads) and
    stored the resulting joined tuples in a table named OutputTable (the output table name is passed to the function).
    The schema of OutputTable should be similar to the schema of both InputTable1 and InputTable2 combined.
    :param InputTable1: Name of the first table on which you need to perform join.
    :param InputTable2: Name of the second table on which you need to perform join.
    :param Table1JoinColumn: Name of the column from first table i.e. join key for first table.
    :param Table2JoinColumn: Name of the column from second table i.e. join key for second table.
    :param OutputTable: Name of the table where the output needs to be stored.
    :param openconnection: connection to the database.
    :return:
    """
    # num of threads to use
    num_of_threads = 5
    cur = openconnection.cursor()

    # get the interval value of the given join column (float)
    cur.execute("SELECT MAX({}) FROM {}".format(Table1JoinColumn, InputTable1))
    sorting_col_max_1 = cur.fetchone()[0]
    cur.execute("SELECT MIN({}) FROM {}".format(Table1JoinColumn, InputTable1))
    sorting_col_min_1 = cur.fetchone()[0]
    cur.execute("SELECT MAX({}) FROM {}".format(Table2JoinColumn, InputTable2))
    sorting_col_max_2 = cur.fetchone()[0]
    cur.execute("SELECT MIN({}) FROM {}".format(Table2JoinColumn, InputTable2))
    sorting_col_min_2 = cur.fetchone()[0]
    minmin = min(sorting_col_min_1, sorting_col_min_2)
    maxmax = max(sorting_col_max_1, sorting_col_max_2)
    interval = (maxmax - minmin)/float(num_of_threads)

    # create OutputTable with all the column names from InputTable1 and InputTable2
    cur.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    cur.execute("CREATE TABLE {} AS SELECT * FROM {}, {} WHERE 1=2".format(OutputTable, InputTable1, InputTable2))

    # use multiple threads
    threads = []
    for thread_num in range(num_of_threads):

        cur.execute("DROP TABLE IF EXISTS JoinPart{}".format(thread_num))
        cur.execute("CREATE TABLE JoinPart{} AS SELECT * FROM {},{} WHERE 1=2".format(thread_num, InputTable1, InputTable2))

        lower, upper = minmin + thread_num * interval, minmin + (thread_num + 1) * interval
        t = threading.Thread(target=Joinfunction,
                             args=(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,
                                   lower, upper, 'JoinPart'+str(thread_num)))
        t.start()
        threads.append(t)

    # join threads
    [thread.join() for thread in threads]

    # append each of the joined tables into OutputTable
    for thread_num in range(num_of_threads):
        cur.execute("INSERT INTO {} SELECT * FROM JoinPart{}".format(OutputTable, thread_num))
        cur.execute("DROP TABLE IF EXISTS JoinPart{}".format(thread_num))

    cur.close()
    openconnection.commit()


def Joinfunction(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lower, upper, JoinPart):
    """
    This function is used for threading. It first partition two input tables based on the lower and upper bound then
    inner join partitions from two input tables and save into JoinPart.
    :param openconnection:
    :param InputTable1:
    :param InputTable2:
    :param Table1JoinColumn:
    :param Table2JoinColumn:
    :param lower:
    :param upper:
    :param OutputTable:
    :return:
    """
    cur = openconnection.cursor()
    cur.execute("INSERT INTO {0} SELECT * FROM {1} INNER JOIN {2} ON {1}.{3}={2}.{4} WHERE {1}.{3}<={5} AND {1}.{3}>={6}".
                format(JoinPart, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, upper, lower))
    cur.close()
    openconnection.commit()
