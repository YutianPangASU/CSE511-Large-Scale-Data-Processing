#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Do not close the connection inside this file i.e. do not perform openconnection.close()


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()

    # write headers
    if os.path.exists('RangeQueryOut.txt'):
        os.remove('RangeQueryOut.txt')
    with open('RangeQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')

    # query range tables and save into file
    cur.execute("SELECT MAX(partitionnum)+1 FROM rangeratingsmetadata;")
    range_num = int(cur.fetchone()[0])

    for range_partition_num in range(range_num):
        range_query = "SELECT 'RangeRatingsPart" + str(range_partition_num) + "' AS PartitionName, UserID, MovieID, Rating " \
                      "FROM rangeratingspart" + str(range_partition_num) + \
                      " WHERE rating <= " + str(ratingMaxValue) + " AND rating >= " + str(ratingMinValue)

        cur.execute(range_query)
        range_query_result = cur.fetchall()

        with open('RangeQueryOut.txt', 'a') as f:
            for line in range_query_result:
                line = ", ".join(map(str, line))
                f.write(str(line) + '\n')

    # query roundrobin and save into file
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr_num = int(cur.fetchone()[0])

    for rr_partition_num in range(rr_num):
        range_query = "SELECT 'RoundRobinRatingsPart" + str(rr_partition_num) + "' AS PartitionName, UserID, MovieID, Rating " \
                      "FROM roundrobinratingspart" + str(rr_partition_num) + \
                      " WHERE rating <= " + str(ratingMaxValue) + " AND rating >= " + str(ratingMinValue)

        cur.execute(range_query)
        rr_query_result = cur.fetchall()

        with open('RangeQueryOut.txt', 'a') as f:
            for line in rr_query_result:
                line = ", ".join(map(str, line))
                f.write(str(line) + '\n')

    openconnection.commit()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()

    # write headers
    if os.path.exists('PointQueryOut.txt'):
        os.remove('PointQueryOut.txt')
    with open('PointQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')

    # query range tables and save into file
    cur.execute("SELECT MAX(partitionnum)+1 FROM rangeratingsmetadata;")
    range_num = int(cur.fetchone()[0])

    for range_partition_num in range(range_num):
        range_query = "SELECT 'RangeRatingsPart" + str(range_partition_num) + "' AS PartitionName, UserID, MovieID, Rating " \
                      "FROM rangeratingspart" + str(range_partition_num) + \
                      " WHERE rating = " + str(ratingValue)

        cur.execute(range_query)
        range_query_result = cur.fetchall()

        with open('PointQueryOut.txt', 'a') as f:
            for line in range_query_result:
                line = ", ".join(map(str, line))
                f.write(str(line) + '\n')

    # query roundrobin and save into file
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr_num = int(cur.fetchone()[0])

    for rr_partition_num in range(rr_num):
        range_query = "SELECT 'RoundRobinRatingsPart" + str(rr_partition_num) + "' AS PartitionName, UserID, MovieID, Rating " \
                      "FROM roundrobinratingspart" + str(rr_partition_num) + \
                      " WHERE rating = " + str(ratingValue)

        cur.execute(range_query)
        rr_query_result = cur.fetchall()

        with open('PointQueryOut.txt', 'a') as f:
            for line in rr_query_result:
                line = ", ".join(map(str, line))
                f.write(str(line) + '\n')

    openconnection.commit()


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
