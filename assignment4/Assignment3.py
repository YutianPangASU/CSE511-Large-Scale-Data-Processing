#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    """
    Implement a Python function Load Ratings() that takes a file system path that contains the rating.dat file as input.
    Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has
    the following schema
       UserID(int) - MovieID(int) - Rating(float)

    :param ratingstablename:
    :param ratingsfilepath:
    :param openconnection:
    :return:
    """

    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename + ";")
    cur.execute("CREATE TABLE " + ratingstablename +
                " (UserID INT, DUMMY1 CHAR, MovieID INT, DUMMY2 CHAR, Rating FLOAT, DUMMY3 CHAR, Timestamp BIGINT);")
    with open(ratingsfilepath, 'r') as path:
        cur.copy_from(path, ratingstablename, sep=':')
    cur.execute("ALTER TABLE " + ratingstablename +
                " DROP COLUMN DUMMY1, DROP COLUMN DUMMY2, DROP COLUMN DUMMY3, DROP COLUMN Timestamp;")
    cur.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    """
    Implement a Python function Range_Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and
    (2) an integer value N; that represents the number of partitions. Range_Partition() then generates N horizontal
    fragments of the Ratings table and store them in PostgreSQL. The algorithm should partition the ratings table
    based on N uniform ranges of the Rating attribute.

    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    """

    cur = openconnection.cursor()
    interval = 5.0 / numberofpartitions
    for partition_number in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS range_part" + str(partition_number) + ";")
        if partition_number == 0:
            cur.execute("CREATE TABLE range_part" + str(partition_number) + " AS " +
                        "SELECT * FROM " + str(ratingstablename) + " WHERE Rating>=" + str(partition_number*interval) +
                        " AND Rating<=" + str((1+partition_number)*interval) + ";")
        else:
            cur.execute("CREATE TABLE range_part" + str(partition_number) + " AS " +
                        "SELECT * FROM " + str(ratingstablename) + " WHERE Rating>" + str(partition_number*interval) +
                        " AND Rating<=" + str((1+partition_number)*interval) + ";")

    cur.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    """
    Implement a Python function RoundRobin_Partition() that takes as input: (1) the Ratings table stored in PostgreSQL
    and (2) an integer value N; that represents the number of partitions. The function then generates N horizontal
    fragments of the Ratings table and stores them in PostgreSQL. The algorithm should partition the ratings table
    using the round robin partitioning approach.

    :param ratingstablename:
    :param numberofpartitions:
    :param openconnection:
    :return:
    """

    cur = openconnection.cursor()

    for partition_number in range(numberofpartitions):
        cur.execute("CREATE TABLE rrobin_part" + str(partition_number) + " (UserID INT, MovieID INT, Rating FLOAT);")

    cur.execute("SELECT * FROM " + ratingstablename + ";")
    rows = cur.fetchall()

    row_number = 0
    for row in rows:
        cur.execute("INSERT INTO rrobin_part" + str(row_number % numberofpartitions) +
                    " VALUES (" + str(row[0]) + ", " + str(row[1]) + ", " + str(row[2]) + ");")
        row_number = row_number + 1

    cur.close()
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
     Implement a Python function RoundRobin_Insert() that takes as input: (1) Ratings table stored in PostgreSQL,
     (2) UserID, (3) ItemID, (4) Rating. RoundRobin_Insert() then inserts a new tuple to the Ratings table and
     the right fragment based on the round robin approach.

    :param ratingstablename:
    :param userid:
    :param itemid:
    :param rating:
    :param openconnection:
    :return:
    """

    cur = openconnection.cursor()

    cur.execute("SELECT * FROM " + ratingstablename + ";")
    number_of_rows = len(cur.fetchall())

    cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%rrobin_part%';")
    num_of_partitions = len(cur.fetchall())

    cur.execute("INSERT INTO " + str(ratingstablename) +
                " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")

    cur.execute("INSERT INTO rrobin_part" + str(number_of_rows % num_of_partitions) +
                " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")

    cur.close()
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Implement a Python function Range_Insert() that takes as input: (1) Ratings table stored in PostgreSQL
    (2) UserID, (3) ItemID, (4) Rating. Range_Insert() then inserts a new tuple to the Ratings table and
    the correct fragment (of the partitioned ratings table) based upon the Rating value.

    :param ratingstablename:
    :param userid:
    :param itemid:
    :param rating:
    :param openconnection:
    :return:
    """

    cur = openconnection.cursor()

    cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%range_part%';")
    num_of_partitions = len(cur.fetchall())

    cur.execute("INSERT INTO " + str(ratingstablename) +
                " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")

    interval = 5.0 / num_of_partitions

    cur.execute("INSERT INTO range_part" + str(int(rating/interval)-1) +
                " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")

    cur.close()
    openconnection.commit()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
