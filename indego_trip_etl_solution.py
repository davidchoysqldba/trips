#!/usr/bin/env python3
"""
Generates a bike share trip data database via a basic ETL pipeline.

Example usage as an executable:

    python indego_trip_etl.py indego-trips-2021-q2.csv
    
will generate a sqlite3 database file: trips.db

A full dataset can be found at:

https://u626n26h74f16ig1p3pt0f2g-wpengine.netdna-ssl.com/wp-content/uploads/2021/07/indego-trips-2021-q2.zip
"""
import logging
import sqlite3
import sys
from typing import Dict, Iterable, TextIO

import csv
from datetime import datetime


logger = logging.getLogger()

# This is supplement function for converting to posix time
def _convert_posix_time(value, value_format="%m/%d/%Y %H:%M"):
    try:
        return datetime.strptime(value, value_format).timestamp()
    except Exception as error:
        logger.exception(error)
        return None
dict_transform = {'start_time': _convert_posix_time, 'end_time': _convert_posix_time}


def create_db(name: str = "trips.db") -> sqlite3.Connection:
    """
    This function should:

    1. Create a new sqlite3 database using the supplied name
    2. Create a `trips` table to allow our indego bike data to be queryable via SQL
    3. Return a connection to the database

    Note: This function should be idempotent
    """
    conn = sqlite3.connect(name)
    
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS trips
                   (trip_id integer primary key, duration integer, start_time text, end_time text, start_station integer, start_lat text, start_lon text, end_station text, end_lat text, end_lon text, bike_id integer, plan_duration integer, trip_route_category text, passholder_type text, bike_type text)''')

    cur.execute('delete from trips')

    return conn


def extract(file: TextIO) -> Iterable:
    """
    This function should:

    1. Accept a file-like object (Text I/O)
    2. Return an iterable value to be transformed
    """
    # use generator to return as iterable
    for f in csv.DictReader(file):
        yield f


def transform(rows: Iterable) -> Iterable[Dict]:
    """
    This function should:

    1. Accept an iterable rows value to be transformed
    2. Transform any date time value into a POSIX timestamp
    3. Transform remaining fields into sqlite3 supported types
    4. Output to stdout or stderr if a row fails to be transformed
    5. Return an iterable collection of transformed rows as trip dictionaries to be loaded into our trips table
    """
    for r in rows:
        try:
            error = False
            for field, transform_function in dict_transform.items():
                transformed_value = transform_function(r[field])
                if transformed_value == None:
                    # yield None
                    error = True
                r[field] = transformed_value
            if not error:
                print('transform: ', r)
                yield r
        except Exception as error:
            logger.exception('error parsing start_time and end_time', r)
            # yield None


def load(trips: Iterable[Dict], conn: sqlite3.Connection):
    """
    This function should:

    1. Accept a collection of trip object data and a connection to the trip database
    2. Insert the trip records into the database
    """
    for t in trips:
        print(t)
        if t:
            try:
                cur = conn.cursor()
                cur.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tuple(t.values()))
            except Exception as error:
                logger.exception('error inserting int trips table', t)
    conn.commit()
    cur.execute('select * from trips')
    print('-----------------------------------------')
    for r in cur.fetchall():
        print(r)


def main(fname) -> None:
    """Given an indego bike trip csv file, run our ETL process on it for further querying"""
    conn = create_db()
    with open(fname) as f:
        rows = extract(f)
        trip_objs = transform(rows)
        load(trip_objs, conn)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(fname=sys.argv[1]))
    except Exception as e:
        logger.exception(e)
        sys.exit(1)