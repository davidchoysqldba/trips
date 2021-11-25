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

def convert_float(s):
    if str(s).isdigit() and '.' in str(s):
        return float(s)
    return None

def convert_int(s):
    if str(s).isdigit() and '.' not in str(s):
        return int(s)
    return None


# This is supplement function for converting to posix time
def _convert_posix_time(value, value_format="%m/%d/%Y %H:%M"):
    try:
        return datetime.strptime(value, value_format).timestamp()
    except Exception as error:
        logger.exception(error)
        return None
# posix convert fields
dict_transform = {'start_time': _convert_posix_time, 'end_time': _convert_posix_time}


# field check list
dict_field_types = {'trip_id': convert_int, 'duration': convert_int, 'bike_id': convert_int, 'plan_duration': convert_int}
def field_type_checker(field_name: str, field_type: str):
    return dict_field_types[field_name] == type(field_type)


# This function is used to convert posix fields and check field types
def field_checker(row: Dict):
    return_row = row.copy()
    error = False
    
    # loop through posix time fields
    for trans_field_name, trans_field_function in dict_transform.items():
        trans_value = trans_field_function(return_row[trans_field_name])
        # print(trans_value)
        if trans_value == None:
            logger.exception(f"Error converting to posix time: {row}")
            error = True
            break
        else:
            return_row[trans_field_name] = trans_value
    
    # loop through number fields to check
    if error != True:
        for convert_field_name, convert_field_type in dict_field_types.items():
            convert_value = convert_field_type(return_row[convert_field_name])
            if  convert_value == None:
                logger.exception(f"Error invalid data type for: {row}")
                error = True
                break
            else:
                return_row[convert_field_name] = convert_value
    
    if error != True:
        return return_row


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
        row = field_checker(r)
        if row:
            yield row


def load(trips: Iterable[Dict], conn: sqlite3.Connection):
    """
    This function should:

    1. Accept a collection of trip object data and a connection to the trip database
    2. Insert the trip records into the database
    """
    for t in trips:
        if t:
            # print(t)
            try:
                cur = conn.cursor()
                cur.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tuple(t.values()))
            except Exception as error:
                logger.exception('error inserting int trips table', t)
    conn.commit()
    cur.execute('select * from trips')


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
