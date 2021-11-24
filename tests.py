#!/usr/bin/env python3
import io
import unittest
from typing import Iterable

import indego_trip_etl_solution as indego_trip_etl

sample_csv = """trip_id,duration,start_time,end_time,start_station,start_lat,start_lon,end_station,end_lat,end_lon,bike_id,plan_duration,trip_route_category,passholder_type,bike_type
373614767,7,4/1/2021 0:44,4/1/2021 0:51,3213,39.938869,-75.166634,3000,,,18928,30,One Way,Indego30,electric
3736147z+,7,4/2/2021 0:31,4/2/2021 0:51,3000,39.938869,-75.166634,3213,,,18928,30,One Way,Indego30,electric
373688718,1016,4/1/2021 1:49,4/1/2021 18:45,3214,39.978909,-75.157799,3108,39.953159,-75.165512,16508,30,One Way,Indego30,electric
373688719,1016,99/100/2021 1:49,4/1/2021 18:45,3214,39.978909,-75.157799,3108,39.953159,-75.165512,1650z,30,One Way,Indego30,standard
"""


class EtlTestCase(unittest.TestCase):
    rows = []
    trips = []

    @classmethod
    def setUpClass(cls) -> None:
        cls.csv_file = io.StringIO(sample_csv.strip())
        cls.conn = indego_trip_etl.create_db(name=":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_1_extract(self):
        rows = indego_trip_etl.extract(EtlTestCase.csv_file)
        self.assertIsInstance(rows, Iterable)
        row_cnt = 0
        for row in rows:
            row_cnt += 1
            self.rows.append(row)
        self.assertEqual(len(self.rows), 4)

    def test_2_transform(self):
        trip_cnt = 0
        for trip in indego_trip_etl.transform(self.rows):
            trip_cnt += 1
            self.trips.append(trip)
        self.assertEqual(trip_cnt, 2)
        self.assertEqual(
            set([1617256140.0, 1617252240.0]),
            set([t["start_time"] for t in self.trips]),
        )
        self.assertNotEqual(
            "1892816508",
            sum([t["bike_id"] for t in self.trips]),
        )

    def test_3_load(self):
        indego_trip_etl.load(self.trips, self.conn)
        cur = self.conn.cursor()
        cur.execute("SELECT count(*) from trips")
        row_count = cur.fetchone()
        self.assertEqual(row_count[0], 2)
        cur.execute("SELECT sum(start_station) from trips")
        ss_sum = cur.fetchone()
        self.assertEqual(ss_sum[0], 6427)


if __name__ == "__main__":
    unittest.main()
