import sqlite3
from typing import *
from prefect import flow, task
import pandas as pd
from sodapy import Socrata
import os
import boto3
import json

DATASET_SOURCE = "data.cstx.gov"
KEY = "g7k6-2zz6"
DATABASE_PATH = r"./warehouse.db"
EXPECTED_NUM_STREET_PAIRS = 112

EXPECTED_KEYS = [
    'systemid',
    'origreaderid',
    'origreaderid_location',
    'destreaderid',
    'destreaderid_location',
    'origroadway',
    'origcrossstreet',
    'origdirection',
    'destroadway',
    'destcrossstreet',
    'destdirection',
    'segmentlenmiles',
    'datetimerecorded',
    'traveltime',
    'speedmph',
    'speedmphstddev',
    'summaryminutes',
    'summarysamples',
    'mapdisplay',
    'substitutespeed'
 ]

def validate_results(results: List[Dict]):
    # make sure that all of our elements are present
    num_street_pairs = len(results)
    num_results_msg = f"Expected {EXPECTED_NUM_STREET_PAIRS} results from API. Received {num_street_pairs}"
    assert num_street_pairs == EXPECTED_NUM_STREET_PAIRS, num_results_msg

    # make sure that the keys are still the same in a given result
    for key in EXPECTED_KEYS:
        assert key in results[0], f"API results missing key {key}"


@task(name="Get data from traffic API", retries=3, retry_delay_seconds=15)
def get_data(key):
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(DATASET_SOURCE, None)

    # https://dev.socrata.com/foundry/data.cstx.gov/g7k6-2zz6
    # returns the travel times for 112 source/destination pairs
    results = client.get(key, limit=200)
    validate_results(results)

    return results


@task(name="Extract average speed by cardinal direction")
def speed_by_direction(results: List[Dict]):
    results_df = pd.DataFrame.from_records(results)
    results_df["speedmph"] = results_df["speedmph"].astype(int)
    speed_by_dir = results_df.groupby("origdirection").mean().speedmph

    return speed_by_dir


@task(name="Save raw results to s3")
def save_raw_results(results: List[Dict]):
    access_key_id = os.getenv("ACCESS_KEY_ID")
    access_key_secret = os.getenv("KEY_SECRET")

    key = "cs-api-" + results[0]["datetimerecorded"] + ".txt"

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_key_secret
    )

    s3 = session.resource('s3')
    object = s3.Object("c1-de-demo-bucket", key)
    object.put(Body=json.dumps(results))


@task(name="Write average speeds to database")
def write_speeds(speed_by_dir, results):
    insert_statement = """
    INSERT INTO speeds (direction, speed, timestamp)
    VALUES(?, ?, ?);
    """
    conn = sqlite3.connect(DATABASE_PATH)
    datetime = results[0]["datetimerecorded"]
    cursor = conn.cursor()

    for direction, speed in zip(speed_by_dir.index, speed_by_dir):
        cursor.execute(insert_statement, (direction, speed, datetime))
    conn.commit()
    conn.close()


@flow(name="collect-traffic-speed-by-cardinal-direction")
def get_cstat_data(key):
    data = get_data(key)
    speeds = speed_by_direction(data)
    save_raw_results(data)
    write_speeds(speeds, data)

if __name__ == "__main__":
    get_cstat_data(KEY)