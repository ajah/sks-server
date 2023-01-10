import psycopg2
import csv
import glob
import os
from itertools import islice
import sys

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)

activities = None
entities = None

os.chdir("sks-backend/data/processed")
for file in glob.glob("*.csv"):
    if 'activities' in file:
        activities = file
    if 'interface' in file:
        entities = file


def create_tables():
    """ create tables in the PostgreSQL database"""
    act_command = """
        CREATE TABLE activities (
            act_sks_id INTEGER PRIMARY KEY,
            source_id VARCHAR,
            source_authority VARCHAR,
            source_url VARCHAR,
            grant_title VARCHAR,
            funding_amount FLOAT,
            funding_type VARCHAR,
            funder VARCHAR,
            recipient_organization VARCHAR,
            recipient_id VARCHAR,
            grant_description VARCHAR,
            funder_id VARCHAR,
            grant_region VARCHAR,
            grant_municipality VARCHAR,
            date VARCHAR,
            date_type VARCHAR,
            end_date VARCHAR,
            end_date_type VARCHAR,
            expected_results VARCHAR,
            ent_sks_id INTEGER,
            program_name VARCHAR,
            type VARCHAR
        )
        """
    # Note; revenue & employees need to be changed later
    ent_command = """
        CREATE TABLE entities (
            ent_sks_id INTEGER PRIMARY KEY,
            external_id VARCHAR,
            FPE VARCHAR,
            focus_area VARCHAR,
            legal_status VARCHAR,
            name VARCHAR,
            location_municipality VARCHAR, 
            location_postal_code VARCHAR,
            legal_designation_type VARCHAR,
            location_region VARCHAR,
            location_country VARCHAR,
            revenue VARCHAR,
            employees VARCHAR,
            website VARCHAR,
            website_text VARCHAR,
            regulating_authority VARCHAR,
            revenue_currency VARCHAR,
            revenue_year VARCHAR,
            data_source VARCHAR,
            legal_status_date VARCHAR,
            type VARCHAR
        )
        """
    conn = None
    try:
        # read the connection parameters
        # reading params from env vars
        DATABASE = os.getenv('DATABASE')
        HOST = os.getenv('HOST')
        USER = os.getenv('USER')
        PASSWORD = os.getenv('PASSWORD')
        DB_PORT = os.getenv('DB_PORT')

        conn = psycopg2.connect(
            dbname=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=DB_PORT)
        # connect to the PostgreSQL server

        cur = conn.cursor()
        cur.execute(act_command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_rows_acts():
    with open(activities, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row.
        for row in reader:
            # for row in islice(reader, 10):
            try:
                insert_row_act(row)
            except:
                pass


def insert_row_act(row):
    act_fields = [
        'act_sks_id',
        'source_id',
        'source_authority',
        'source_url',
        'grant_title',
        'funding_amount',
        'funding_type',
        'funder',
        'recipient_organization',
        'recipient_id',
        'grant_description',
        'funder_id',
        'grant_region',
        'grant_municipality',
        'date',
        'date_type',
        'end_date',
        'end_date_type',
        'expected_results',
        'ent_sks_id',
        'program_name',
        'type',
    ]
    placeholders = ["%s"] * len(act_fields)  # Generates correct number of placeholders

    sql = "INSERT INTO activities({}) VALUES({}) RETURNING act_sks_id".format(
        ", ".join(act_fields), ", ".join(placeholders))

    conn = None
    record_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (row))
        # get the generated id back
        record_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return record_id


def insert_row_ents(row):
    ent_fields = [
        'external_id',
        'FPE',
        'focus_area',
        'legal_status',
        'name',
        'location_municipality',
        'location_postal_code',
        'legal_designation_type',
        'location_region',
        'location_country',
        'revenue',
        'employees',
        'website',
        'website_text',
        'ent_sks_id',
        'regulating_authority',
        'revenue_currency',
        'revenue_year',
        'data_source',
        'legal_status_date',
        'type'
    ]
    placeholders = ["%s"] * len(ent_fields)  # Generates correct number of placeholders

    sql = "INSERT INTO entities({}) VALUES({}) RETURNING ent_sks_id".format(
        ", ".join(ent_fields), ", ".join(placeholders))

    conn = None
    record_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (row))
        # get the generated id back
        record_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return record_id


def insert_rows_ents():
    with open(entities, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row.
        for row in reader:
            try:
                insert_row_ents(row)
            except:
                pass


if __name__ == '__main__':
    create_tables()
    insert_rows_acts()
    insert_rows_ents()
