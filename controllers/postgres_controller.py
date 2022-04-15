import psycopg2
from configparser import ConfigParser
import os

columns= ['FPE', 'focus_area', 'legal_status', 'name', 'location_municipality', 'location_postal_code', 'legal_designation_type', 'location_region', 'location_country', 'revenue', 'employees', 'website', 'npk_id', 'regulating_authority', 'revenue_currency', 'revenue_year', 'data_source', 'legal_status_date', 'record_type']


def read_table(_id, type, identifier,limit=10):
    """ create tables in the PostgreSQL database"""
    command =  """
        SELECT * FROM {type}
        WHERE {identifier} = {_id}
        LIMIT {limit}
       """.format(type=type, identifier=identifier, _id=_id, limit=limit)
    conn = None

    try:
        # reading params from env vars
        DATABASE=os.getenv('DATABASE')
        HOST=os.getenv('HOST')
        USER=os.getenv('USER')
        PASSWORD=os.getenv('PASSWORD')
        PORT=os.getenv('PORT')

        conn = psycopg2.connect(
            dbname=DATABASE, 
            user=USER, 
            password=PASSWORD, 
            host=HOST,
            port=PORT)

        cur = conn.cursor()

        # create table one by one
        cur.execute(command)

        rows = cur.fetchall() 
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in rows]
        print(r)
        
        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes
        conn.commit()

        return r

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    read_table()