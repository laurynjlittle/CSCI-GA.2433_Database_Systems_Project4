"""
Framework for defining the database schema for the airlines data warehouse.
"""

import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy import orm  as sa_orm
from sqlalchemy.ext.declarative import declarative_base
import redshift_connector
import shutil
import numpy as np

from dotenv import load_dotenv


load_dotenv()

## warehouse vars
SQL_PASS = os.environ.get('REDSHIFT_PASS')
SQL_USER = os.environ.get('REDSHIFT_USER')
SQL_DB = os.environ.get('REDSHIFT_DB')
SQL_HOST = os.environ.get('REDSHIFT_HOST')


Base = declarative_base()


class RedShiftConnection():

    def __init__(self):
        self.USER = SQL_USER
        self.HOST = SQL_HOST
        self.PASSWORD = SQL_PASS
        self.DATABASE = SQL_DB


    def create_connection(self):

        url = URL.create(
            drivername='redshift+redshift_connector',
            host=self.HOST,
            database=self.DATABASE,
            username=self.USER,
            password=self.PASSWORD
        )

        return sa.create_engine(url)

    def create_session(self):
        rs_engine = self.create_connection()
        return rs_engine, sa_orm.create_session(bind=rs_engine)


    def create_tables(self, engine, table_object=None):

        if table_object is not None:
            Base.metadata.create_all(engine, tables=[table_object])

        else:
            Base.metadata.create_all(engine)


    def load_to_rds(self, file, table_obj, idx_col):

        try:
            rs_engine, session = self.create_session()
            self.create_tables(rs_engine, table_object=table_obj.__table__)

            columns = table_obj.__table__.columns.keys()
            parameter_indices = pd.RangeIndex(0, len(columns)).tolist()

            table = f"{table_obj.__table__.schema}.{table_obj.__table__.name}"
            conn = rs_engine.raw_connection()

            col_obj = getattr(table_obj, idx_col)

      
            staging_table = sa.Table(
                "staging_temp",
                sa.MetaData(),
                *[col.copy() for col in table_obj.__table__.columns],
                prefixes=["TEMPORARY"]
            )

            with session.begin():

                staging_table.create(bind=session.connection(), checkfirst=True)
                # raw_conn = session.connection().connection.dbapi_connection

                with conn.cursor() as cursor:

                    print('creating staging table...')
                    compiled_create = sa.schema.CreateTable(staging_table).compile(dialect=session.bind.dialect)
                    cursor.execute(str(compiled_create))

                    print('inserting records into staging...')
                    ## bulk insert
                    cursor.insert_data_bulk(
                        filename=file, 
                        table_name="staging_temp", 
                        parameter_indices = parameter_indices,
                        column_names=columns,
                        delimiter=',',
                        batch_size=1024
                        )

                print(f'delete from {table} ahead of insert')

                # delete records that are being reuploaded
                delete_stmt = sa.text(f"""
                    DELETE FROM {table}
                    USING staging_temp
                    WHERE {table_obj.__table__.name}.{col_obj.name} = staging_temp.{col_obj.name}
                """)

                session.execute(delete_stmt)

                print(f'insert new records into {table}')
                insert_from_staging = sa.insert(table_obj).from_select(
                    columns,
                    sa.select(staging_table)
                )
                session.execute(insert_from_staging)

                # conn.commit()

        except Exception as e:
            print('error during loading to Redshift: ',e)
            return False

        return True


class Airlines(Base):

    __tablename__ = 'dim_airlines'
    __table_args__ = {"schema": "project"}


    iata_code = sa.Column('iata_code', sa.VARCHAR(50), primary_key=True)
    airline_name = sa.Column('airline_name',sa.VARCHAR(255), nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)


class Airports(Base):

    __tablename__ = 'dim_airports'
    __table_args__ = {"schema": "project"}

    iata_code = sa.Column('iata_code', sa.VARCHAR(50), primary_key=True)
    airport = sa.Column('airport',sa.VARCHAR(255), nullable=True)
    city = sa.Column('city', sa.VARCHAR(255), nullable=True)
    state = sa.Column('state', sa.VARCHAR(255), nullable=True)
    country = sa.Column('country', sa.VARCHAR(255), nullable=True)
    latitude = sa.Column('latitude', sa.FLOAT, nullable=True)
    longitude = sa.Column('longitude', sa.FLOAT, nullable=True)
    dot_id = sa.Column('dot_id', sa.VARCHAR(255), nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)


class TimeKey(Base):

    __tablename__ = 'dim_time_key'
    __table_args__ = {"schema": "project"}

    time_key = sa.Column('time_key',sa.VARCHAR(50), primary_key=True)
    year = sa.Column('year', sa.VARCHAR(10), nullable=True)
    month = sa.Column('month', sa.VARCHAR(10), nullable=True)
    day = sa.Column('day', sa.VARCHAR(10), nullable=True)
    day_of_week = sa.Column('day_of_week', sa.INT, nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)


class FlightStaus(Base):

    __tablename__ = 'dim_flight_status'
    __table_args__ = {
        "schema": "project",
        "redshift_diststyle": "KEY", 
        "redshift_distkey": "airline",
        "redshift_sortkey": "time_key",
    }

    time_key = sa.Column('time_key', sa.VARCHAR(50), primary_key=True)
    airline = sa.Column('airline',sa.VARCHAR(255), primary_key=True)
    flight_number = sa.Column('flight_number', sa.VARCHAR(50), primary_key=True)
    tail_number = sa.Column('tail_number', sa.VARCHAR(50), primary_key=True)
    origin_airport = sa.Column('origin_airport', sa.VARCHAR(255), primary_key=True)
    destination_airport = sa.Column('destination_airport', sa.VARCHAR(255), primary_key=True)
    diverted = sa.Column('diverted', sa.BOOLEAN, nullable=True)
    cancelled = sa.Column('cancelled', sa.BOOLEAN, nullable=True)
    cancellation_reason = sa.Column('cancellation_reason', sa.VARCHAR(10), nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)


class FlightDelays(Base):

    __tablename__ = 'fct_flight_delays'
    __table_args__ = {
        "schema": "project",
        "redshift_diststyle": "KEY", 
        "redshift_distkey": "airline",
        "redshift_sortkey": "time_key",
    }

    time_key = sa.Column('time_key', sa.VARCHAR(50), primary_key=True)
    airline = sa.Column('airline',sa.VARCHAR(255), primary_key=True)
    flight_number = sa.Column('flight_number', sa.VARCHAR(50), primary_key=True)
    tail_number = sa.Column('tail_number', sa.VARCHAR(50), primary_key=True)
    origin_airport = sa.Column('origin_airport', sa.VARCHAR(255), primary_key=True)
    destination_airport = sa.Column('destination_airport', sa.VARCHAR(255), primary_key=True)
    air_system_delay = sa.Column('air_system_delay',sa.FLOAT, nullable=True)
    security_delay = sa.Column('security_delay', sa.FLOAT, nullable=True)
    airline_delay = sa.Column('airline_delay', sa.FLOAT, nullable=True)
    late_aircraft_delay = sa.Column('late_aircraft_delay', sa.FLOAT, nullable=True)
    weather_delay = sa.Column('weather_delay', sa.FLOAT, nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)


class Flights(Base):

    __tablename__ = 'fct_flights'
    __table_args__ = {
        "schema": "project",
        "redshift_diststyle": "KEY", 
        "redshift_distkey": "airline",
        "redshift_sortkey": "time_key",
    }

    time_key = sa.Column('time_key', sa.VARCHAR(50), primary_key=True)
    airline = sa.Column('airline',sa.VARCHAR(255), primary_key=True)
    flight_number = sa.Column('flight_number', sa.VARCHAR(50), primary_key=True)
    tail_number = sa.Column('tail_number', sa.VARCHAR(50), primary_key=True)
    origin_airport = sa.Column('origin_airport', sa.VARCHAR(255), primary_key=True)
    destination_airport = sa.Column('destination_airport', sa.VARCHAR(255), primary_key=True)
    scheduled_departure = sa.Column('scheduled_departure', sa.VARCHAR(50), nullable=True)
    departure_time = sa.Column('departure_time',  sa.VARCHAR(50), nullable=True)
    departure_delay = sa.Column('departure_delay', sa.FLOAT, nullable=True)
    taxi_out = sa.Column('taxi_out', sa.FLOAT, nullable=True)
    wheels_off = sa.Column('wheels_off',  sa.VARCHAR(50), nullable=True)
    scheduled_time = sa.Column('scheduled_time', sa.FLOAT, nullable=True)
    elapsed_time = sa.Column('elapsed_time', sa.FLOAT, nullable=True)
    air_time = sa.Column('air_time',sa.FLOAT, nullable=True)
    distance = sa.Column('distance', sa.FLOAT, nullable=True)
    wheels_on = sa.Column('wheels_on', sa.VARCHAR(50), nullable=True)
    taxi_in = sa.Column('taxi_in', sa.FLOAT, nullable=True)
    scheduled_arrival = sa.Column('scheduled_arrival', sa.VARCHAR(50), nullable=True)
    arrival_time = sa.Column('arrival_time', sa.VARCHAR(50), nullable=True)
    arrival_delay = sa.Column('arrival_delay', sa.FLOAT, nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)



class CancellationReason(Base):

    __tablename__ = 'dim_cancellation_reason'
    __table_args__ = {"schema": "project"}

    cancellation_reason = sa.Column('cancellation_reason',sa.VARCHAR(10), primary_key=True)
    value = sa.Column('value',sa.VARCHAR(255), nullable=True)
    processed_at = sa.Column('processed_at', sa.TIMESTAMP)

