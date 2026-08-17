
""""
Creation of normalized airlines dataset.
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt
import shutil
import os
import glob
import re
from lib import sql_ingestion as sql


## FILE_PATH for batching processing
INPUT_FILE_PATH = 'input'
NORMALIZED_FILE_PATH  = 'input/{}/normalized'
PROCESSED_FILE_PATH = 'input/{}/processed'
ARCHIVE_FILE_PATH = 'input/{}/archive'


def load_and_process_airlines_data():
    SOURCE = 'airlines'

    airline_files = glob.glob(os.path.join(INPUT_FILE_PATH, SOURCE,'airlines.csv'))
    new_files = len(airline_files)
    print(f'new files to process: {new_files}')

    if airline_files:

        for file in airline_files:

            ## load data 
            print('loading airlines data')
            airlines = pd.read_csv(file)

            ## add processed timestamp
            airlines['processed_at'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            ## move original file to processed folder
            shutil.move(os.path.join(INPUT_FILE_PATH, 'airlines.csv'), 
                        os.path.join(PROCESSED_FILE_PATH.format(SOURCE), 'airlines.csv'))

            ## save new file to normalized folder
            print('saving normalized airlines')
            file_str = os.path.join(NORMALIZED_FILE_PATH.format(SOURCE), 'airlines_normalized.csv')
            airlines.to_csv(file_str, index=None)

    ## load and move to archive
    load_to_rds(SOURCE, sql.Airlines, 'iata_code')



def load_and_process_airports_data():
    SOURCE = 'airports'

    airport_files = glob.glob(os.path.join(INPUT_FILE_PATH, SOURCE,'airlines.csv'))
    new_files = len(airport_files)
    print(f'new files to process: {new_files}')

    if airport_files:

        for file in airport_files:
            ## load data 
            print('loading airports data')
            airports = pd.read_csv(file)

            ## add DOT ID for aiports
            airport_dot = pd.read_csv(INPUT_FILE_PATH + '/T_T100D_MARKET_US_CARRIER_ONLY.csv')
            airport_dot = airport_dot[['DEST_AIRPORT_ID','DEST']].drop_duplicates()
            airport_dot['DEST_AIRPORT_ID'] = airport_dot['DEST_AIRPORT_ID'].astype(str)
            airport_dot.columns=['DOT_ID','IATA_CODE']

            airports = pd.merge(airports, airport_dot, on=['IATA_CODE'], how='left')

            ## add processed timestamp
            airports['processed_at'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            ## move original file to processed folder
            shutil.move(os.path.join(INPUT_FILE_PATH, 'airports.csv'),
                        os.path.join(PROCESSED_FILE_PATH.format(SOURCE), 'airports.csv'))

            ## save new file to normalized folder
            print('saving normalized airports')
            file_str = os.path.join(NORMALIZED_FILE_PATH.format(SOURCE), 'airports_normalized.csv')
            airports.to_csv(file_str, index=None)

    ## load and move to archive
    load_to_rds(SOURCE, sql.Airports, 'iata_code')



def load_and_process_flights_data():
    SOURCE = 'flights'

    flight_files = glob.glob(os.path.join(INPUT_FILE_PATH, SOURCE,'flights_*.csv'))
    new_files = len(flight_files)
    print(f'new files to process: {new_files}')

    if flight_files:

        for file in flight_files:
            ## load data 
            print('loading flights data')
            flights = pd.read_csv(file, dtype=str)

            ## get date string
            file_date = re.search(r'(\d{6})', file).group(0)

            ## create time key from raw date columns
            ## add leading zero for date strings
            flights['MONTH'] = flights['MONTH'].apply(lambda x: '0'+str(x) if len(str(x)) == 1 else str(x))
            flights['DAY'] = flights['DAY'].apply(lambda x: '0'+str(x) if len(str(x)) == 1 else str(x))
            flights['TIME_KEY'] = flights[['YEAR','MONTH','DAY','DAY_OF_WEEK']].astype(str).sum(axis=1)


            ## keep these records in, and fill in TAIL_NUMBER to avoid nulls in primary key 
            flights.fillna({'TAIL_NUMBER':'UNKNOWN'}, inplace=True)
            flights['TAIL_NUMBER'].dropna().apply(lambda x: len(x)).value_counts()

            ## primary keys
            primary_keys = ['TIME_KEY','AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']

            ## create time key relation
            print('creating time key df')
            time_key_df = flights[['TIME_KEY','YEAR','MONTH','DAY','DAY_OF_WEEK']].drop_duplicates().reset_index(drop=True)

            ## create flight_status relation
            print('creating flight status df')
            flight_status_df = flights[primary_keys + ['DIVERTED','CANCELLED',
                                                       'CANCELLATION_REASON']].drop_duplicates().reset_index(drop=True)

            ## create flight_delays relation
            print('creating flight delays df')
            flight_delays_df = flights[primary_keys + ['AIR_SYSTEM_DELAY','SECURITY_DELAY',
                                                       'AIRLINE_DELAY','LATE_AIRCRAFT_DELAY',
                                                       'WEATHER_DELAY']].drop_duplicates().reset_index(drop=True)
            flight_delays_df.dropna(inplace=True)

            ## create flights relation
            print('creating flights df')
            flights_df = flights[primary_keys + 
                                 ['SCHEDULED_DEPARTURE','DEPARTURE_TIME','DEPARTURE_DELAY',
                                  'TAXI_OUT','WHEELS_OFF','SCHEDULED_TIME','ELAPSED_TIME','AIR_TIME',
                                  'DISTANCE','WHEELS_ON','TAXI_IN','SCHEDULED_ARRIVAL',
                                  'ARRIVAL_TIME','ARRIVAL_DELAY']].copy()

            flights_df = flights_df.drop_duplicates().reset_index(drop=True)


            ## clean flight times
            flight_time_cols = ['SCHEDULED_DEPARTURE','DEPARTURE_TIME','WHEELS_OFF','WHEELS_ON','SCHEDULED_ARRIVAL','ARRIVAL_TIME']

            for ftc in flight_time_cols:
                print(ftc)
                flights_df[ftc] = pd.to_datetime(flights_df[ftc].astype(str).str.zfill(4), format="%H%M", errors='coerce').dt.time


            ## move original file to processed folder
            shutil.move(file, os.path.join(PROCESSED_FILE_PATH.format(SOURCE), os.path.basename(file)))


            ## mapping of new file to file name, since these are created from parent flights file
            df_to_filename_map = {
                'time_key': time_key_df,
                'flight_status': flight_status_df,
                'flight_delays': flight_delays_df,
                'flights': flights_df,
            }

            for filename, file in df_to_filename_map.items():

                ## add processed timestamp
                file['processed_at'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

                ## save new file to normalized folder
                print(f'saving normalized {filename}')
                file_str = os.path.join(NORMALIZED_FILE_PATH.format(filename), 
                                        f'{filename}_{file_date}_normalized.csv')
                # print(df_.shape)

                file.to_csv(file_str, index=None)


    ## check for new files
    table_to_filename_map = {
        'time_key': sql.TimeKey,
        'flight_status': sql.FlightStaus,
        'flight_delays': sql.FlightDelays,
        'flights': sql.Flights,
    }

    for filename, table_obj in table_to_filename_map.items():
        load_to_rds(filename, table_obj, 'time_key')


def create_and_process_cancellation_df():
    SOURCE = 'cancellation_reason'

    ## create cancelled_reason mapping relation from the data definitions in kaggle
    cancellation_df = pd.DataFrame([['A', 'Airline/Carrier'],
                                    ['B','Weather'],
                                    ['C','National Air System'],
                                    ['D','Security']],
                                columns=['CANCELLATION_REASON','VALUE'])

    ## add processed timestamp
    cancellation_df['processed_at'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    ## save new file to normalized folder
    file_str = os.path.join(NORMALIZED_FILE_PATH.format(SOURCE), 'cancellation_reason_normalized.csv')
    cancellation_df.to_csv(file_str, index=None)

    load_to_rds(SOURCE, sql.CancellationReason, 'cancellation_reason')


def load_to_rds(source, table_obj, idx_col):

    files = list(glob.glob(os.path.join(NORMALIZED_FILE_PATH.format(source), "*.csv")))
    print(f'files to load: {len(files)}')

    for file in files:
        print(file)

        rds = sql.RedShiftConnection()
        success = rds.load_to_rds(file=file, table_obj=table_obj, idx_col=idx_col)

        if success:
            move_file(file, source)

        else:
            print(f'load failed; {file} not moved.')


def move_file(file_name, source):

    print('moving file...')
    new_loc = os.path.join(ARCHIVE_FILE_PATH.format(source), os.path.basename(file_name))

    if not os.path.exists(ARCHIVE_FILE_PATH.format(source)):
        os.mkdir(ARCHIVE_FILE_PATH.format(source))

    try:
        print("Moving file "+file_name+" to "+new_loc)
        shutil.move(file_name,new_loc)
        print("Moved file "+file_name+" to "+new_loc)
    except:
        print("File "+file_name+" not moved.")  

    

if __name__ == "__main__":

    load_and_process_airports_data()
    load_and_process_airlines_data()
    load_and_process_flights_data()
    create_and_process_cancellation_df()

