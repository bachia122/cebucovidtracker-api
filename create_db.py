import pandas as pd
import sqlite3
import time
import os 
from os import listdir
from datetime import date, datetime

def delete_old_db(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print("The file does not exist.")


def create_connection(database):
    conn = None
    print("----------Connecting to database using Sqlite3 version {version} ...".format(version = sqlite3.version))
    conn = sqlite3.connect(database)
    print("----------Connected to {database}".format(database = database))
    if conn:
        conn.close()

def create_casesdb(csv, db, tablename):
    conn = sqlite3.connect(db) 
    fields = ['CaseCode','DateRepConf','DateDied','DateRecover','ProvRes','CityMunRes','HealthStatus', 'DateSpecimen', 'DateOnset']
    df = pd.read_csv(csv, usecols=fields, low_memory=False)
    df = df.fillna(value="NULL")
    df = df.loc[df['ProvRes'] == 'CEBU']

    # new cases: if DateOnset is 'NULL', replace with DateSpecimen
    df['DateNewCase'] = df.apply(lambda row: get_datenewcase(row), axis = 1)

    # just some string cleaning (insert flowerbeds images here)
    df['CityMunRes'] = df['CityMunRes'].str.title()
    df = df.replace(to_replace='City Of Talisay', value='Talisay City')
    df = df.replace(to_replace='City Of Naga', value='Naga City')
    df = df.replace(to_replace='Null', value='Location Unknown')
    df = df.replace(to_replace='Cebu City (Capital)', value='Cebu City')
    df = df.replace(to_replace='Lapu-Lapu City (Opon)', value='Lapu-Lapu City')
    df = df.replace(to_replace='City Of Carcar', value='Carcar City')
    df = df.replace(to_replace='City Of Bogo', value='Bogo City')

    df.to_sql(tablename, conn, if_exists = "append", index = False)
 
def get_datenewcase(row):
    if row['DateOnset'] == 'NULL':
        return row['DateSpecimen']
    else:
        return row['DateOnset']


def create_testsdb(csv, db, tablename):
    conn = sqlite3.connect(db) 
    fields = ['facility_name','report_date', 'daily_output_unique_individuals','daily_output_positive_individuals','cumulative_unique_individuals','cumulative_positive_individuals','cumulative_negative_individuals','pct_positive_cumulative','remaining_available_tests']
    #sadly, no way to auto-retrieve the facilities; had to countercheck with gov records
    tests_list =['Allegiant Regional Care Hospital','BioPath Clinical Diagnostics, Inc - CEBU', 'BioPath Clinical Diagnostics, Inc. - AFRIMS', 'Biopath Clinical Diagnostics, Inc. - AFRIMS GeneXpert Laboratory', 'Cebu Doctors University Hospital, Inc.', 'Cebu TB Reference Laboratory - GeneXpert', 'Cebu TB Reference Laboratory - Molecular Facility for COVID-19 Testing', 'Chong Hua Hospital', 'Philippine Red Cross - Cebu Chapter', 'Prime Care Alpha Covid-19 Testing Laboratory', 'University of Cebu Medical Center', 'Vicente Sotto Memorial Medical Center (VSMMC)']
    df = pd.read_csv(csv, usecols=fields, low_memory=False)
    df = df.fillna(value='0')
    df = df.loc[df['facility_name'].isin(tests_list)]
    #df = df.loc[df['report_date'] == '11/3/21']
    df.to_sql(tablename, conn, if_exists = 'append', index = False)
   

def create_bedsdb(csv, db, tablename):
    conn = sqlite3.connect(db)
    fields = ['cfname', 'reportdate', 'icu_o', 'icu_v','beds_ward_o','beds_ward_v','isolbed_o','isolbed_v','conf_asym','conf_mild','conf_severe','conf_crit','province']
    df = pd.read_csv(csv, usecols=fields, low_memory=False)
    df = df.fillna(value = '0')
    df = df.loc[df['province'] == 'CEBU']
    #df.to_csv('bedstest.csv')
    df.to_sql(tablename, conn, if_exists = 'append', index = False)
    
def create_dbs():
    dir_name = '/Users/bernadettechia/Downloads/covidtracker/flask_herok/'
    for item in os.listdir(dir_name):
        if item.endswith('.db'):
            os.remove(os.path.join(dir_name, item))
            print("Deleted yesterday's db file!")
    
    csv_path = '/Users/bernadettechia/Downloads/covidtracker/raw_data/'
    csv_list = sorted([csv for csv in listdir(csv_path) if csv.endswith('.csv')])
    # YYYY-MM-DD_filename.csv
    csv_date = csv_list[0][:11] 
    db_list = [[csv_date+'cases.db','Case Information.csv','Cases'], [csv_date+'tests.db', 'Testing Aggregates.csv','Tests'], [csv_date+'beds.db','DOH Data Collect - Daily Report.csv','Beds']]
    for db, name, table_name in db_list:
        csv = csv_path + csv_date + name
        #create_connection(db)
        if table_name == 'Cases':
            create_casesdb(csv,db,table_name)
        if table_name == 'Tests':
            create_testsdb(csv,db,table_name)
        if table_name == 'Beds':
            create_bedsdb(csv,db,table_name)

if __name__ == '__main__':
    create_dbs()

  