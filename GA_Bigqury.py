"""Google Analytics Reporting API V4."""
# !/usr/bin/python3.4
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from google.cloud import bigquery
import sys
import os
from pprint import pprint
import pprint
import traceback
import datetime
import json
import csv
import pandas as pd
from pandas.io.json import json_normalize

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
VIEW_ID_LIST = ['xxxxx', xxxxxxx']

STG_METRICS = "GA_Metrics_Staging"
STG_GOAL_SET1 = "GA_Goalset1_Staging"
STG_GOAL_SET2 = "GA_Goalset2_Staging"
STG_GOAL_SET3 = "GA_Goalset3_Staging"
STG_GOAL_SET4 = "GA_Goalset4_Staging"
STG_GOAL_SET5 = "GA_Goalset5_Staging"
STG_GOAL_SET6 = "GA_Goalset6_Staging"
STG_GOAL_SET7 = "GA_Goalset7_Staging"
STG_GOAL_SET8 = "GA_Goalset8_Staging"
STG_GOAL_SET9 = "GA_Goalset9_Staging"
STG_GOAL_SET10 = "GA_Goalset10_Staging"

GA_OUT_FILE = "/mnt/data/Client/GA/Report/ga_result.csv"
GA_OUT_FILE_G1 = '/mnt/data/Client/GA/Report/ga_out_file_g1.csv'
GA_OUT_FILE_G2 = '/mnt/data/Client/GA/Report/ga_out_file_g2.csv'
GA_OUT_FILE_G3 = '/mnt/data/Client/GA/Report/ga_out_file_g3.csv'
GA_OUT_FILE_G4 = '/mnt/data/Client/GA/Report/ga_out_file_g4.csv'
GA_OUT_FILE_G5 = '/mnt/data/Client/GA/Report/ga_out_file_g5.csv'
GA_OUT_FILE_G6 = '/mnt/data/Client/GA/Report/ga_out_file_g6.csv'
GA_OUT_FILE_G7 = '/mnt/data/Client/GA/Report/ga_out_file_g7.csv'
GA_OUT_FILE_G8 = '/mnt/data/Client/GA/Report/ga_out_file_g8.csv'
GA_OUT_FILE_G9 = '/mnt/data/Client/GA/Report/ga_out_file_g9.csv'
GA_OUT_FILE_G10 = '/mnt/data/Client/GA/Report/ga_out_file_g10.csv'

schema_metrics = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('sessions', 'INTEGER'),
    bigquery.SchemaField('users', 'INTEGER'),
    bigquery.SchemaField('newUsers', 'INTEGER'),
    bigquery.SchemaField('bounces', 'INTEGER'),
    bigquery.SchemaField('pageviews', 'INTEGER'),
    bigquery.SchemaField('sessionDuration', 'FLOAT'),
    bigquery.SchemaField('goalStartsAll', 'INTEGER'),
    bigquery.SchemaField('goalCompletionsAll', 'INTEGER'),
    bigquery.SchemaField('goalValueAll', 'FLOAT'),
    bigquery.SchemaField('goalAbandonsAll', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga1 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal1Starts', 'INTEGER'),
    bigquery.SchemaField('goal1Completions', 'INTEGER'),
    bigquery.SchemaField('goal1Value', 'FLOAT'),
    bigquery.SchemaField('goal1Abandons', 'INTEGER'),
    bigquery.SchemaField('goal2Starts', 'INTEGER'),
    bigquery.SchemaField('goal2Completions', 'INTEGER'),
    bigquery.SchemaField('goal2Value', 'FLOAT'),
    bigquery.SchemaField('goal2Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga2 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal3Starts', 'INTEGER'),
    bigquery.SchemaField('goal3Completions', 'INTEGER'),
    bigquery.SchemaField('goal3Value', 'FLOAT'),
    bigquery.SchemaField('goal3Abandons', 'INTEGER'),
    bigquery.SchemaField('goal4Starts', 'INTEGER'),
    bigquery.SchemaField('goal4Completions', 'INTEGER'),
    bigquery.SchemaField('goal4Value', 'FLOAT'),
    bigquery.SchemaField('goal4Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga3 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal5Starts', 'INTEGER'),
    bigquery.SchemaField('goal5Completions', 'INTEGER'),
    bigquery.SchemaField('goal5Value', 'FLOAT'),
    bigquery.SchemaField('goal5Abandons', 'INTEGER'),
    bigquery.SchemaField('goal6Starts', 'INTEGER'),
    bigquery.SchemaField('goal6Completions', 'INTEGER'),
    bigquery.SchemaField('goal6Value', 'FLOAT'),
    bigquery.SchemaField('goal6Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga4 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal7Starts', 'INTEGER'),
    bigquery.SchemaField('goal7Completions', 'INTEGER'),
    bigquery.SchemaField('goal7Value', 'FLOAT'),
    bigquery.SchemaField('goal7Abandons', 'INTEGER'),
    bigquery.SchemaField('goal8Starts', 'INTEGER'),
    bigquery.SchemaField('goal8Completions', 'INTEGER'),
    bigquery.SchemaField('goal8Value', 'FLOAT'),
    bigquery.SchemaField('goal8Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga5 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal9Starts', 'INTEGER'),
    bigquery.SchemaField('goal9Completions', 'INTEGER'),
    bigquery.SchemaField('goal9Value', 'FLOAT'),
    bigquery.SchemaField('goal9Abandons', 'INTEGER'),
    bigquery.SchemaField('goal10Starts', 'INTEGER'),
    bigquery.SchemaField('goal10Completions', 'INTEGER'),
    bigquery.SchemaField('goal10Value', 'FLOAT'),
    bigquery.SchemaField('goal10Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga6 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal11Starts', 'INTEGER'),
    bigquery.SchemaField('goal11Completions', 'INTEGER'),
    bigquery.SchemaField('goal11Value', 'FLOAT'),
    bigquery.SchemaField('goal11Abandons', 'INTEGER'),
    bigquery.SchemaField('goal12Starts', 'INTEGER'),
    bigquery.SchemaField('goal12Completions', 'INTEGER'),
    bigquery.SchemaField('goal12Value', 'FLOAT'),
    bigquery.SchemaField('goal12Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga7 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal13Starts', 'INTEGER'),
    bigquery.SchemaField('goal13Completions', 'INTEGER'),
    bigquery.SchemaField('goal13Value', 'FLOAT'),
    bigquery.SchemaField('goal13Abandons', 'INTEGER'),
    bigquery.SchemaField('goal14Starts', 'INTEGER'),
    bigquery.SchemaField('goal14Completions', 'INTEGER'),
    bigquery.SchemaField('goal14Value', 'FLOAT'),
    bigquery.SchemaField('goal14Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga8 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal15Starts', 'INTEGER'),
    bigquery.SchemaField('goal15Completions', 'INTEGER'),
    bigquery.SchemaField('goal15Value', 'FLOAT'),
    bigquery.SchemaField('goal15Abandons', 'INTEGER'),
    bigquery.SchemaField('goal16Starts', 'INTEGER'),
    bigquery.SchemaField('goal16Completions', 'INTEGER'),
    bigquery.SchemaField('goal16Value', 'FLOAT'),
    bigquery.SchemaField('goal16Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga9 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal17Starts', 'INTEGER'),
    bigquery.SchemaField('goal17Completions', 'INTEGER'),
    bigquery.SchemaField('goal17Value', 'FLOAT'),
    bigquery.SchemaField('goal17Abandons', 'INTEGER'),
    bigquery.SchemaField('goal18Starts', 'INTEGER'),
    bigquery.SchemaField('goal18Completions', 'INTEGER'),
    bigquery.SchemaField('goal18Value', 'FLOAT'),
    bigquery.SchemaField('goal18Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

schema_ga10 = [
    bigquery.SchemaField('date', 'INTEGER'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('sourceMedium', 'STRING'),
    bigquery.SchemaField('adContent', 'STRING'),
    bigquery.SchemaField('landingPagePath', 'STRING'),
    bigquery.SchemaField('keyword', 'STRING'),
    bigquery.SchemaField('campaignCode', 'STRING'),
    bigquery.SchemaField('goal19Starts', 'INTEGER'),
    bigquery.SchemaField('goal19Completions', 'INTEGER'),
    bigquery.SchemaField('goal19Value', 'FLOAT'),
    bigquery.SchemaField('goal19Abandons', 'INTEGER'),
    bigquery.SchemaField('goal20Starts', 'INTEGER'),
    bigquery.SchemaField('goal20Completions', 'INTEGER'),
    bigquery.SchemaField('goal20Value', 'FLOAT'),
    bigquery.SchemaField('goal20Abandons', 'INTEGER'),
    bigquery.SchemaField('viewID', 'STRING')
]

config_file = 'ga_config.json'
with open(config_file) as file:
    data = json.load(file)

schemas = data['bigquery_schema']
DATASET_ID = data['bigquery_details']['dataset']
STG_TABLE_ID = data['bigquery_details']['table_stg']
MASTER_TABLE_ID = data['bigquery_details']['table_master']
KEY_FILE_LOCATION = data['bigquery_details']['credentials']


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    print '>>> Building the client object using Credentials File '
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    print '>>> Returning the object to request the GA report'
    return analytics


def get_report(analytics, view):
    """Queries the Analytics Reporting API V4.

    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    print '>>> Getting the GA report'
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view,
                    'pageSize': 100000,
                    'dateRanges': [{'startDate': startDATE, 'endDate': endDATE}],
                    'metrics': [{'expression': 'ga:sessions'},
                                {'expression': 'ga:users'},
                                {'expression': 'ga:newUsers'},
                                {'expression': 'ga:bounces'},
                                {'expression': 'ga:pageviews'},
                                {'expression': 'ga:sessionDuration'},
                                {'expression': 'ga:goalStartsAll'},
                                {'expression': 'ga:goalCompletionsAll'},
                                {'expression': 'ga:goalValueAll'},
                                {'expression': 'ga:goalAbandonsAll'},
                                ],
                    'dimensions': [{'name': 'ga:date'},
                                   {'name': 'ga:campaign'},
                                   {'name': 'ga:sourceMedium'},
                                   {'name': 'ga:adContent'},
                                   {'name': 'ga:landingPagePath'},
                                   {'name': 'ga:keyword'},
                                   {'name': 'ga:campaignCode'}]
                }]
        }
    ).execute()


def get_goal_report(i, analytics, view):
    # print "i"+str(i)
    """Queries the Analytics Reporting API V4.

    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    print '>>> Getting the GA Goals: Goal Set Table: ' + str(i)

    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view,
                    'pageSize': 100000,
                    'dateRanges': [{'startDate': startDATE, 'endDate': endDATE}],
                    'metrics': [{'expression': 'ga:goal' + str(i) + 'Starts'},
                                {'expression': 'ga:goal' + str(i) + 'Completions'},
                                {'expression': 'ga:goal' + str(i) + 'Value'},
                                {'expression': 'ga:goal' + str(i) + 'Abandons'},
                                {'expression': 'ga:goal' + str(i + 1) + 'Starts'},
                                {'expression': 'ga:goal' + str(i + 1) + 'Completions'},
                                {'expression': 'ga:goal' + str(i + 1) + 'Value'},
                                {'expression': 'ga:goal' + str(i + 1) + 'Abandons'}
                                ],
                    'dimensions': [{'name': 'ga:date'},
                                   {'name': 'ga:campaign'},
                                   {'name': 'ga:sourceMedium'},
                                   {'name': 'ga:adContent'},
                                   {'name': 'ga:landingPagePath'},
                                   {'name': 'ga:keyword'},
                                   {'name': 'ga:campaignCode'}]
                }]
        }
    ).execute()


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
      response: An Analytics Reporting API V4 response.
    """

    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                str = header + ': ' + dimension
                print(str)

            for i, values in enumerate(dateRangeValues):
                print('Date range: ' + str(i))
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    print(metricHeader.get('name') + ': ' + value)


def parse_data(response, callcount, view):
    print '>>> parse data for view:' + str(view)
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')

    reports = response['reports'][0]
    columnHeader = reports['columnHeader']['dimensions']
    metricHeader = reports['columnHeader']['metricHeader']['metricHeaderEntries']

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric['name'])

    try:
        data = json_normalize(reports['data']['rows'])
        data_dimensions = pd.DataFrame(data['dimensions'].tolist())
        data_metrics = pd.DataFrame(data['metrics'].tolist())
        data_metrics = data_metrics.applymap(lambda x: x['values'])
        data_metrics = pd.DataFrame(data_metrics[0].tolist())
        result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)
        result.loc[-1] = columns
        result.index = result.index + 1
        result = result.sort_index()
        result["viewID"] = view
        data.rename(columns={"viewID": 'viewID'}, inplace=True)
    except:
        result = None
    return result


def connectToBigQuery():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_FILE_LOCATION)
        project_id = 'Client-data-warehouse'
        client = bigquery.Client(credentials=credentials, project=project_id)
        print('>>> Connected to Bigquery')
    except Exception as e:
        print('>>> Error while establishing connection to BigQuery! \n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)
    return client


def insert_into_bigquery(client, dataset_id, stg_table_id, master_table_id):
    try:
        print '>>> inserting data from {} to {} table'.format(stg_table_id, master_table_id)
        dataset_ref = client.dataset(dataset_id)
        stg_table_ref = dataset_ref.table(stg_table_id)
        master_table_ref = dataset_ref.table(master_table_id)

        master_rows_number = client.get_table(master_table_ref).num_rows
        print(">>> Number of rows in master table before delete and insert operation = {}".format(master_rows_number))
        print(">>> Deleting the duplicate rows from master table that are also present in staging table.....")
        delete_duplicate_query = """DELETE FROM {}.{} 
           	                  WHERE EXISTS (
                	          SELECT * from {}.{} WHERE 
                                  {}.date = {}.date AND
                                  {}.ViewID = {}.viewID )""".format(dataset_id, master_table_id,
                                                                    dataset_id, stg_table_id,
                                                                    master_table_id, stg_table_id,
                                                                    master_table_id, stg_table_id)
        print(">>> " + delete_duplicate_query)
        delete_query_job = client.query(delete_duplicate_query)
        delete_query_job.result()
        retained_row = 0
        for row in delete_query_job.result():
            retained_row += 1
        print(">>> Number of deleted rows = {}".format(master_rows_number - retained_row))

        """Insert the data in staging table to master table"""
        print(">>> Inserting data from staging table to master......")
        stg_master_copy_job_config = bigquery.CopyJobConfig()
        stg_master_copy_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        stg_master_copy_job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        schema_config = []

        for schema in schemas:
            schema_config.append(eval(schema))
            stg_master_copy_job_config.schema = schema_config

        stg_master_copy_job = client.copy_table(
            stg_table_ref,
            master_table_ref,
            location='US',
            job_config=stg_master_copy_job_config
        )
        stg_master_copy_job.result()
        print(">>> Number of rows inserted = {}".format(client.get_table(stg_table_ref).num_rows))
        print(">>> Total number of rows in master table after delete and insert operation = {}".format(
            client.get_table(master_table_ref).num_rows))
    except Exception as e:
        pprint('>>> Error occured while inserting in the Master BigQuery Table \n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def merge_metrics_stg_to_Staging(client, dataset, staging_table, main_staging_table):
    try:
        insert_query = """INSERT INTO  {}.{} 
                          (date,campaign,sourceMedium,adContent,
                           landingPagePath,keyword,campaignCode,sessions,users,newUsers,
                           bounces,pageviews,sessionDuration,goalStartsAll,goalCompletionsAll,
                           goalValueAll,goalAbandonsAll,viewID)
                           select * from  {}.{}""".format(dataset, main_staging_table, dataset, staging_table)
        print(">>> " + insert_query)
        insert_query_job = client.query(insert_query)
        insert_query_job.result()
        total_insert_rows = 0
        for row in insert_query_job.result():
            total_insert_rows += 1
        print(">>> Number of inserted rows = {}".format(total_insert_rows))

    except Exception as e:
        pprint('>>> Error occured inserting into staging table\n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def merge_goals_to_Staging(client, dataset, staging_table, main_staging_table, fieldlist):
    try:
        insert_query = """UPDATE {}.{} AS S1 SET {} 
        FROM {}.{} 
        AS S2 WHERE S1.date = S2.date AND 
        S1.campaign = S2.campaign AND 
        S1.sourceMedium = S2.sourceMedium AND  
        S1.adContent = S2.adContent AND 
        S1.landingPagePath = S2.landingPagePath AND 
        S1.keyword = S2.keyword AND 
        S1.campaignCode = S2.campaignCode AND 
        S1.viewID = s2.viewID
        """.format(dataset, main_staging_table, fieldlist,
                   dataset, staging_table)
        print(">>> " + insert_query)
        insert_query_job = client.query(insert_query)
        insert_query_job.result()
        total_insert_rows = 0
        for row in insert_query_job.result():
            total_insert_rows += 1
        print(">>> Number of inserted rows = {}".format(total_insert_rows))

    except Exception as e:
        pprint('>>> Error occured while merging goals to main staging Table \n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def insert_into_bigquery_staging(client, file_in_ec2, staging_table, schemas):
    try:
        print(">>> Loading Data into the '{}:{}' in BigQuery........".format(DATASET_ID, staging_table))
        dataset_ref = client.dataset(DATASET_ID)
        stg_table_ref = dataset_ref.table(staging_table)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job_config.schema = schemas  # **************************
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1

        with open(file_in_ec2, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file,
                stg_table_ref,
                location='US',
                job_config=job_config
            )
        job.result()
        print('>>> Loaded {} rows into {}:{}.'.format(job.output_rows, DATASET_ID, staging_table))
    except Exception as e:
        pprint('>>> Error occured while copying the contents of a CSV file to the BigQuery Table \n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def getDates(client):
    startDATE = os.getenv('startDATE', "")
    endDATE = os.getenv('endDATE', "")

    if startDATE == "":
        print '>>> Calculating StartDATE '
        date_query = " select max(date) as date from " + DATASET_ID + "." + MASTER_TABLE_ID
        print(">>> " + date_query)
        date_query_job = client.query(date_query)
        try:
            results = date_query_job.result()
            print '>>> Executed the max Date query '
            print '>>> Fetching the date row,if any '
            for row in results:
                if row.date == None:
                    startDATE = ""
                else:
                    startDATE = str(row.date)
                    startDATE = datetime.datetime.strptime(startDATE, '%Y%m%d').strftime("%Y-%m-%d")
            if startDATE == "":
                print '>>> No max Date row '
                yesterDate = (datetime.datetime.now() - datetime.timedelta(2)).strftime('%Y-%m-%d')
                startDATE = yesterDate
            else:
                print '>>> setting the startDATE to next day of last updated data'
                tempDATE = str(datetime.datetime.strptime(startDATE, "%Y-%m-%d") - datetime.timedelta(2))
                print tempDATE
                startDATE = str(datetime.datetime.strptime(tempDATE, '%Y-%m-%d 00:00:00').strftime("%Y-%m-%d"))
                print '>>> startDate is: ' + startDATE
        except Exception as e:
            pprint.pprint(">>> Failed to get Max Date from the Master Table %s " % e)
            type_, value_, traceback_ = sys.exc_info()
            pprint(traceback.format_tb(traceback_))
            print(type_, value_)
            sys.exit(1)
    else:
        print '>>> Settiing startDATE from Jenkins parm'
        startDATE = os.environ['startDATE']

    if endDATE == "":
        print '>>> Going to calculate endDate'
        yesterDate = (datetime.datetime.now() - datetime.timedelta(2)).strftime('%Y-%m-%d')
        endDATE = yesterDate
    else:
        print '>>> Setting the endDATE from Jenkins parm'
        endDATE = os.environ['endDATE']

    startDATE = str(startDATE)
    endDATE = str(endDATE)

    if startDATE > endDATE:
        print '>>> startDATE greater than endDATE ?? I am setting both as endDATE'
        startDATE = endDATE

    print '>>> startDate : ' + startDATE
    print '>>> endDate: ' + endDATE

    return [startDATE, endDATE]


def truncate_main_staging(client, dataset, main_staging_table):
    try:
        print '>>> Trucating the staging table: ' + main_staging_table
        truncate_query = """DELETE FROM {}.{} WHERE 1=1""".format(dataset, main_staging_table)
        delete_query_job = client.query(truncate_query)
        delete_query_job.result()
        print '>>> Truncated Table: ' + main_staging_table
    except Exception as e:
        pprint('>>> Error occured while truncating the main staging table\n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def main():
    global startDATE
    global endDATE

    try:
        ViewID = os.getenv('ViewID', "")

        if ViewID == "":
            ViewID_list = VIEW_ID_LIST
        else:
            ViewID_list = [ViewID]

        analytics = initialize_analyticsreporting()
        client = connectToBigQuery()

        startDATE, endDATE = getDates(client)

        for view in ViewID_list:

            response = get_report(analytics, view)

            finalresult_df = parse_data(response, 1, view)

            truncate_main_staging(client, DATASET_ID, STG_TABLE_ID)

            if finalresult_df is not None:
                finalresult_df.to_csv(GA_OUT_FILE, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE, STG_METRICS, schema_metrics)
                merge_metrics_stg_to_Staging(client, DATASET_ID, STG_METRICS, STG_TABLE_ID)

            response_1 = get_goal_report(1, analytics, view)
            finalresult_df_1 = parse_data(response_1, 1, view)
            if finalresult_df_1 is not None:
                finalresult_df_1.to_csv(GA_OUT_FILE_G1, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G1, STG_GOAL_SET1, schema_ga1)
                fieldlist = 'S1.goal1Starts = S2.goal1Starts,S1.goal1Completions = S2.goal1Completions,S1.goal1Value = S2.goal1Value,S1.goal1Abandons = S2.goal1Abandons,S1.goal2Starts = S2.goal2Starts,S1.goal2Completions = S2.goal2Completions,S1.goal2Value = S2.goal2Value,S1.goal2Abandons = S2.goal2Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET1, STG_TABLE_ID, fieldlist)

            response_2 = get_goal_report(3, analytics, view)
            finalresult_df_2 = parse_data(response_2, 2, view)
            if finalresult_df_2 is not None:
                finalresult_df_2.to_csv(GA_OUT_FILE_G2, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G2, STG_GOAL_SET2, schema_ga2)
                fieldlist = 'S1.goal3Starts = S2.goal3Starts,S1.goal3Completions = S2.goal3Completions,S1.goal3Value = S2.goal3Value,S1.goal3Abandons = S2.goal3Abandons,S1.goal4Starts = S2.goal4Starts,S1.goal4Completions = S2.goal4Completions,S1.goal4Value = S2.goal4Value,S1.goal4Abandons = S2.goal4Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET2, STG_TABLE_ID, fieldlist)

            response_3 = get_goal_report(5, analytics, view)
            finalresult_df_3 = parse_data(response_3, 3, view)
            if finalresult_df_3 is not None:
                finalresult_df_3.to_csv(GA_OUT_FILE_G3, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G3, STG_GOAL_SET3, schema_ga3)
                fieldlist = 'S1.goal5Starts = S2.goal5Starts,S1.goal5Completions = S2.goal5Completions,S1.goal5Value = S2.goal5Value,S1.goal5Abandons = S2.goal5Abandons,S1.goal6Starts = S2.goal6Starts,S1.goal6Completions = S2.goal6Completions,S1.goal6Value = S2.goal6Value,S1.goal6Abandons = S2.goal6Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET3, STG_TABLE_ID, fieldlist)

            response_4 = get_goal_report(7, analytics, view)
            finalresult_df_4 = parse_data(response_4, 4, view)
            if finalresult_df_4 is not None:
                finalresult_df_4.to_csv(GA_OUT_FILE_G4, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G4, STG_GOAL_SET4, schema_ga4)
                fieldlist = 'S1.goal7Starts = S2.goal7Starts,S1.goal7Completions = S2.goal7Completions,S1.goal7Value = S2.goal7Value,S1.goal7Abandons = S2.goal7Abandons,S1.goal8Starts = S2.goal8Starts,S1.goal8Completions = S2.goal8Completions,S1.goal8Value = S2.goal8Value,S1.goal8Abandons = S2.goal8Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET4, STG_TABLE_ID, fieldlist)

            response_5 = get_goal_report(9, analytics, view)
            finalresult_df_5 = parse_data(response_5, 5, view)
            if finalresult_df_5 is not None:
                finalresult_df_5.to_csv(GA_OUT_FILE_G5, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G5, STG_GOAL_SET5, schema_ga5)
                fieldlist = 'S1.goal9Starts = S2.goal9Starts,S1.goal9Completions = S2.goal9Completions,S1.goal9Value = S2.goal9Value,S1.goal9Abandons = S2.goal9Abandons,S1.goal10Starts = S2.goal10Starts,S1.goal10Completions = S2.goal10Completions,S1.goal10Value = S2.goal10Value,S1.goal10Abandons = S2.goal10Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET5, STG_TABLE_ID, fieldlist)

            response_6 = get_goal_report(11, analytics, view)
            finalresult_df_6 = parse_data(response_6, 6, view)
            if finalresult_df_6 is not None:
                finalresult_df_6.to_csv(GA_OUT_FILE_G6, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G6, STG_GOAL_SET6, schema_ga6)
                fieldlist = 'S1.goal11Starts = S2.goal11Starts,S1.goal11Completions = S2.goal11Completions,S1.goal11Value = S2.goal11Value,S1.goal11Abandons = S2.goal11Abandons,S1.goal12Starts = S2.goal12Starts,S1.goal12Completions = S2.goal12Completions,S1.goal12Value = S2.goal12Value,S1.goal12Abandons = S2.goal12Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET6, STG_TABLE_ID, fieldlist)

            response_7 = get_goal_report(13, analytics, view)
            finalresult_df_7 = parse_data(response_7, 7, view)
            if finalresult_df_7 is not None:
                finalresult_df_7.to_csv(GA_OUT_FILE_G7, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G7, STG_GOAL_SET7, schema_ga7)
                fieldlist = 'S1.goal13Starts = S2.goal13Starts,S1.goal13Completions = S2.goal13Completions,S1.goal13Value = S2.goal13Value,S1.goal13Abandons = S2.goal13Abandons,S1.goal14Starts = S2.goal14Starts,S1.goal14Completions = S2.goal14Completions,S1.goal14Value = S2.goal14Value,S1.goal14Abandons = S2.goal14Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET7, STG_TABLE_ID, fieldlist)

            response_8 = get_goal_report(15, analytics, view)
            finalresult_df_8 = parse_data(response_8, 8, view)
            if finalresult_df_8 is not None:
                finalresult_df_8.to_csv(GA_OUT_FILE_G8, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G8, STG_GOAL_SET8, schema_ga8)
                fieldlist = 'S1.goal15Starts = S2.goal15Starts,S1.goal15Completions = S2.goal15Completions,S1.goal15Value = S2.goal15Value,S1.goal15Abandons = S2.goal15Abandons,S1.goal16Starts = S2.goal16Starts,S1.goal16Completions = S2.goal16Completions,S1.goal16Value = S2.goal16Value,S1.goal16Abandons = S2.goal16Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET8, STG_TABLE_ID, fieldlist)

            response_9 = get_goal_report(17, analytics, view)
            finalresult_df_9 = parse_data(response_9, 9, view)
            if finalresult_df_9 is not None:
                finalresult_df_9.to_csv(GA_OUT_FILE_G9, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G9, STG_GOAL_SET9, schema_ga9)
                fieldlist = 'S1.goal17Starts = S2.goal17Starts,S1.goal17Completions = S2.goal17Completions,S1.goal17Value = S2.goal17Value,S1.goal17Abandons = S2.goal17Abandons,S1.goal18Starts = S2.goal18Starts,S1.goal18Completions = S2.goal18Completions,S1.goal18Value = S2.goal18Value,S1.goal18Abandons = S2.goal18Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET9, STG_TABLE_ID, fieldlist)

            response_10 = get_goal_report(19, analytics, view)
            finalresult_df_10 = parse_data(response_10, 10, view)
            if finalresult_df_10 is not None:
                finalresult_df_10.to_csv(GA_OUT_FILE_G10, index=False, header=False)
                insert_into_bigquery_staging(client, GA_OUT_FILE_G10, STG_GOAL_SET10, schema_ga10)
                fieldlist = 'S1.goal19Starts = S2.goal19Starts,S1.goal19Completions = S2.goal19Completions,S1.goal19Value = S2.goal19Value,S1.goal19Abandons = S2.goal19Abandons,S1.goal20Starts = S2.goal20Starts,S1.goal20Completions = S2.goal20Completions,S1.goal20Value = S2.goal20Value,S1.goal20Abandons = S2.goal20Abandons'
                merge_goals_to_Staging(client, DATASET_ID, STG_GOAL_SET10, STG_TABLE_ID, fieldlist)

            insert_into_bigquery(client, DATASET_ID, STG_TABLE_ID, MASTER_TABLE_ID)

    except Exception as e:
        pprint.pprint(" >> Error in the script : %s" % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


if __name__ == '__main__':
    main()