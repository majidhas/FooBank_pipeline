
import apache_beam as beam
from google.cloud import bigquery
from datetime import date
import time
import re
import argparse

#getting current day to use as files' suffix
today_date = str(date.today())

#importing all loan files using (*) wildcard
loans_source_url = "gs://rocker-assignment/Files/loan-*.csv"                                        ## ------to be defined by the user------##

visits_url = "gs://rocker-assignment/Files/visits.csv"                                              ## ------to be defined by the user------##

customers_url = "gs://rocker-assignment/Files/customers.json"                                       ## ------to be defined by the user------##





# ---------------Constructing a apache beam pipeline with Dataflow runner to import and clean all loan csv files---------------------------

# Command line arguments
parser = argparse.ArgumentParser(description='Demonstrate side inputs')
parser.add_argument('--bucket', required=True, help='Specify Cloud Storage bucket for output')     ## ------to be defined by the user in command line------##
parser.add_argument('--project',required=True, help='Specify Google Cloud project')                ## ------to be defined by the user in command line------##
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--DirectRunner',action='store_true')                                                        
group.add_argument('--DataFlowRunner',action='store_true')                                         ## ------to be defined by the user in command line------##

opts = parser.parse_args()

if opts.DirectRunner:
    runner='DirectRunner'
if opts.DataFlowRunner:
    runner='DataFlowRunner'

#setting project and bucket names from command line
bucket = opts.bucket
project = opts.project


#constructing pipeline 
cleaning_argv = [
    '--project={0}'.format(project),
    '--job_name=cleaningpipeline',
    '--save_main_session',
    '--staging_location=gs://{0}/foobank_output/staging/'.format(bucket),
    '--temp_location=gs://{0}/foobank_output/staging/'.format(bucket),
    '--runner={0}'.format(runner),
    '--region=us-central1',                                                                     ## ------to be defined by the user------##
    '--max_num_workers=5'
    ]

cleaning_pipeline = beam.Pipeline(argv=cleaning_argv)

#fn for solving the problem of webvisit_id empty field and csv file first rows
def stripper(text):
  if text.count(',') == 9:
    return text.rstrip(',')
  elif text[1:3] == 'id':
    return '0,0,0,0,0,0,0,0,0'
  else:
      return text

cleaned_loans_url = f'gs://{bucket}/foobank_output/medial_output_{today_date}.csv'


loans = (cleaning_pipeline  |  'LoansReadFromStorage' >> beam.io.ReadFromText(loans_source_url)
                            |  'RemoveParentheses' >> beam.Regex.replace_all('[()]', '')
                            |  'RemoveDoubleQuotation' >> beam.Regex.replace_all(r'"', '') | 'StripLastComma' >> beam.Map(stripper)
                            |  'WriteToStorage' >> beam.io.WriteToText(cleaned_loans_url[:-4] ,file_name_suffix='.csv',shard_name_template=''))

run_p= cleaning_pipeline.run().wait_until_finish()
print('Beam pipeline is ' + run_p)

#buffering for any latency
time.sleep(20)













# ----------------------------------------------------------------------------
# ------------------Constructing BigQuery jobs for making tables-----------------
# ----------------------------------------------------------------------------

client = bigquery.Client()

# ---------BigQuery job for making loans table-----------

loans_table_id = f"rocker-assignment.foobank_pipeline.loans_{today_date}"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("unknown_field", 'INTEGER'),
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("timestamp", "INTEGER"),
        bigquery.SchemaField("loan_amount", "INTEGER"),
        bigquery.SchemaField("loan_purpose", "STRING"),
        bigquery.SchemaField("outcome", "STRING"),
        bigquery.SchemaField("interest", "FLOAT"),
        bigquery.SchemaField("webvisit_id", "INTEGER")
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

# Make an API request.
load_loans_job = client.load_table_from_uri(
    cleaned_loans_url, loans_table_id, job_config=job_config
)  

load_loans_job.result()  # Waits for the job to complete.

print('loans table is loaded')

#buffering for any latency
time.sleep(10)







# --------BigQuery job for making visits table-----------

visits_table_id = f"rocker-assignment.foobank_pipeline.visits_{today_date}"

job_config = bigquery.LoadJobConfig(
    autodetect = True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV)

# Make an API request.
load_visits_job = client.load_table_from_uri(
    visits_url, visits_table_id, job_config=job_config)  

load_visits_job.result()  # Waits for the job to complete.

print('visits table is loaded')

#buffering for any latency
time.sleep(10)






# -------BigQuery job for making customers table----------

customers_table_id = f"rocker-assignment.foobank_pipeline.customers_{today_date}"

job_config = bigquery.LoadJobConfig(
    autodetect = True,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

# Make an API request.
load_customers_job = client.load_table_from_uri(
    customers_url, customers_table_id, job_config=job_config)  

load_customers_job.result()  # Waits for the job to complete.

print('customers table is loaded')

#buffering for any latency
time.sleep(10)







# ---------- Quering and saving the combined table using Bigquery API-------------

#making an acceptable date format for table name
the_date = re.sub('-','_',str(today_date))

query = f"""
    CREATE TABLE foobank_pipeline.combined_loan_visit_customer_{the_date}
        OPTIONS( description="Top ten words per Shakespeare corpus") 
    AS
    
        with main_query as (SELECT
                             l.id loan_id,
                             l.user_id,
                             l.timestamp loan_timestamp,
                             l.loan_amount,
                             l.loan_purpose,
                             l.outcome,
                             l.interest,
                             l.webvisit_id,
                             v.id visit_id,
                             v.timestamp vtimestamp,
                             c.zip_code,
                             c.name,
                             c.birthday,
                             c.city,
                             c.ssn,
                             c.gender,
                             c.id cid,
                             CONCAT(v.campaign_name, '_', v.referrer) AS campaign_referrer_con
                         FROM
                             `rocker-assignment.foobank_pipeline.loans_*` l
                             LEFT JOIN
                             `rocker-assignment.foobank_pipeline.visits_*` v
                         ON
                             l.webvisit_id = v.id
                         LEFT JOIN
                             `rocker-assignment.foobank_pipeline.customers_*` c
                         ON
                             l.user_id = c.id  )



         SELECT
             loan_id,
             user_id,
             loan_timestamp,
             loan_amount,
             loan_purpose,
             outcome,
             interest,
             visit_id,
             zip_code,
             name,
             birthday,
             city,
             ssn,
             gender,
             cid,
             STRING_AGG(campaign_referrer_con, " , ") AS campaign_referrer
         FROM
             main_query
         GROUP BY
             loan_id,
             user_id,
             loan_timestamp,
             loan_amount,
             loan_purpose,
             outcome,
             interest,
             visit_id,
             zip_code,
             name,
             birthday,
             city,
             ssn,
             gender,
             cid
    """
 # Make an API request.
query_job = client.query(query)
query_job.result() # Waits for the job to complete.

print('combined table is made')

#buffering for any latency
time.sleep(10)








#  ---------- Creaing Bigquery API request for exporting final CSV file-------------

final_csv_output_path = f'gs://{bucket}/foobank_output/combined_loan_visit_customer_data_{today_date}.csv'
dataset_id = "foobank_pipeline"                                                                  ## ------to be defined by the user------##
table_id = f"combined_loan_visit_customer_{the_date}" 

destination_uri = final_csv_output_path
dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table(table_id)

# API request
extract_job = client.extract_table(
    table_ref,
    destination_uri,
    location="US",
)  
extract_job.result()  # Waits for job to complete.

print('Finished')
