''' Pulls data from the YouVisit API for a given client; writes the results to  S3 bucket.


'''

import requests
import csv
import os
import boto3 as boto
import datetime
import logging 
import math
import time
import psycopg2
import psycopg2.extras
from datetime import date 

# Setup
in_aws = os.getenv('IN_AWS', True)
api_record_limit = int(os.getenv('API_RECORD_LIMIT', 500))
# headers = ["id", "experience_name", "experience_id", "firstname", "lastname", "email", "gender", "birthdate", "creation_time",
#                 "update_time", "street", "city", "state", "postal", "country", "phone", "graduation_year", "major", "school",
#                 "school_ceeb_code", "userkey", "source", "visitor_type", "enroll_year", "enroll_term", "is_cif", "full_registration"]

# The API is throttled to 100 requests/minute.
delay = int(os.getenv('API_DELAY_SECONDS', 5))

# Logging
logger = logging.getLogger('custom_log_stat')
logger.setLevel(logging.DEBUG)

sql_success = """Update panto.you_visit_process_log l 
                               set status_indicator = 'SUCCESS' ,
                               no_of_rows_in_file = %s    
                               where status_indicator = 'LAUNCHED: Message passed on to SQS Queue' 
                               and mbr_sk= %s  
                               and create_start_date = %s""" 

sql_partial = """Update panto.you_visit_process_log l 
                               set status_indicator = 'PARTIAL' ,
                               no_of_rows_in_file = %s,
                               create_start_date = %s,
                               error_decription = 'API hit successful but not all records pulled'
                               where status_indicator = 'LAUNCHED: Message passed on to SQS Queue' 
                               and mbr_sk= %s  
                               and create_start_date = %s""" 
                               
sql_zero_rows = """Update panto.you_visit_process_log l 
                               set status_indicator = 'COMPLETED WITH ERROR' ,
                               no_of_rows_in_file = 0 ,
                               error_decription = 'API hit successful but unable to retrieve any records'
                               where status_indicator = 'LAUNCHED: Message passed on to SQS Queue' 
                               and mbr_sk= %s
                               and create_start_date = %s"""    
                                     
sql_failed =   """Update panto.you_visit_process_log l 
                               set status_indicator = 'FAILED' ,
                               no_of_rows_in_file = 0 ,
                               error_decription = %s
                               where status_indicator = 'LAUNCHED: Message passed on to SQS Queue' 
                               and mbr_sk= %s
                               and create_start_date = %s""" 


sql_default_start_time =   """select count(*) from panto.you_visit_process_log   
                            where mbr_sk = %s and (status_indicator ='SUCCESS' or status_indicator = 'PARTIAL')"""          

sql_start_time = """SELECT  MAX(date(create_start_date))  
                    from panto.you_visit_process_log  where mbr_sk = %s and (status_indicator ='SUCCESS' or status_indicator = 'PARTIAL')"""                    

# CSV logic
def csv_export(output_file, output_buffer, in_aws, key_slug):
    if in_aws:
        os.chdir('/tmp')
    original_file_name = output_file
    full_file_path = '/tmp/' + output_file
    logger.info(f"INFO: FFN -> {full_file_path}")
    with open(output_file, mode = "w", encoding='utf-8') as output_file:
        output_writer = csv.writer(output_file, delimiter = "|",
            quotechar = '"', quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n')

        for row in output_buffer:
            output_writer.writerow(row)

    if in_aws == True:
        # Now we push it to S3!
        s3_connection = boto.resource('s3', region_name='us-east-1')
        bucket = s3_connection.Bucket(os.environ["PANTO_BUCKET_NAME"])
        key = f"{key_slug}/{original_file_name}"
        logger.info(f"INFO: full_file_path -> {full_file_path}")
        logger.info(f"INFO: key -> {key}")
        bucket.upload_file(full_file_path, key)

# Recon - get the count of records that are in scope, determine how many times we need to repeat this
def recon(url, auth_token, start_date, cur, con, member_sk, create_date_time):

    error_encountered = False
    most_recent = start_date
    headers = []

    times_to_run = 0

    url += f"?startDate={start_date}&limit={api_record_limit}"

    logger.info(f"INFO: Running recon on {url}")

    head = {'Authorization': 'Bearer ' + auth_token}

    try:
        response = requests.get(url, headers=head)
        logger.info(f"INFO: Response received.")
        logger.debug(f'DEBUG: recon - response: {response}')
    except Exception as em:
        cur.execute(sql_failed, (str(em), member_sk, create_date_time))
        con.commit()
        logger.exception("ERROR: " + str(em))
        error_encountered = True

    if response.status_code == 200:
        logger.info("INFO: Response in recon - Status 200 OK")

        try:
            return_data = response.json()
            # Only process further if records are returned
            if len(return_data['resources']) > 0:
                for record in return_data['resources']['data']:
                    this_dict = dict(record)
                    # If this row contains more keys than the previous rows we'll include this as the header row
                    # This will allow us to pick the file header dynamically instead of hardcoding
                    if len(this_dict.keys()) > len(headers):
                        headers = this_dict.keys()
                        logger.info(f"INFO: headers changed to: {headers}")

                total_records = int(return_data['resources']['meta']['total'])
                times_to_run = math.floor(total_records / api_record_limit)
                logger.info(f"INFO: recon - records that are in scope -> {total_records}")
                logger.info(f"INFO: recon - how many times to repeat -> {times_to_run}")
            else:
                error_encountered = True
                cur.execute(sql_zero_rows, (member_sk, create_date_time))
                con.commit()
                logger.info(f"INFO: Zero results returned; terminating.")
                logger.info(f"INFO: Payload => {return_data}")
                time.sleep(delay)
        except Exception as em:
            cur.execute(sql_failed, (str(em), member_sk, create_date_time))
            con.commit()
            logger.exception("ERROR: " + str(em))
            error_encountered = True
    else:
        graceful_death(response, most_recent)
        error_encountered = True

    return times_to_run, error_encountered, headers

# Pull the actual data that will be appended to the output buffer and written to Panto's S3
def pull_data(current_run, times_to_run, url, auth_token, start_date, output_buffer, offset, most_recent, headers):

    error_encountered = False

    offset = current_run * api_record_limit

    url += f"?startDate={start_date}&limit={api_record_limit}&offset={offset}"

    logger.info(f"INFO: Pulling data, batch {current_run}/{times_to_run} -> {url}")

    head = {'Authorization': 'Bearer ' + auth_token}

    try:
        response = requests.get(url, headers=head)
        logger.info(f"INFO: Response received.")
    except Exception as em:
        logger.exception("ERROR: Exception in pull_data - " + str(em))
        error_encountered = True

    if response.status_code == 200:
        logger.info("INFO: Response in pull_data - Status 200 OK")
    else:
        graceful_death(response, most_recent)
        error_encountered = True

    if error_encountered == False:
        return_data = response.json()

        try:
            if len(return_data['resources']) > 0:
                logger.info(f"INFO: Performing check for line feeds")
                for record in return_data['resources']['data']:
                    this_dict = dict(record)
                    for key, value in this_dict.items():
                        if value is not None:
                            if '\n' in value:
                                logger.info(f"INFO: \\n found in " + key + " value - replacing with spaces")
                                this_dict[key] = value.replace('\n', ' ')
                            if '\r' in value:
                                logger.info(f"INFO: \\r found in " + key + " value - replacing with spaces")
                                this_dict[key] = value.replace('\r', ' ')
                    this_row = []
                    try:
                        most_recent = this_dict['creation_time']
                    except:
                        most_recent = most_recent
                    for header in headers:
                        this_row.append(this_dict.get(header, ""))
                    output_buffer.append(this_row)
            else:
                logger.info(f"INFO: Zero results returned; terminating.")
                logger.info(f"INFO: Payload => {return_data}")
                error_encountered = True

        except TypeError as e:
            logger.error(f"ERROR: Exception in pull_data - " + str(e))
            logger.info(f"INFO: Payload => {return_data}")
            error_encountered = True

        time.sleep(delay)
    return error_encountered, most_recent
    
def connect_panto():

    try:
            con = psycopg2.connect(
                    boto.client('ssm').get_parameter(Name='/panto/{}/lambda/db/panto'.format(os.environ["ENVIRONMENT"]), WithDecryption=True)['Parameter']['Value']
                  )  
                  
            logger.info('Panto Connection Successful')
            return con, False
        
    except psycopg2.Error:
     
            logger.exception("Panto Connection Unsuccessful") 
            return None, True

# A single place to handle an unexpected http code
def graceful_death(response, most_recent):

    error_encountered = True
    logger.exception(f"ERROR: Error encountered. Termination imminent.")
    logger.exception(f"ERROR: Most recent record pulled: {most_recent}")
    logger.exception(f"ERROR: Unexpected status {response.status_code}: {response.content}")
    observation_timestamp = get_observation_timestamp()
    today = date.today()

    panto_conn, error_encountered = connect_panto()
    cur = panto_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 

    if len(output_buffer) > 1:
        key_slug = f"archive/{tenant_guid}/YouVisit/Inquiry/{today}"
        csv_export(f"youvisit-inquiry-{tenant_guid}-{observation_timestamp}.txt", output_buffer, in_aws, key_slug)
        logger.info(f"OUTPUT BUFFER LENGTH: {len(output_buffer)}")
        cur.execute(sql_partial,(len(output_buffer)-1, most_recent,member_sk, create_date_time))
        panto_conn.commit()
    else:
        logger.info(f"OUTPUT BUFFER LENGTH: {len(output_buffer)}")
        cur.execute(sql_failed, ("No records pulled", member_sk, create_date_time))
        panto_conn.commit()

# Observation timestamp format: YYYY-MM-dd_HH-mm-ss-SSS
def get_observation_timestamp():
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d_%H-%M-%S') + ('-%03d' % (now.microsecond / 10000))
    return ts

def main(event, context):

    error_encountered = False

    offset = 0
    current_run = 0
    panto_conn, error_encountered = connect_panto()
    cur = panto_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 
    
    today = date.today()
    # Event/Context Parsing
    # !- If no date, remove date parameter from URL
    # ! - Expect the following: PARTNER_ID|AUTH_TOKEN|URL|DATE_LAST_PULLED|PANTO_TENANT_GUID|MEMBER_SK

    # These values can be set for local execution
    global tenant_guid, member_sk, create_date_time
    if in_aws:
        payload = event['Records'][0]['body']
        payload = payload.replace("('", "").replace("',)", "")
        partner_id, auth_token, url, create_date_time, tenant_guid, member_sk = payload.split('|')
        member_sk = member_sk.replace("']","")
    else:
        partner_id = 00000
        auth_token = ""
        url = ""
        start_date = ""
        tenant_guid = ""
        member_sk = ""

    cur.execute(sql_default_start_time, (member_sk,))
    prev_success_list = cur.fetchone()
    if prev_success_list[0] == 0:
        start_date = "1900-01-01"
    else:
       cur.execute(sql_start_time,(member_sk,)) 
       start_time_list = cur.fetchone()
       start_date = start_time_list[0]
        
    most_recent = start_date
    headers = []
    global output_buffer
    output_buffer = []

    times_to_run, error_encountered, headers = \
        recon(url, auth_token, start_date, cur, panto_conn, member_sk, create_date_time)
    output_buffer.append(headers)

    while current_run <= times_to_run and error_encountered == False:
        error_encountered, most_recent = \
            pull_data(current_run, times_to_run, url, auth_token, start_date, output_buffer,
                      offset, most_recent, headers)
        current_run += 1

    if error_encountered == False:
        logger.info(f"INFO: Finished pulling all batches.")
        logger.info(f"INFO: Output buffer length: {len(output_buffer)}")
        
        observation_timestamp = get_observation_timestamp()

        if in_aws:
            key_slug = f"archive/{tenant_guid}/YouVisit/Inquiry/{today}"
            csv_export(f"youvisit-inquiry-{tenant_guid}-{observation_timestamp}.txt", output_buffer, in_aws, key_slug)
            logger.info(f"OUTPUT BUFFER LENGTH: {len(output_buffer)}")
            cur.execute(sql_success,(len(output_buffer)-1,member_sk,create_date_time))
            panto_conn.commit()           
            
        else:
            logger.info(f"OUTPUT BUFFER LENGTH: {len(output_buffer)}")
            key_slug = f"archive/{tenant_guid}/YouVisit/Inquiry/{today}"
            csv_export(f"youvisit-inquiry-{tenant_guid}-{observation_timestamp}.txt", output_buffer, in_aws, key_slug)
            cur.execute(sql_success,(len(output_buffer)-1,member_sk,create_date_time))
            panto_conn.commit()     
            
        cur.close()
        panto_conn.close()  
    else:
        logger.exception(f"ERROR: Script finished unexpectedly")
        logger.exception(f"ERROR: Output buffer length: {len(output_buffer)}")    

    return 0

if __name__ == "__main__":
    main("test", "")