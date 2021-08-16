"""

Read Json contacts for each Partner from specific parter-id folder in components S3 bucket
Append all contacts to destination file per partner in dest S3 bucket
Move Json contacts to outbox from inbox

Avi Patil / apatil@eab.com / 2021-02-21

Make change to use smart_open for writing files to s3
Avi Patil / apatil@eab.com / 2021-03-12

"""
import os
import boto3
import logging
import csv
import json
import datetime
from datetime import timezone, timedelta
import io
from smart_open import open, s3


logger = logging.getLogger('custom_log_stat')
logger.setLevel(logging.DEBUG)


def s3_objects_config():
    """Define S3 Objects"""
    s3_obj = boto3.client('s3')
    s3_connection = boto3.resource('s3', region_name='us-east-1')
    return s3_obj, s3_connection


def strip_file_path(source_list):
    """Strip File path in destination bucket
      i/p -> inbox/parter-id/acs*.json
      output -> /parter-id/acs*.json"""

    new_list = []

    for contact_string in source_list:
        new_string = contact_string.replace('inbox', '')
        new_list.append(new_string)
    return new_list


def move_file_s3(bucket_name, file_list, partner_id, s3_connection):
    """Move files to Outbox folder after writing to csv is complete"""

    generalized_file_path_list = strip_file_path(file_list.copy())  # Strip inbox from inbox/partner-444/xyz.json
    for file_path in generalized_file_path_list:
        src_file_path = 'inbox' + file_path
        # define new file path
        dest_file_path = 'outbox' + file_path
        copy_source = {
            'Bucket': bucket_name,
            'Key': src_file_path
        }
        try:
            # COPY file to outbox
            s3_connection.meta.client.copy(copy_source, bucket_name, dest_file_path)
        except Exception as e:
            raise Exception(f'There was an error copying  file  {src_file_path}' + str(e))

        try:
            # delete file from inbox
            s3_connection.Object(bucket_name, src_file_path).delete()
        except Exception as e:
            raise Exception(f'There was an error deleting  file  {src_file_path}' + str(e))


def read_json(bucket_name, key_name, s3):
    """Read Json contacts in a python dictionary"""

    try:
        result = s3.get_object(Bucket=bucket_name, Key=key_name)
        content = result['Body']
        json_dict = json.loads(content.read().decode('utf-8'))
        return json_dict

    except Exception as e:
        raise Exception(f'There was an error while reading the json file {key_name}' + str(e))


def calc_header_list(content_dict):
    """Parse header row from dictionary and return list"""

    header_list_fn = []
    for key, value in content_dict.items():
        header_list_fn.append(key)
    return header_list_fn


def calc_body_row(content_dict):
    """ Parse body from dictionary and return list"""

    body_list_fn = []
    for key, value in content_dict.items():
        body_list_fn.append(value)
    return body_list_fn


def create_output_buffer(b_list, output_list, h_list=None):
    """ Create Output Buffer with header and body list"""

    if h_list is None:
        output_list.append(b_list)
    else:
        output_list.append(h_list)
        output_list.append(b_list)
    return output_list


def write_to_csv(output_b, bucket_name, file_name, key_path):
    """Write to csv from output buffer to corresponding s3 bucket"""

    try:
        """
        Use stream IO and stream open to transfer buffer to file 
        """
        f = io.StringIO()
        original_file_name = file_name
        key = f"{key_path}/{original_file_name}"

        # 's3://commoncrawl/robots.txt' is syntax to be passed to open
        path_to_open_file = 's3://' + bucket_name + '/' + key
        logger.info(f'file path is{path_to_open_file}')

        with open(path_to_open_file, mode='w', encoding='utf-8') as file_out:
            output_writer = csv.writer(f, delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

            for row in output_b:
                f.seek(0)
                f.truncate(0)
                output_writer.writerow(row)
                file_out.write(f.getvalue())

        f.close()
        logger.info(f'File {key} successfully uploaded')

    except Exception as e:
        raise Exception(f'There was an error while writing to the file  {file_name}' + str(e))


def get_matching_s3_keys(s3_obj, bucket, prefix, suffix):
    """
    Generate the keys in an S3 bucket.
    :param suffix: Only fetch keys that end with this suffix
    :param s3_obj: S3 object
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix.

    Returns all contact files in s3 bucket
    """
    try:
        paginator = s3_obj.get_paginator("list_objects_v2")
        operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            try:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith(suffix):
                        yield key

            except KeyError:
                break

    except Exception as e:
        raise Exception('There was an error while trying to identify json contact file' + str(e))
        

def calc_list_partner_id(path):
    """Parse partner ids from the json contact files"""

    list_path = path.split('/')  # path = inbox/partner-1234
    return list_path[1]  # partner-1234


def get_observation_timestamp():
    """Get timestamp"""

    now = datetime.datetime.now(timezone.utc)
    # sub 5 hours to showcase est (convert all times to UTC as a good practice)
    now_sub_5 = now + timedelta(hours=-5)
    ts = now_sub_5.strftime('%Y-%m-%d_%H-%M-%S') + ('-%03d' % (now.microsecond / 10000))
    return ts


def calculate_sub_file_list(all_partner_file_list, partner_id):
    """Get a list of files specific to a single partner"""

    try:
        new_list = []
        for file in all_partner_file_list:
            partner_id_parsed = calc_list_partner_id(file)
            if partner_id_parsed == partner_id:
                new_list.append(file)
        return new_list

    except Exception as e:
        raise Exception('Issue in calculating a sub list' + str(e))


def main(event, context):

    partner_id_list = []
    file_list = []
    bucket_name = os.environ['SourceBucket']
    bucket_name_destination = os.environ['DestinationBucket']
    s3_obj, s3_connection = s3_objects_config()

    for key in get_matching_s3_keys(s3_obj, bucket=bucket_name, prefix='inbox/partner', suffix='.json'):
        partner_id_list.append(calc_list_partner_id(key))
        file_list.append(key)

    if len(file_list) == 0:
        logger.info("no newer files identified in the run since files moved to inbox for any partners")
        return 0

    partner_id_list = list(set(partner_id_list))  # dedupe the list
    logger.info(f'partners identified with files {partner_id_list}')

    """ 
        For each partner loop through the corresponding files 
        Read in a Output buffer
        Append to a '|' delimited txt file in destination s3 bucket
        Move json contacts to outbox folder as a archive 
        
    """

    for partner_id in partner_id_list:

        output_buffer = []
        partner_file_list = calculate_sub_file_list(file_list.copy(), partner_id)
        header_flag = 0
        current_time = get_observation_timestamp()

        for partner_json_contact in partner_file_list:
            content_dict = read_json(bucket_name, partner_json_contact, s3_obj)

            if header_flag == 0:
                header_list = calc_header_list(content_dict)
                body_list = calc_body_row(content_dict)
                output_buffer = create_output_buffer(body_list, output_buffer, header_list)
                header_flag = 1
            else:
                body_list = calc_body_row(content_dict)
                output_buffer = create_output_buffer(body_list, output_buffer)

        logger.info(f'Reading Contacts for {partner_id} into output_buffer has completed')
        key_path = f"archive/{partner_id}"
        partner_file_name = f'Acquia-{partner_id}-{current_time}.txt'
        write_to_csv(output_buffer, bucket_name_destination, partner_file_name, key_path)

        logger.info(f'{len(partner_file_list)} contact files for {partner_id} successfully written to '
                    f'{partner_file_name}')
        move_file_s3(bucket_name, partner_file_list, partner_id, s3_connection)
        logger.info(f"Successfully Moved {len(partner_file_list)} json files for {partner_id} in outbox folder ")
