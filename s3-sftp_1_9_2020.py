
"""
Created on Thu Jan  9 09:45:48 2020
@author: Avadhoot(Avi) Patil
Lambda function that triggers when a new file is in the S3 Bucket and sends it to the NSC SFTP

"""

import logging
import os
import boto3
import paramiko
import botocore.exceptions
import nsc_config as config
import nsc_helpers as helpers
import nsc_time

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG')) # logging level is DEBUG and higher 

# Environmant variables
SSH_HOST = config.secrets['host']          
SSH_USERNAME = config.secrets['username']
SSH_PASSWORD = config.secrets['password']
SSH_PORT = config.secrets['port']
SSH_DIR = config.secrets['upload_dir']

# Function to  making a connection to SFTP
def connect_to_sftp(hostname, port, username, password):
    
    transport = paramiko.Transport((hostname, port))

    # Program will exit if failure to Connect and Log an Exception 
    try :
        transport.connect(username=username, password=password)

    except paramiko.SSHException as ex :
        logger.exception("Connection to remote SFTP server failed ")  
        exit(1)

    client = paramiko.SFTPClient.from_transport(transport)
    logger.info("S3-SFTP: Connected to remote SFTP server")
    return client

# The entry-point for the trigger event 
def on_trigger_event(event, context):
    logger.info("S3-SFTP: received trigger event")
    sftp_client = connect_to_sftp(
        hostname=SSH_HOST,
        port=SSH_PORT,
        username=SSH_USERNAME,
        password=SSH_PASSWORD
    )
    
    sftp_client.chdir(SSH_DIR) 
    logger.debug("S3-SFTP: Switched into remote SFTP upload directory")
    
    # Get the Bucket and Key attributes 
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f"S3-SFTP: Received trigger on '{ key }'")
       
    s3_file= boto3.resource('s3',region_name = 'us-east-1').Object(bucket, key)   #s3_file_object
    
 # SFTP File Trasnfer
    try :
        logger.info(f"S3-SFTP: Transferring S3 file '{s3_file.key}' started")
        with sftp_client.file(s3_file.key, 'w') as sftp_file:
            s3_file.download_fileobj(Fileobj=sftp_file)
        logger.info(f"S3-SFTP: Transferred successfully '{ s3_file.key }' from S3 to SFTP")
        status_message = f"{nsc_time.pretty_time()} File uploaded successfully: {key}"
        helpers.send_to_slack(status_message, config.nsc_log_channel)
    
    except paramiko.IOError as e : 
        logger.exception("S3-SFTP: Transferred Failed , File on SFTP cannot be opened in Write Mode")
        status_message = f"{nsc_time.pretty_time()} Error while attempting to upload: {key}"
        helpers.send_to_slack(status_message, config.nsc_log_channel)
        exit(1)
    
    except  botocore.exceptions.BotoCoreError as b :
      logger.exception("S3-SFTP: Transferred Failed , Issue with the S3 Bucket/File")
      status_message = f"{nsc_time.pretty_time()} Error while attempting to upload: {key}"
      helpers.send_to_slack(status_message, config.nsc_log_channel)
      exit(1) 
        