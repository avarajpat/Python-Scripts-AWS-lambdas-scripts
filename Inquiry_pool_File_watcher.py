""" Efficiencies introduced to remove any redundant code to reduce run time and add 
    commit after any update/insert rather than have a single commit to prevent data 
    loss in case of failures".
    Avi Patil / 2020-10-20
    
    Added code to take the daylight saving into consideration 
    Avi Patil / 2020-11-03

    Siju Abraham Avi Patil
    /2020-11-12 added two column in the select enrollment_type,file_category and updated the status with enrollment type.
    
    Avi Patil 11/15 
    Alter Sqs message to add file category

    Avi Patil 1/5/2021
    Alter Sqs message to add file category

"""
import os 
import logging
from datetime import datetime
import boto3
import json
import pyodbc
import pytz
import re
from smb.SMBConnection import SMBConnection
from smb import smb_structs

logger = logging.getLogger('custom_log_stat')
logger.setLevel(logging.DEBUG)
sqs = boto3.client('sqs', region_name = 'us-east-1')
sqs_url =os.environ["IPFilePathQueue_URL"]
secret_man= boto3.client('secretsmanager')
response= secret_man.get_secret_value(SecretId = 'panto-user-secrets')
secretDict= json.loads(response['SecretString'])


def get_smb_connection():
    
    try:
        username = secretDict['Zenaida_username']
        password = secretDict['Zenaida_pwd']
        domain = secretDict['Zenaida_domain']
        address = secretDict['Zenaida_address']
        target_ip = secretDict['Zenaida_target_ip']
        port_number = int(secretDict['Zenaida_port_number'])
        conn = SMBConnection(
                              username, password, domain = domain, use_ntlm_v2=True,
                              is_direct_tcp=True, remote_name = address, my_name = address
                            )
        conn.connect(target_ip, port_number)
        logger.info('Zenaida Connection Successful')
        return conn
    
    except Exception as e:
        raise Exception('There was an error connecting to Zenaida: ' + str(e))


def binary_search(arr, x): 
    
    low = 0
    high = len(arr) - 1
    mid = 0
    while low <= high: 
        mid = (high + low) // 2
        # Check if x is present at mid 
        if arr[mid] < x: 
            low = mid + 1
        # If x is greater, ignore left half 
        elif arr[mid] > x: 
            high = mid - 1
        # If x is smaller, ignore right half 
        else: 
            return mid 
    # If we reach here, then the element was not present ?,
    return -1


def make_dict_file_timestamp(file_list,path,conn,logged_dt):
    
    adict={}
    for sharedfile in file_list:
        file_attr = conn.getAttributes('ftpsites',path + sharedfile.filename)
        time_obj = datetime.fromtimestamp(file_attr.last_write_time)
        time_st_obj_utc= make_UTC_aware(time_obj)
        if time_st_obj_utc > logged_dt :
            adict[file_attr.last_write_time] = file_attr.filename
    return adict
    

def latest_date_file(file_list,path,conn):
    
    file_name_list =[]
    for sharedfile in file_list:
        file_attr = conn.getAttributes('ftpsites',path + sharedfile.filename)
        file_name_list.append(path+file_attr.filename)
    return file_name_list    
      

def get_latest_file_date(file_list,path,conn,logged_dt):
    
    dict_1 = make_dict_file_timestamp(file_list,path,conn,logged_dt)
    if bool(dict_1):
            keymax = max(dict_1, key=int)
            
    else:
            keymax = 0 
    return keymax,dict_1
           

def all_files_till_date(file_list,path,logged_date_dt_obj,conn,dict_1):
    
     file_name_list =[]
     for time_st in dict_1:
         file_name_list.append(path + dict_1[time_st])
     return file_name_list    
    
    
def connect_SQL_server():
    
    try:
        conn = pyodbc.connect(secretDict['osprey_conn_string'])
        logger.info("Connection to MSSQL Osprey Server Successful")    
        return conn
    
    except:
         logger.exception("Cannot connect to OSPREY")
         exit(1)
         
         
def isempty(list_1):
    
    if not list_1: 
        return 1
    
    else: 
        return 0
    
    
def convtoUTC(date_obj):
    
    local_time = pytz.timezone("America/New_York")
    local_datetime = local_time.localize(date_obj, is_dst = is_daylightsaving(date_obj))
    utc_datetime = local_datetime.astimezone(pytz.utc)
    return  utc_datetime 

def is_daylightsaving(date_obj):
    x = datetime(datetime.now().year, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('America/New_York')) # Jan 1 of this year 
    if date_obj.utcoffset() == x.utcoffset():
        return False
    else :
        return True
 

def make_UTC_aware(date_obj):
    
    utc_time = pytz.timezone("UTC")
    utc_datetime = utc_time.localize(date_obj, is_dst = None)
    return  utc_datetime
         

def main(event=None, context= None):
    
    mssql_osprey_conn = connect_SQL_server()
    zenaida2_conn = get_smb_connection()
    cur= mssql_osprey_conn.cursor()
    cur.execute("""SELECT enrollment_schedule_id FROM dw_dbo.Panto_inquiry_file_log with (nolock) order by enrollment_schedule_id  """)
    logged_rows = cur.fetchall()
    list_logged_schedule_id = [i[0] for i in logged_rows]
    cur.execute(
                  """SELECT enrollment_schedule_id,school_cd,school_name,ftp_path,file_name_search_pattern,school_id,counter_ID__C,file_category,enrollment_type,schedule_id
            	       FROM  dbo.panto_ip_file_transfer
            	       order by RunDate, enrollment_schedule_id
                  """
                )
    master_driver_rows = cur.fetchall()
    for row in master_driver_rows:
                try:
                    idx = row[3].index("ftpsites")
                    relative_ftp_path= row[3][idx+8:]
                    result = binary_search(list_logged_schedule_id, row[0])
                    sharedfiles = zenaida2_conn.listPath('ftpsites',relative_ftp_path , pattern= row[4])
                    file_category = re.sub('[\`\'\t,|\\\ (){}\\[\\]~!@#$%^&*+=:;?/>.<-]', '_', str(row[7])).lower()
                    enrollment_type = row[8]
                    if result == -1 :
                        if not isempty(sharedfiles):
                             all_files =  latest_date_file(sharedfiles,relative_ftp_path,zenaida2_conn)
                             for file in all_files :
                                sqs_message = str(row[5]) + '|'+ str(row[1]) + '|'+ str(row[6]) +'|'+ str(file) + '|'+ file_category + '|' + enrollment_type
                                sqs.send_message(
                                QueueUrl=sqs_url,
                                MessageAttributes={},
                                MessageBody=(f"{sqs_message}")
                                )
                                logger.info(f"SQS Event for Partner {sqs_message}:")   
                                        
                             status_message = str(row[8])+' - Files successfully identified for insert'
                             cur.execute(
                                           """insert into dw_dbo.Panto_inquiry_file_log (enrollment_schedule_id,school_cd,new_files_identified,status_log,schedule_id)
                                           select ?,?,?,?,?""",(row[0],row[1],len(all_files),status_message,row[9])
                                        )
                             mssql_osprey_conn.commit()    
                             
                        else:
                            
                            status_message = str(row[8])+' - Empty folder location or file format changed on insert'
                            cur.execute(
                                        """insert into dw_dbo.Panto_inquiry_file_log  (enrollment_schedule_id,school_cd,new_files_identified,status_log,schedule_id)
                                        select ?,?,?,?,?""",(row[0],row[1],0,status_message,row[9])
                                       ) 
                            mssql_osprey_conn.commit()
                            
                    else:   
                     if not isempty(sharedfiles):
                            cur.execute("""select  isnull(rec_update_dt,rec_create_dt)  from dw_dbo.Panto_inquiry_file_log with (nolock) where enrollment_schedule_id = ? """,(row[0]))
                            logged_dt = cur.fetchone()
                            logged_date_dt_obj = logged_dt[0]
                            logged_date_dt_obj_utc =convtoUTC(logged_date_dt_obj)  
                            latest_file_date,dict_1 =  get_latest_file_date(sharedfiles,relative_ftp_path,zenaida2_conn,logged_date_dt_obj_utc)
                            if latest_file_date != 0:
                                all_files = all_files_till_date(sharedfiles,relative_ftp_path,logged_date_dt_obj_utc,zenaida2_conn,dict_1)
                                for file in all_files :
                                    sqs_message = str(row[5]) + '|'+ str(row[1]) + '|'+ str(row[6]) +'|'+ str(file)+ '|'+ file_category + '|' + enrollment_type
                                    sqs.send_message(
                                    QueueUrl=sqs_url,
                                    MessageAttributes={},
                                    MessageBody=(f"{sqs_message}")
                                    )
                                    logger.info(f"SQS Event for Partner {sqs_message}:")
                                
                                status_message = str(row[8])+' - Files updated successfully'
                                cur.execute(
                                             """update dw_dbo.Panto_inquiry_file_log
                                                set new_files_identified = ?, rec_update_dt= getdate(),status_log = ?
                                                where enrollment_schedule_id = ? """,(len(all_files),status_message,row[0])
                                           )
                                mssql_osprey_conn.commit()
                                
                     else:    
                                status_message = str(row[8])+' - Empty folder location or file format changed on update'
                                cur.execute(
                                              """update dw_dbo.Panto_inquiry_file_log
                                               set new_files_identified = '0' ,status_log = ? , rec_update_dt = getdate()
                                               where enrollment_schedule_id = ? """,(status_message,row[0])
                                           ) 
                                mssql_osprey_conn.commit()    
                                
                except smb_structs.OperationFailure :
                    if result == -1 :
                            status_message = str(row[8])+' - Error:Invalid FTP path on Insert'
                            cur.execute(
                                          """insert into dw_dbo.Panto_inquiry_file_log (enrollment_schedule_id,school_cd,new_files_identified,status_log,schedule_id)
                                          select ?,?,?,?,?""",(row[0],row[1],0,status_message,row[9])
                                        )
                            mssql_osprey_conn.commit()
                            
                    else:
                            status_message = str(row[8])+' - Error: Invalid FTP path on update'
                            cur.execute(
                                         """update dw_dbo.Panto_inquiry_file_log
                                            set new_files_identified = '0' ,status_log = ?  , rec_update_dt = getdate()
                                            where enrollment_schedule_id= ? """,(status_message,row[0])
                                       )
                            mssql_osprey_conn.commit()  
                continue
    mssql_osprey_conn.close()
    zenaida2_conn.close()