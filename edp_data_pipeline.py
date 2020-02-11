""" EDP Data Pipeline 

This module copies S3 EDP input files to Snowflake Prestage tables and then merges the Pre-stage records to Stage table
Key steps:
    1. Establish AWS connections
    2. Scans input S3 bucket>folder and identifies all pending files
    3. Connect to Snowflake
    4. Using Snowflake COPY copies all records to an error simulator table that stores the PAYLOAD_JSON as a string (_ESS table).
       Each PreStg table for an object has a corresponding _ESS table
    5. Copies data from _ESS table to the pre-stage table of the given OBJECT_NAME
    5. Prepares the pre-stage records for MERGE
        a. Updates the EXECUTION_ID and STATUS fields of prestage records which are 'UnProcessed' or 'Deferred' + Commit 
        b. De-duplicates and marks duplicate records within the current batch in PreStg to 'Passed-Dup-PreStg' 
        c. De-duplicates and compares data for current record and identifies duplicate records within Stg and sets PreStg status to 'Passed-Dup-PreStg' 
        c. Checks against Stage table for Source_TimeStamp - and marks record 'Passed' if input TS <= STAGE TS Commit
        d. Checks against Stage table for Deleted flag - and marks record 'Passed' if deleted == TRUE + Commit 
        e. Check status of the MERGE table records (e.g. InProgress) and mark the corresponding pre-stage records to 'Deferred' (Commit along with MERGE) 
    6. Using Snowflake MERGE statment merge the pre-stage and stage tables for the given OBJECT_NAME 

Example:
        $ python edp_data_pipeline.py --obj=CUSTOMER

"""

#Error handling
#ERROR AWS - AWS related errors 
#ERROR BUCKET - error getting object from bucket
#ERROR FILE - incorrect naming convention or folder location of the uploaded file
#ERROR PROCESSED - Unable to upload to processed folder
#SUCCESS PROCESSED - Successfully uploaded to process folder
#ERROR SNOWFLAKE - Snowflake related errors
#SUCCESS SNOWFLAKE - Snowflake operations successful 

import snowflake.connector
import sys
import boto3
import boto
import boto.s3.connection
import datetime
import json
from optparse import OptionParser
import logging
from decipher import *
from stripJSONretain import *
from stripBrokenLine import *
from boto.s3.key import Key
import time
from functools import wraps
import inspect
from S3_unzip import *
from inflate import *



def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print ("Total time running %s: %s seconds" %
               (function.__name__, str(t1-t0))
               )
        return result
    return function_timer

# INITIALIZE ALL GLOBAL PARAMETERS HERE
#@fn_timer
def init():
    global orig_object_name
    global object_name
    global logger
    global t_snowflake_start
    global t_snowflake_end
    global mode
    global noData
    global zero  
    
    zero = 0  
 
    t_snowflake_start = datetime.datetime.now()
    t_snowflake_end = datetime.datetime.now()
    
   #logger config
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # create a file handler
    #change the logging config appropriately
    handler = logging.FileHandler('./logs/edp_data_pipeline.log')
    handler.setLevel(logging.INFO)
    
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # add the handlers to the logger
    logger.addHandler(handler)
    
    #command line options to drive which entity to select
    parser = OptionParser()

    parser.add_option("-o","--obj", dest="object",help="object name")

    #THIS IS THE CODE TO SKIP S3 COPY ON DEMAND
    parser.add_option("--mode", dest="mode")

    (options, args) = parser.parse_args()

 
    if options.object == None :
        logger.info ( "object not specified (PRODUCT/CUSTOMER/..). use --obj=<OBJECT>")
        sys.exit(1)
    else :
        p_edp_object_name = options.object

    object_name = p_edp_object_name
    orig_object_name = p_edp_object_name


    if options.mode == None :
        logger.info ("No mode specified. Processing pipeline as usual")       
        mode = "none"  
    else:
        mode = options.mode    




#
# function to construct the Execution Log Insert Statement
def make_execution_log_sql (f_operation,
                            f_object_name,
                            f_file_names,
                            f_execution_id,
                            f_total_rows,
                            f_copied_rows,
                            f_processed_rows,
                            f_errored_rows,
                            f_status,
                            f_message,
                            f_start_ts,
                            f_end_ts):
    ''' this function will construct the insert sql stament for meta_execution_log table
        based on the parameters passed
    '''

    f_execution_log_sql = """INSERT into LANDING.META_EXECUTION_LOG (
                                OPERATION,	
                                OBJECT_NAME,
                                FILE_NAMES,
                                EXECUTION_ID, 
                                TOTAL_ROWS,
                                COPIED_ROWS,
                                PROCESSED_ROWS, 
                                ERRORED_ROWS, 
                                STATUS,
                                MESSAGE,
                                START_TS,
                                END_TS
                            ) values (
                                '"""+f_operation+"""' ,
                                '"""+f_object_name+"""' ,
                                '"""+f_file_names+"""' ,
                                '"""+f_execution_id+"""' ,
                                '"""+f_total_rows+"""' ,
                                '"""+f_copied_rows+"""' ,
                                '"""+f_processed_rows+"""' ,
                                '"""+f_errored_rows+"""' ,
                                '"""+f_status+"""' ,
                                '"""+f_message+"""' ,
                                CURRENT_TIMESTAMP,
                                CURRENT_TIMESTAMP
                                );"""
    #logger.info (f_execution_log_sql)
    return f_execution_log_sql

#end function definitions

#@fn_timer
def connect_to_s3():
    try:
        print('Attempting AWS S3 Connection')
        t_s3_start = datetime.datetime.now()
        #s3_client = boto3.resource('s3',aws_access_key_id = p_aws_access_key_id, aws_secret_access_key = p_aws_secret_access_key)
        global conn
        conn = boto.connect_s3(aws_access_key_id = p_aws_access_key_id, aws_secret_access_key = p_aws_secret_access_key)
        global bucket
        bucket = conn.get_bucket(p_aws_s3_bucket_name)
        print('Connected to AWS')
        ts = time.time()
        st1 = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        print(st1)
    except Exception as e:
        print('[ERROR AWS] - Unable to connect to AWS S3, please check AWS credentials')
        print(e)
        sys.exit(100)



#@fn_timer
def connect_to_snowflake():
    # Gets the Snowflake Connector
    try:
        t_snowflake_start = datetime.datetime.now()
        ctx = snowflake.connector.connect(
            user=p_snowflake_user,
            password=p_snowflake_password,
            account=p_snowflake_account,
            database=p_snowflake_database,
            warehouse=p_snowflake_warehouse,
            schema=p_snowflake_schema
        )
        return ctx
    except Exception as e:
        logger.info ('[ERROR SNOWFLAKE] - Unable to connect to Snowflake - please validate snowflake credentials')
        logger.info(e)
        sys.exit(200) 

#@fn_timer
def get_metadata_params():
    global Prestg
    global Stg
    global Prestg_es
    global Prestg_ess
    global string_splitter
    global s3path
    global obj_para  
    global separator
    global file_format 
    global operating_mode   
    global s3uri
    global zipLogFolder
    global s3CurrentPath
    global s3CurrentUri
 
    ctx = connect_to_snowflake()
    cs = ctx.cursor()

    try:
        
        print("MY DB IS: " + p_snowflake_database)
        print("MY SCHEMA IS: " + p_snowflake_schema)
        
        cs.execute ("Use " + p_snowflake_database + " ; ")
        cs.execute ("Use schema " + p_snowflake_schema + " ; ")
        cs.execute 
        try:
            cs.execute ("Alter warehouse " + p_snowflake_warehouse + " resume;")
        except:
            logger.info ("Warehouse " + p_snowflake_warehouse + " already active")
            cs.execute ("Use warehouse " + p_snowflake_warehouse + " ; ")       
    
        # From the Control Parameter Table get the table names for the given object
        get_para = """select object_name, prestgtablename, stgtablename, s3path, project_name, separator, file_format, prestgestablename,                                                                                prestgesstablename, stringsplittertablename, operatingmode, s3currentpath from LANDING.MTD_S3_to_STG_PARAM where object_name = '"""+object_name+"""';"""
        logger.info ("Para ???=" + get_para) 
        obj_para,Prestg,Stg,s3path,projName,separator,file_format,Prestg_es,Prestg_ess,string_splitter,operating_mode,s3CurrentPath = cs.execute(get_para).fetchone()
        

        print("Object name =" + object_name) 
        print("Pre stg =" + Prestg) 
        print("Stg =" +   Stg ) 
        print("s3CurrentPath =" + s3CurrentPath)
        print("s3path =" + s3path) 
        print("Obj Para =" + obj_para)
        s3uri = s3path.split("/",1)[1]+"/"
        s3CurrentUri = s3CurrentPath.split("/",1)[1]+"/"
        objectpath, current = os.path.split(os.path.split(s3uri)[0])
        zipLogFolder = "@LANDING_STAGE/" + objectpath + "/zip_logs/"        
        print("s3uri = "+s3uri)
        print("s3CurrentUri = "+s3CurrentUri)
        
        cs.close()
        ctx.close()
    except Exception as e:
        logger.info('[ERROR PROCESSED] - Unable to get metadata parameters')
        logger.info(e)


#@fn_timer
def archive_files(s3CurrentPath):
    # Move all current files to Archive folders
    global file_names
    file_names = ""
    connect_to_s3()

    bucket_src = bucket
    bucket_dest = bucket

    #s3_client = boto3.resource('s3', p_aws_access_key_id, p_aws_secret_access_key)

    folder_path = stripAlpha(s3CurrentPath)


    for file in bucket.list(folder_path):
        key = file.name
        dest_key = key.replace('Current', 'Archive')

        file_names = file_names + file.name + ", "

        logger.info(key)
        copy_source = {'Bucket': bucket.name, 'Key': key}
        logger.info('Moving Files')
        try:
            #bucket_dest.copy_key(dest_key,bucket_src.name,key,preserve_acl=False, encrypt_key=True)
            #mybucket.copy_key(key.name, mybucket, dest_key.name)
            #s3_client.meta.client.copy(copy_source, bucket.name, dest_key)
            logger.info('[SUCCESS PROCESSED] - Uploaded file '+key+' moved to Archive folder')
        except Exception as e:
            logger.info('[ERROR PROCESSED] - Unable to upload file '+key+' to Archive folder')
            logger.info(e)

    logger.info ("Files processed in current run " + file_names)

#@fn_timer
def getExecutionId():
    global execution_id

    try:
        ctx = connect_to_snowflake()
        cs = ctx.cursor()

        get_execution_id_sql = "select t.nextval from table(getnextval(EXECUTION_SEQUENCE)) t;"
        seq_num = cs.execute(get_execution_id_sql).fetchone()

        execution_id = str(seq_num[0])

        cs.execute("commit")
        logger.info("The Execution Id for this run is " + str(execution_id))
        cs.close()
        ctx.close()
    except Exception as e:
        logger.info('Failed to copy records')
        raise e


# This function is only used for SO data since the table structures are different
def load_to_stg_cdc(cursor, exec_id, pre_stg, cdc_stg):
    pre_process_state_update_sql = """update """ + pre_stg + """
                                      set CDC_STATUS='PROCESSING', CDC_EXEC_ID=%s
                                      where CDC_STATUS='UNPROCESSED';"""
    process_sql = """
        insert into STG_SALES_ORDER_CDC(
            OBJECT_NAME,
            OBJECT_ID,
            SOURCE,
            TABLE_NAME,
            TABLE_KEY,
            SOURCE_TS,
            PAYLOAD_JSON,
            CREATED_TS,
            LAST_UPDATED,
            EXECUTION_ID,
            DELETED,
            STATUS,
            RECORD_SEQUENCE_ID,
            CDC_STATUS,
            FILE_NAME) 
        select
            OBJECT_NAME,
            OBJECT_ID,
            SOURCE,
            TABLE_NAME,
            TABLE_KEY,
            SOURCE_TS,
            PAYLOAD_JSON,
            CREATED_TS,
            CURRENT_TIMESTAMP,
            EXECUTION_ID,
            DELETED,
            STATUS,
            RECORD_SEQUENCE_ID,
            'UNPROCESSED',
            FILE_NAME
        from """ + pre_stg + """ pstg
        where pstg.CDC_STATUS='PROCESSING' and pstg.CDC_EXEC_ID=%s;"""

    post_process_state_update_sql = """update """ + pre_stg + """ set 
                                       CDC_STATUS='PROCESSED'
                                       where CDC_STATUS='PROCESSING' and CDC_EXEC_ID=%s;"""

    cursor.execute(pre_process_state_update_sql, (exec_id,))
    cursor.execute(process_sql, (exec_id,))
    cursor.execute(post_process_state_update_sql, (exec_id,))



# BUILD SQL STATEMENTS
#
#s3path='@LANDING_STAGE/MASTER_DATA/SAP_ECC/Product/Reprocess'
# alter session parameter for duplicate record handling in MERGE 
#@fn_timer    
def build_sql_stmts():
    global alter_session_sql
    global validate_copy_sql
    global copy_s3toPreStg_sql
    global copy_s3toPreStg_ess_sql
    global copy_s3toPreStg_es_sql
    global copy_ess_to_es_sql
    global copy_es_to_PreStg_sql
    global copy_ess_to_PreStg_sql
    global truncate_ess_sql    
    global truncate_es_sql
    global log_err_sql
    global upd_prestg_id_sql
    global upd_prestg_id_1_sql
    global upd_prestg_id_2_sql
    global dedup_prestg_status_sql
    global pass_prestg_exact_dups_sql
    global pass_prestg_stg_dup_sql
    global pass_prestg_src_ts_sql
    global pass_prestg_deleted_sql
    global defer_on_stg_status_sql
    global merge_prestg_to_stg_sql
    global upd_prestg_comp_sql
    global upd_prestg_error_sql
    global get_count_prestg_sql
    #global execution_id
   

    global copy_s3toStringSplitter_sql
    global updateStringSplitter_sql
    global insertStringSplitter_to_ess_sql
    global insertStringSplitter_to_Prestg_sql
    global truncate_string_splitter_sql


    global get_prestg_count_bc_sql
    global delete_stg_data_sql
    global get_total_records_s3_sql
    global get_total_files_s3_sql
    global get_error_lines_sql
    global insert_pipeline_file_log_sql
    global update_pipeline_log_sql
    global insert_meta_error_log_sql
    global object_name
    global Prestg_es
    global string_splitter 
    global update_meta_error_log_sql 
    global string_splitter_es_diff_sql
    global records_processed_to_es_sql
    global get_total_records_loaded_sql
    global get_records_loaded_pass2_sql
    global get_total_records_sql
    global loaded_to_prestg
    global insert_src_tbl_agg_sum_sql
    global edp_count_summary_sql
    global get_tar_contents_sql

    object_name = 'PRODUCT' if orig_object_name.upper() == 'PRODUCT_BC' else orig_object_name.upper()
    #object_name = 'GLOBAL' if orig_object_name.upper() == 'GLOBAL_DATA' else orig_object_name.upper()

    #DEFINE THE ERROR SIMULATORS HERE
    #Prestg_ess = Prestg + "_ESS"
    #Prestg_es = Prestg + "_ES"
    #string_splitter = object_name + "_STRING_SPLITTER"
    
    prefix_curly = separator+ "{"
    prefix_curly_replacer = prefix_curly + '"'
    suffix_curly = "}" + separator



    
    ctx = connect_to_snowflake()
    cs = ctx.cursor()

    get_prestg_count_bc_sql = """select count(*) from """+Prestg+""" where TABLE_NAME in ('PRODUCT_CLASSIFICATION_V', 'MAST', 'STPO') and OBJECT_NAME = 'PRODUCT' 
                            and STATUS in ('UnProcessed', 'Reprocess', 'Deferred');"""


    delete_prod_bc_stg_data_sql = """delete from """+Stg+""" where TABLE_NAME in ('PRODUCT_CLASSIFICATION_V', 'MAST', 'STPO');"""
    

    copy_s3toStringSplitter_sql = """copy into """+ string_splitter +""" from (select F.$1, '', '', '', replace(split_part(METADATA$FILENAME,'/',-1),'Current'),                                                                       METADATA$FILE_ROW_NUMBER from """ + s3path + """ F) pattern = '.*.*' FILE_FORMAT = 'WHOLE_STRING' FORCE = FALSE purge = TRUE;"""
    
    
   # updateStringSplitter_sql = """update """+ string_splitter + """ set payload_json = regexp_substr(full_string,'\{(.*?)\}'), 
   #                                     string_prefix = left(regexp_substr(full_string,'\(.*?)\{'), length(regexp_substr(full_string,'\(.*?)\{')) - 1 ),
   #                                     string_suffix = substr(full_string, CHARINDEX ( '"}' , full_string, 0 ) + 3, length(full_string) - CHARINDEX ( '"}' , full_string, 0 ) );"""



    #updateStringSplitter_sql = """update """+ string_splitter + """ set payload_json = replace(replace(replace(REGEXP_REPLACE(regexp_substr(full_string,'\{(.*?)\"}'),
    #                                    '[^[:print:]]', ''), '\\\\\\\\' , '' ),' " ', ' \" '),'\', 'BACKLASH'),
    #                                    string_prefix = left(regexp_substr(full_string,'\(.*?)\{'), length(regexp_substr(full_string,'\(.*?)\{')) - 1 ),
    #                                    string_suffix = substr(full_string, CHARINDEX ( '"}' , full_string, 0 ) + 3, length(full_string) - CHARINDEX ( '"}' , full_string, 0 ) );"""


    # updateStringSplitter_sql = """update """+ string_splitter + """ set payload_json = replace(replace(REGEXP_REPLACE(regexp_substr(full_string,'\{(.*?)\"}'),                                                                      '[^[:print:]]', ''), '\\\\\\\\' , '' ),' " ', ' \" '),                                                                                                                                               string_prefix = left(regexp_substr(full_string,'\(.*?)\{'), position('{',full_string) - 1 ),                                                                                                         string_suffix = right(full_string, length(full_string)-(length(left(regexp_substr(full_string,'\(.*?)\{'), position('{',full_string) - 1 ))+                                                         length(regexp_substr(full_string,'\{(.*?)"}')))-1);"""


    # updateStringSplitter_sql = """update """ + string_splitter + """ set
    # payload_json = replace(replace(REGEXP_REPLACE(regexp_substr(full_string, '\\{(.*)\\}',REGEXP_INSTR(full_string,'\\\\""" + separator + """',1,6)),'[^[:print:]]', ''), '\\\\\\\\' , '' ),' " ', ' \" ') ,
    # string_prefix = left(full_string,REGEXP_INSTR(full_string,'\\\\""" + separator + """',1,6)),
    # string_suffix = substr(full_string, length(regexp_substr(full_string, '\\(.*)\\}'))+2)
    # where REGEXP_INSTR(full_string,'\\\\""" + separator + """',1,6) != 0;
    # """

    updateStringSplitter_sql = """
    update """ + string_splitter + """ set 
    payload_json = replace(replace(REGEXP_REPLACE(trim(regexp_substr(full_string,'\\\\""" + separator + """\{(.*)\}\\\\""" + separator + """'),'""" + separator + """'),'[^[:print:]]', ''), '\\\\\\\\' , '' ),' " ', ' \" ') ,
    string_prefix = left(regexp_substr(full_string,'\(.*?)\\\\""" + separator + """\{'), length(regexp_substr(full_string,'\(.*?)\\\\""" + separator + """\{')) - 1 ) ,
    string_suffix = substr(full_string, length(regexp_substr(full_string, '\(.*)\}'))+2) ;
    """

    insertStringSplitter_to_ess_sql = """insert into """ + Prestg_ess + """ select split_part(string_prefix, '|', 1), split_part(string_prefix, '|', 2),
                                        split_part(string_prefix, '|', 3), split_part(string_prefix, '|', 4),split_part(string_prefix, '|', 5), split_part(string_prefix, '|', 6),
                                        payload_json, split_part(string_suffix, '|', 1), split_part(string_suffix, '|', 2), file_name from """ + string_splitter + """ where 
                                        STRING_PREFIX is not NULL and STRING_PREFIX like '""" + object_name + """%' and PAYLOAD_JSON is not NULL and PAYLOAD_JSON <> '{}' and STRING_SUFFIX like 'N%'
                                        and lower(STRING_SUFFIX) like '%unprocessed';"""




    get_replace_patterns = """select listagg(src, ','), listagg(tgt, ',') from landing.edp_text_replace where edp_domain = '"""+object_name+"""';"""
    pattern_src, pattern_tgt = cs.execute(get_replace_patterns).fetchone()


    # OLD STATEMENT - WORKS PERFECTLY BUT HAS PIPE HARD CODED
    #insertStringSplitter_to_Prestg_sql = """insert into """ + Prestg+ """ select split_part(string_prefix, '|', 1), split_part(string_prefix, '|', 2),
    #                                    split_part(string_prefix, '|', 3), split_part(string_prefix, '|', 4),split_part(string_prefix, '|', 5),
    #                                    UDF_FORMATDATEINPUT(split_part(string_prefix, '|', 6)),
    #                                    PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON, '""" + pattern_src + """', '""" + pattern_tgt + """')),
    #                                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '', split_part(string_suffix, '|', 1), split_part(string_suffix, '|', 2),
    #                                    LANDING.RECORD_SEQUENCE.nextval, file_name from """ + string_splitter + """ where
    #                                    STRING_PREFIX is not NULL and STRING_PREFIX like '""" + object_name + """%' and PAYLOAD_JSON is not NULL and STRING_SUFFIX like 'N%';"""



    # NEW STATEMENT - WITH PARAMETERS
    insertStringSplitter_to_Prestg_sql = """insert into """ + Prestg+ """ select split_part(string_prefix, '""" + separator + """', 1), split_part(string_prefix, '""" + separator + """', 2),
                                        split_part(string_prefix, '""" + separator + """', 3), split_part(string_prefix, '""" + separator + """', 4),
                                        split_part(string_prefix, '""" + separator + """', 5), UDF_FORMATDATEINPUT(split_part(string_prefix, '""" + separator + """', 6)),
                                        PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON, '""" + pattern_src + """', '""" + pattern_tgt + """')),
                                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '', split_part(string_suffix, '""" + separator + """', 1), split_part(string_suffix, '""" + separator + """', 2),
                                        LANDING.RECORD_SEQUENCE.nextval, file_name, '', line_number from """ + string_splitter + """ where
                                        concat(upper(file_name), line_number) not in (select concat(upper(file_name), line_number) from """+ Prestg_es+""") and PAYLOAD_JSON <> '{}'
                                        and STRING_PREFIX is not NULL and STRING_PREFIX like '""" + object_name + """%' and PAYLOAD_JSON is not NULL and (STRING_SUFFIX like 'N%'                                                            or STRING_SUFFIX like 'Y%') and upper(STRING_SUFFIX) like '%UNPROCESSED' and check_json(payload_json) is null;"""

    if object_name == 'ORDER':
        insertStringSplitter_to_Prestg_sql = """insert into """ + Prestg+ """ select split_part(string_prefix, '""" + separator + """', 1), split_part(string_prefix, '""" + separator + """', 2),
                                        split_part(string_prefix, '""" + separator + """', 3), split_part(string_prefix, '""" + separator + """', 4),
                                        split_part(string_prefix, '""" + separator + """', 5), concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat
(substring(split_part(string_prefix, '""" + separator + """', 6), 1, 4), '-'), substring(split_part(string_prefix, '""" + separator + """', 6), 5, 2)), '-'),
substring(split_part(string_prefix, '""" + separator + """', 6),7,2)), ' '), substring(split_part(string_prefix, '""" + separator + """', 6),9,2)), ':'), 
substring(split_part(string_prefix, '""" + separator + """', 6),11,2)), ':'), substring(split_part(string_prefix, '""" + separator + """', 6),13,2)), iff(substring(split_part(string_prefix, '""" + separator + """', 6),15,6) != '',concat('.',substring(split_part(string_prefix, '""" + separator + """', 6),15,6)), '.000000')),
                                        PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON, '""" + pattern_src + """', '""" + pattern_tgt + """')),
                                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '', split_part(string_suffix, '""" + separator + """', 1), split_part(string_suffix, '""" + separator + """', 2),
                                        LANDING.RECORD_SEQUENCE.nextval, file_name,'','','', line_number from """ + string_splitter + """ where
                                        concat(file_name, line_number) not in (select concat(file_name, line_number) from """+ Prestg_es+""") and PAYLOAD_JSON <> '{}'
                                        and STRING_PREFIX is not NULL and STRING_PREFIX like 'ORDER%' and PAYLOAD_JSON is not NULL and (STRING_SUFFIX like 'N%'                                                            or STRING_SUFFIX like 'Y%') and upper(STRING_SUFFIX) like '%UNPROCESSED';"""
    alter_session_sql = "ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;"

    # copy command with validation mode
    validate_copy_sql = "copy into "+ Prestg_ess + """ from (select F.$1,F.$2,F.$3,F.$4,F.$5,F.$6,replace(REGEXP_REPLACE(F.$7,'[^[:print:]]', ''),
                        '\\\\', ''),F.$8,F.$9,metadata$filename from """ + s3path + """ F) FORCE = False 
                        FILE_FORMAT =  (FORMAT_NAME = '""" + file_format + """') pattern = '.*.*' VALIDATION_MODE='RETURN_ALL_ERRORS'"""  
    

    # copy data into prestg (add purge = true to delete the files from S3 current folder)
    copy_s3toPreStg_sql= "copy into "+ Prestg +""" from (select F.$1,F.$2,F.$3,F.$4,F.$5,F.$6,replace(F.$7,'+','__'),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'',F.$9,F.$8,
                                     LANDING.RECORD_SEQUENCE.nextval,metadata$filename from """+ s3path + """ F)
                                     FORCE = False pattern = '.*.*' on_error = 'continue';"""
    


    #THE ABOVE STATEMENT WON'T CONTINUE ON ERROR. WE HAVE TO WORKAROUND BY COPYING TO THE ES TABLE AND THEN COPYING BACK TO PRESTG
    copy_s3toPreStg_es_sql = "copy into "+ Prestg_es + """ from (select F.$1,F.$2,F.$3,F.$4,F.$5,
                        concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat
                        (substring(F.$6, 1, 4), '-'), substring(F.$6, 5, 2)), '-'),
                        substring(F.$6,7,2)), ' '), substring(F.$6,9,2)), ':'), 
                        substring(F.$6,11,2)), ':'), substring(F.$6,13,2)), iff(substring(F.$6,15,6) != '',concat('.',substring(F.$6,15,6)), '.000000')),                
                        replace(replace(replace(replace(replace(replace(replace(REGEXP_REPLACE(REGEXP_REPLACE(F.$7,'[^[:print:]]', ''), '^{', '{"'), '", ', '", "'), ': "', '": "'),'\\\\\\\\"', '\\\\\\\\\\\\\\\\"'), '" , "', '\\\\\\\\" \\\\\\\\, \\\\\\\\"'), '","', '\\\\\\\\"\\\\\\\\,\\\\\\\\"'),'"\\\\"','\\\\\\\\"\\\\\\\\\\\\"'),'"\\\\\\\\"', '\\\\\\\\"\\\\\\\\\\\\\\\\"'),
                        F.$8,F.$9,replace(split_part(metadata$filename,'/',-1),'Current'),METADATA$FILE_ROW_NUMBER from """ + s3path + """ F) FORCE = FALSE                                                                   FILE_FORMAT =  (FORMAT_NAME = '""" + file_format + """') pattern = '.*.*' 
                        on_error = 'continue';"""
            
    if object_name == 'ORDER':
        copy_s3toPreStg_es_sql = "copy into " + Prestg_es + """ from (select F.$1,F.$2,F.$3,F.$4,F.$5,
                                concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat
                                (substring(F.$6, 1, 4), '-'), substring(F.$6, 5, 2)), '-'),
                                substring(F.$6,7,2)), ' '), substring(F.$6,9,2)), ':'), 
                                substring(F.$6,11,2)), ':'), substring(F.$6,13,2)), iff(substring(F.$6,15,6) != '',concat('.',substring(F.$6,15,6)), '.000000')),                
                                replace(replace(replace(replace(replace(replace(replace(REGEXP_REPLACE(REGEXP_REPLACE(F.$7,'[^[:print:]]', ''), '^{', '{"'), '", ', '", "'), ': "', '": "'),'\\\\\\\\"', '\\\\\\\\\\\\\\\\"'), '" , "', '\\\\\\\\" \\\\\\\\, \\\\\\\\"'), '","', '\\\\\\\\"\\\\\\\\,\\\\\\\\"'),'"\\\\"','\\\\\\\\"\\\\\\\\\\\\"'),'"\\\\\\\\"', '\\\\\\\\"\\\\\\\\\\\\\\\\"'),
                                F.$8,F.$9,replace(split_part(metadata$filename,'/',-1),'Current'),METADATA$FILE_ROW_NUMBER from """ + s3path + """ F) FORCE = TRUE                                                                   FILE_FORMAT =  (FORMAT_NAME = 'LANDING_PLAIN') pattern = '.*.*' 
                                on_error = 'continue';"""

    #THIS STATEMENT REPLACES THE ABOVE TWO. IT IS USED FOR ENHANCING THE SPEED OF THE PIPELINE
    copy_s3toPreStg_ess_sql = "copy into "+ Prestg_ess + """ from (select F.$1,F.$2,F.$3,F.$4,F.$5,F.$6,
                        replace(replace(REGEXP_REPLACE(F.$7,'[^[:print:]]', ''), '\\\\\\\\' , '' ),'" ', '\" '),
                        F.$8,F.$9,metadata$filename from """ + s3path + """ F) FORCE = TRUE FILE_FORMAT =  (FORMAT_NAME = '""" + file_format + """') 
                        pattern = '.*.*' on_error = 'continue' purge = true;""" 
    
    #copy_ess_to_es_sql = "insert into " + Prestg_es + """ ( OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, SOURCE_TS, 
    #                    PAYLOAD_JSON, DELETED, STATUS,FILE_NAME) 
    #                    select OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, UDF_FORMATDATEINPUT(SOURCE_TS),
    #                    to_variant(replace(replace(replace(replace(replace(REGEXP_REPLACE(PAYLOAD_JSON_STRING, 
    #                    '[^[:print:]]', ''), '{', '{"'), ', ', ', "'), ': "', '": "'), '" ', '"'), ' "', '"')), DELETED, STATUS, 
    #                    FILE_NAME from """ + Prestg_ess + """;"""
    
    copy_ess_to_es_sql = "insert into " + Prestg_es + """ ( OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, SOURCE_TS, 
                        PAYLOAD_JSON, DELETED, STATUS,FILE_NAME) 
                        select OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, UDF_FORMATDATEINPUT(SOURCE_TS),
                        PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON_STRING, '""" + pattern_src + """', '""" + pattern_tgt + """')), DELETED, STATUS, FILE_NAME from """ + Prestg_ess + """;"""
  
    
    copy_ess_to_PreStg_sql = "insert into " + Prestg + """ ( OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, SOURCE_TS, 
                        PAYLOAD_JSON, DELETED, STATUS, EXECUTION_ID, FILE_NAME, CREATED_TS, LAST_UPDATED, RECORD_SEQUENCE_ID) 
                        select OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, UDF_FORMATDATEINPUT(SOURCE_TS),
                        PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON_STRING, '""" + pattern_src + """', '""" + pattern_tgt + """')), DELETED, STATUS, '',
                        FILE_NAME, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, LANDING.RECORD_SEQUENCE.nextval  from """ + Prestg_ess + """;"""
    
    


    copy_es_to_PreStg_sql = "insert into " + Prestg + """ ( OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, SOURCE_TS, 
                        PAYLOAD_JSON, DELETED, STATUS,EXECUTION_ID, FILE_NAME, CREATED_TS, LAST_UPDATED, RECORD_SEQUENCE_ID, LINE_NUMBER) 
                        select OBJECT_NAME, OBJECT_ID, SOURCE, TABLE_NAME, TABLE_KEY, SOURCE_TS,                                                                                                                             PARSE_JSON(udf_remove_slash_from_cols(PAYLOAD_JSON, '""" + pattern_src + """', '""" + pattern_tgt + """')), DELETED, STATUS, 
                        '' , FILE_NAME, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, LANDING.RECORD_SEQUENCE.nextval, LINE_NUMBER from """ + Prestg_es + """;"""
       

    truncate_ess_sql = "truncate TABLE " + Prestg_ess + """;"""
    truncate_es_sql = "truncate TABLE " + Prestg_es + """;"""

    truncate_string_splitter_sql = "truncate TABLE " + string_splitter + """;"""
                        
    

    #Insert error records into metadata table
    #Changed this to copy error records from es table
    #log_err_sql ="Insert into METADATA.MTD_S3_TO_PRESTG_LOAD_ERRORS select ERROR,FILE,LINE,CHARACTER,BYTE_OFFSET,CATEGORY,CODE,SQL_STATE,COLUMN_NAME,ROW_NUMBER,ROW_START_LINE,REJECTED_RECORD,'"+ Prestg +"',CURRENT_TIMESTAMP,'' from table(validate("+Prestg+", job_id=>'_last'));"
    log_err_sql ="Insert into LANDING.MTD_S3_TO_PRESTG_LOAD_ERRORS select ERROR,FILE,LINE,CHARACTER,BYTE_OFFSET,CATEGORY,CODE,SQL_STATE,COLUMN_NAME,ROW_NUMBER,ROW_START_LINE,REJECTED_RECORD,'"+ Prestg_ess +"',CURRENT_TIMESTAMP,'' from table(validate("+Prestg_ess+", job_id=>'_last'));"

    #Create Unique id for each Execution
    #get_execution_id_sql = "select t.nextval from table(getnextval(EXECUTION_SEQUENCE)) t;"
    
    
    
        
    #seq_num = cs.execute(get_execution_id_sql).fetchone()
    
    #execution_id = str(seq_num[0])

    logger.info("The Execution Id for this run is " + str(execution_id))
    
      
    
    # Delete the records with old timestamp so that we fetch only the latest records while fetching the delta records
    #del_old_delta = "Delete FROM "+Prestg+" WHERE OBJECT_ID IN (SELECT C.OBJECT_ID FROM "+Prestg+" C WHERE C.OBJECT_ID = "+Prestg+".OBJECT_ID AND C.SOURCE_TS > "+Prestg+".SOURCE_TS and C.OBJECT_NAME = "+Prestg+".OBJECT_NAME and C.TABLE_NAME = "+Prestg+".TABLE_NAME and C.TABLE_KEY = "+Prestg+".TABLE_KEY);"

    ###
    ###
    ### Build all SQL statements for COPY, MERGE and UPDATE operations 
    ###
    ###
    
    
    
    
    # Update sequence id and status in prestg
    #upd_prestg_id_sql = "update "+Prestg+" set EXECUTION_ID = '"+execution_id+"', EDP_TXN_ID = '"+execution_id+"',status = 'InProgress' where status in ( 'Unprocessed', 'UnProcessed','Deferred');"

    upd_prestg_id_1_sql = "update "+Prestg+" set EXECUTION_ID = '"+execution_id+"', EDP_TXN_ID = '"+execution_id+"' where lower(status) in ('unprocessed');"

    upd_prestg_id_2_sql = "update "+Prestg+" set EXECUTION_ID = '"+execution_id+"', EDP_TXN_ID = '"+execution_id+"',status = 'InProgress' where status in ('UnProcessed', 'Unprocessed', 'Deferred');" 


    if operating_mode.upper() == 'FIFO':
       #If there are duplicate records for the same key - mark all but the latest record as 'Passed'
       dedup_prestg_status_sql = "update " + Prestg + """ pstg
       set pstg.STATUS = 'Deferred',
           pstg.EXECUTION_ID = ''
       where
           pstg.STATUS = 'InProgress'
       and pstg.RECORD_SEQUENCE_ID > (
           select min(dup.RECORD_SEQUENCE_ID)
           from """+Prestg+""" dup
           where   dup.SOURCE = pstg.SOURCE
       and     dup.TABLE_NAME = pstg.TABLE_NAME
       and     dup.TABLE_KEY   = pstg.TABLE_KEY
       and     dup.STATUS   = 'InProgress'
       )
       and exists
           (
           select true
           from """+Prestg+""" dup2
           where   dup2.SOURCE = pstg.SOURCE
           and     dup2.TABLE_NAME = pstg.TABLE_NAME
           and     dup2.TABLE_KEY  = pstg.TABLE_KEY
           and     dup2.STATUS   = 'InProgress'
       );"""

    else:
       dedup_prestg_status_sql = "update " + Prestg + """ pstg
       set pstg.STATUS = 'Passed',
           pstg.EXECUTION_ID = ''
       where
           pstg.STATUS = 'InProgress'
       and pstg.SOURCE_TS < (
           select max(dup.SOURCE_TS)
           from """+Prestg+""" dup
           where   dup.SOURCE = pstg.SOURCE
           and     dup.TABLE_NAME = pstg.TABLE_NAME
           and     dup.TABLE_KEY   = pstg.TABLE_KEY
           and     dup.STATUS   = 'InProgress'
           )
       and exists
           (
           select true
           from """+Prestg+""" dup2
           where   dup2.SOURCE = pstg.SOURCE
           and     dup2.TABLE_NAME = pstg.TABLE_NAME
           and     dup2.TABLE_KEY  = pstg.TABLE_KEY
           and     dup2.STATUS   = 'InProgress'
       );"""
           





    #update prestage record status to Passed for lower record_sequence_ids based on exact duplicate records in PreStage
    pass_prestg_exact_dups_sql = "update " + Prestg + """ pstg
    set pstg.STATUS = 'Passed_Dup_PreStg',
        pstg.EXECUTION_ID = ''
    where
        pstg.STATUS = 'InProgress'
    and pstg.RECORD_SEQUENCE_ID < (
        select max(dup.RECORD_SEQUENCE_ID)
        from """+Prestg+""" dup
        where   dup.SOURCE = pstg.SOURCE
        and     dup.TABLE_NAME = pstg.TABLE_NAME
        and     dup.TABLE_KEY   = pstg.TABLE_KEY
        and     dup.PAYLOAD_JSON = pstg.PAYLOAD_JSON
        and     dup.DELETED = pstg.DELETED
        and     dup.STATUS   = 'InProgress'
        and     dup.EXECUTION_ID = '"""+execution_id+"""'
        )
    and exists
        (
        select true
        from """+Prestg+""" dup2
        where   dup2.SOURCE = pstg.SOURCE
        and     dup2.TABLE_NAME = pstg.TABLE_NAME
        and     dup2.TABLE_KEY  = pstg.TABLE_KEY
        and     dup2.DELETED = pstg.DELETED
        and     dup2.STATUS   = 'InProgress'
        and     dup2.PAYLOAD_JSON = pstg.PAYLOAD_JSON
        and     dup2.EXECUTION_ID = '"""+execution_id+"""'
    );"""



    #Update prestage record status to Passed  based on exact duplicate records in Stage
    pass_prestg_stg_dup_sql = """ update  """+ Prestg +""" pstg
    set pstg.STATUS = 'Passed_Dup_Stg',
        pstg.EXECUTION_ID = ''
    where
                pstg.STATUS = 'InProgress'
        and     pstg.EXECUTION_ID = '"""+execution_id+"""'
    and exists
        (
        select true
        from """+Stg+""" stg
        where   stg.SOURCE = pstg.SOURCE
        and     stg.TABLE_NAME = pstg.TABLE_NAME
        and     stg.TABLE_KEY  = pstg.TABLE_KEY
        and     stg.PAYLOAD_JSON = pstg.PAYLOAD_JSON
        and     stg.DELETED = pstg.DELETED
    );"""



    #Update prestage record status to Passed based on source timestamp
    pass_prestg_src_ts_sql = """ update  """+ Prestg +""" pstg
    set pstg.STATUS = 'Passed',
        pstg.EXECUTION_ID = ''
    where
		pstg.STATUS = 'InProgress'
	and   	pstg.EXECUTION_ID = '"""+execution_id+"""'
    and exists
        (
        select true
        from """+Stg+""" stg
        where   stg.SOURCE = pstg.SOURCE
        and     stg.TABLE_NAME = pstg.TABLE_NAME
        and     stg.TABLE_KEY  = pstg.TABLE_KEY
        and     stg.SOURCE_TS >= pstg.SOURCE_TS
    );"""


    #Update prestage record status to Passed based on STAGE deleted flag
    pass_prestg_deleted_sql = """ update  """+ Prestg +""" pstg
    set pstg.STATUS = 'Passed',
        pstg.EXECUTION_ID = ''
    where
	pstg.STATUS = 'InProgress'
	and   	pstg.EXECUTION_ID = '"""+execution_id+"""'
    and exists
        (
        select true
        from """+Stg+""" stg
        where   stg.SOURCE = pstg.SOURCE
        and     stg.TABLE_NAME = pstg.TABLE_NAME
        and     stg.TABLE_KEY  = pstg.TABLE_KEY
        and     nvl(stg.DELETED, 'N') = 'Y'
    );"""



    #If the same record in the STAGE table is being processed or waiting for processing - Defer the prestage records
    defer_on_stg_status_sql = """ update  """+ Prestg +""" pstg
    set pstg.STATUS = 'Deferred',
        pstg.EXECUTION_ID = ''
    where
	pstg.STATUS = 'InProgress'
	and   	pstg.EXECUTION_ID = '"""+execution_id+"""'
    and exists
        (
        select true
        from """+Stg+""" stg
        where   stg.SOURCE = pstg.SOURCE
        and     stg.TABLE_NAME = pstg.TABLE_NAME
        and     stg.TABLE_KEY  = pstg.TABLE_KEY
        and     stg.STATUS NOT IN ('Completed')
    );"""



    # Merge records from PRESTAGE to STAGE - only for the current execution_id_records
    merge_prestg_to_stg_sql = """Merge into """+Stg+""" as tgt using
                        (Select * from """+Prestg+""" as pstg
                            where pstg.STATUS = 'InProgress'
                            and   pstg.EXECUTION_ID = '"""+execution_id+"""'
                            and NOT exists
                                (select true
                                    from """+Stg+""" stg
                                    where
                                        stg.SOURCE = pstg.SOURCE
                                        and stg.TABLE_NAME = pstg.TABLE_NAME
                                        and stg.TABLE_KEY = pstg.TABLE_KEY
                                        and (stg.DELETED = 'Y' or stg.status in ( 'InProgress', 'UnProcessed', 'Reprocess'))
                                        and stg.SOURCE_TS > pstg.SOURCE_TS )) src
                        on  tgt.SOURCE = src.SOURCE
                        and tgt.OBJECT_NAME = src.OBJECT_NAME
                        and tgt.TABLE_NAME = src.TABLE_NAME
                        and tgt.TABLE_KEY = src.TABLE_KEY
                        when matched then update set
                            tgt.PAYLOAD_JSON = src.PAYLOAD_JSON,
                            tgt.DELETED = src.DELETED,
                            tgt.SOURCE_TS = src.SOURCE_TS,
                            tgt.STATUS = 'UnProcessed',
                            tgt.LAST_UPDATED = current_timestamp,
                            tgt.EDP_TXN_ID = src.EDP_TXN_ID
                        when not matched then insert
                        (   tgt.SOURCE,
                            tgt.OBJECT_NAME,
                            tgt.OBJECT_ID,
                            tgt.TABLE_NAME,
                            tgt.TABLE_KEY,
                            tgt.SOURCE_TS,
                            tgt.PAYLOAD_JSON,
                            tgt.STATUS,
                            tgt.DELETED,
                            tgt.CREATED_TS,
                            tgt.LAST_UPDATED,
                            tgt.EDP_TXN_ID)
                        values
                        (   src.SOURCE,
                            src.OBJECT_NAME,
                            src.OBJECT_ID,
                            src.TABLE_NAME,
                            src.TABLE_KEY,
                            src.SOURCE_TS,
                            src.PAYLOAD_JSON,
                            'UnProcessed',
                            src.DELETED,
                            current_timestamp,
                            current_timestamp,
                            src.EDP_TXN_ID);"""


    # After successful MERGE update the prestage records to Completed 
    upd_prestg_comp_sql = """Update LANDING."""+Prestg+""" pstg
    set pstg.STATUS = 'Completed'
    where
        pstg.STATUS = 'InProgress' AND
        pstg.EXECUTION_ID = '"""+execution_id +"""';"""


    # If the MERGE statement fails - update the prestage records to Error 
    upd_prestg_error_sql = """Update LANDING."""+Prestg+""" pstg
    set pstg.STATUS = 'Error'
    where
        pstg.STATUS = 'InProgress' AND
        pstg.EXECUTION_ID = '"""+execution_id +"""';"""


    # Get the count of the tables
    get_count_prestg_sql = "select count(*) from "+Prestg+""" where status = 'Completed' and EXECUTION_ID = '"""+execution_id+"""';"""
    
    get_total_records_s3_sql = """select count(*) from """+string_splitter+""" where STRING_PREFIX is not NULL and STRING_PREFIX like '""" + object_name + """%' and PAYLOAD_JSON is not NULL and            (STRING_SUFFIX like 'N%' or STRING_SUFFIX like 'Y%') and lower (STRING_SUFFIX) like '%unprocessed';"""
    
    get_total_records_sql = """select count(*) from """+string_splitter+""";"""
    
    get_total_files_s3_sql = "select count(DISTINCT FILE_NAME) from """+string_splitter+""";"""
   
    get_error_lines_sql = "select FULL_STRING, FILE_NAME, LINE_NUMBER from """+string_splitter+""" where lower(STRING_SUFFIX) not like '%unprocessed' or STRING_PREFIX is null or PAYLOAD_JSON is null or STRING_PREFIX is null;""" 
 
    insert_pipeline_file_log_sql = """insert into METADATA.PIPELINE_FILE_LOG (select  DISTINCT '"""+object_name+"""', '"""+execution_id+"""', FILE_NAME, count(*), NULL,                                                                split_part(STRING_PREFIX,'"""+separator+"""',4) from """+string_splitter+""" group by FILE_NAME, split_part(STRING_PREFIX,'"""+separator+"""',4));"""

    update_pipeline_log_sql = """update METADATA.PIPELINE_FILE_LOG set TOTAL_RECORDS_LOADED_TO_PRESTG = counts from (select count(*) as counts, FILE_NAME, TABLE_NAME from """+Prestg+"""                                          as PSTG where PSTG.EDP_TXN_ID = '"""+execution_id+"""' group by FILE_NAME, TABLE_NAME)q1 where METADATA.PIPELINE_FILE_LOG.EDP_TXN_ID = '"""+execution_id+"""'                                        and METADATA.PIPELINE_FILE_LOG.FILE_NAME = q1.FILE_NAME and METADATA.PIPELINE_FILE_LOG.TABLE_NAME = q1.TABLE_NAME;"""

    #insert_meta_error_log_sql = """insert into METADATA.S3_FILE_ERROR_RECORDS (select '"""+object_name+"""','"""+execution_id+"""',current_timestamp(),'','','',FULL_STRING, LINE_NUMBER, FILE_NAME     #                             from """+string_splitter+""" where lower(STRING_SUFFIX) not like '%unprocessed' or STRING_PREFIX is null or PAYLOAD_JSON is null or STRING_PREFIX not like 
    #                            '"""+object_name+"""%';"""

    insert_meta_error_log_sql = """insert into METADATA.S3_FILE_ERROR_RECORDS (select '"""+object_name+"""','"""+execution_id+"""',current_timestamp(),'','','',FULL_STRING, LINE_NUMBER, FILE_NAME                                  from """+string_splitter+""" ss where concat(ss.line_number,upper(ss.file_name)) not in (select concat(line_number,upper(file_name)) from """+Prestg+"""                                                           where edp_txn_id = '"""+execution_id+"""'));""" 

    update_meta_error_log_sql = """update  METADATA.S3_FILE_ERROR_RECORDS set src = q.src, tbl_name = q.tbl_name, tbl_key = q.tbl_key from (select split_part(string_prefix,'"""+separator+"""',3) as                                src, split_part(string_prefix,'"""+separator+"""',4) as tbl_name,split_part(string_prefix,'"""+separator+"""',5) as tbl_key from """+string_splitter+""" where                                        concat(line_number,file_name) in (select concat(line_number,file_name) from METADATA.S3_FILE_ERROR_RECORDS where edp_txn_id = '"""+execution_id+"""')                                                    and split_part(string_prefix,'"""+separator+"""',1) = '"""+object_name+"""') q where edp_txn_id = '"""+execution_id+"""';"""

    #string_splitter_es_diff_sql = """select count(*) from """+string_splitter+""" where concat(file_name, line_number) not in (select concat(file_name, line_number) from """+Prestg_es+""");"""
    records_processed_to_es_sql = """select count(*) from """+Prestg_es+""";"""
    get_total_records_loaded_sql = """select count(*) from """+Prestg+""" where edp_txn_id = '"""+execution_id+"""' and upper(status) = 'UNPROCESSED';"""
    get_records_loaded_pass2_sql = """ select ((select count(*) from """+Prestg+""" where edp_txn_id = '"""+execution_id+"""') - (select count(*) from """+Prestg_es+"""));"""
    string_splitter_es_diff_sql = """select ((select count(*) from """+string_splitter+""") - (select count(*) from """+Prestg_es+"""));"""

    #update_pipeline_summary_sql = """update METADATA.PIPELINE_EXECUTION_SUMMARY set TOTAL_RECORDS_LOADED_TO_PRESTG = '"""+loaded_to_prestg+"""';"""
    insert_src_tbl_agg_sum_sql = """insert into METADATA.EDP_SRC_TBL_AGG_SUMMARY (select OBJECT_NAME, OBJECT_ID, EDP_TXN_ID, TABLE_NAME, TABLE_KEY, FILE_NAME, created_ts 
                                 from """+Prestg+""" where EDP_TXN_ID = '"""+execution_id+"""');"""
    
    # edp_count_summary_sql = """insert into METADATA.EDP_RECORD_COUNT_SUMMARY select object_name, object_id, table_name, to_date(source_ts) dates, to_time(source_ts) time,file_name, max(line_number)                            from """+Prestg+""" where edp_txn_id = '"""+execution_id+"""' group by object_name, object_id, table_name, dates, time, file_name;"""
    edp_count_summary_sql = """insert into METADATA.EDP_RECORD_COUNT_SUMMARY select '""" + execution_id + """', object_name, object_id, split_part(file_name,'/',-1), count(*)                           
    from """ + Prestg + """ where edp_txn_id = '""" + execution_id + """' group by object_name, object_id, file_name;
    """
    get_tar_contents_sql = """copy into METADATA.SOURCE_TAR_CONTENTS from (select to_timestamp(F.$3,'YYYYMMDDHH24MISS'), F.$1, F.$2 from """+ zipLogFolder +""" F )                                                                       pattern = '.*.*' FILE_FORMAT = 'LANDING_PLAIN' FORCE = FALSE purge = TRUE;"""
    cs.close()
    ctx.close()

#@fn_timer
def get_file_and_record_count():

    global total_files_in_s3
    global total_records_in_s3
    try:
        print('Calculating File count and number of records')
            
        ctx = connect_to_snowflake()
        cs = ctx.cursor()
        cs.execute("Begin")
        total_records_in_s3 = cs.execute(get_total_records_s3_sql).fetchone()
        total_files_in_s3 = cs.execute(get_total_files_s3_sql).fetchone()
        cs.execute("commit")	

    except Exception as e:
        logger.info(e)
        logger.info('[ERROR] - Failed to get metrics from string_splitter')
        #cs.execute("rollback")
        raise e

    finally:
        print('Pre-execution metrics are as follows:')
        print('Total number of files to be processed = ' + str(total_files_in_s3[0]))
        print('Total number of records to be processed = ' + str(total_records_in_s3[0]))

#@fn_timer
def execute_sql():
   
    global delta_string_splitter_es 
    global records_processed_to_es
    global records_processed_pass2
    global total_records_processed
    ctx = connect_to_snowflake()
    cs = ctx.cursor()

    cs_pass_1 = ctx.cursor()
    cs_pass_2 = ctx.cursor()

    cs_set_prestg_id = ctx.cursor()

    cs_passed_dup_prestg = ctx.cursor()
    cs_passed_dup_stg = ctx.cursor()
    cs_lifo_fifo = ctx.cursor()
    cs_passed_prestg_src_ts = ctx.cursor()
    cs_passed_prestg_deleted = ctx.cursor()
    cs_deferred_on_stg_status = ctx.cursor()
    
    cs_merge = ctx.cursor()
    
    print("I AM EXECUTING NOW")
    
    
    # Delete the records with old timestamp so that we fetch only the latest records while fetching the delta records
    #del_old_delta = "Delete FROM "+Prestg+" WHERE OBJECT_ID IN (SELECT C.OBJECT_ID FROM "+Prestg+" C WHERE C.OBJECT_ID = "+Prestg+".OBJECT_ID AND C.SOURCE_TS > "+Prestg+".SOURCE_TS and C.OBJECT_NAME = "+Prestg+".OBJECT_NAME and C.TABLE_NAME = "+Prestg+".TABLE_NAME and C.TABLE_KEY = "+Prestg+".TABLE_KEY);"

    ###
    ###
    ### Build all SQL statements for COPY, MERGE and UPDATE operations 
    ###
    ###
    
    

    #####################################################################################################
    ##THIS ENTIRE TRY-CATCH BLOCK GOES WITHIN AN IF CONDITION
    #####################################################################################################

    # COPY 
    #Start the Copy from S3 to Prestage within a Try Catch Block

    if mode.lower() != "skips3" :
        try:
            #logger.info("Starting S3 to Prestage COPY validate "+validate_copy_sql)

            t_snowflake_copy_start = datetime.datetime.now()
        

            ############################################################################################
            ##########                               CURSOR 1                                 ##########
            ##########                               cs_pass_1                                ##########
            ############################################################################################

            #Begin Transactions 
            cs_pass_1.execute("Begin")
    
            cs_pass_1.execute(alter_session_sql)

            logger.info("Starting S3 to Prestage COPY copy "+copy_s3toPreStg_sql)
        
            print("Beginning Pass 1: Copying from S3 to "+Prestg_es)
            cs_pass_1.execute(copy_s3toPreStg_es_sql)
            cs_pass_1.execute(copy_s3toStringSplitter_sql)
            cs_pass_1.execute(updateStringSplitter_sql)
            cs_pass_1.execute("Commit")

            cs_pass_1.execute("Begin")

            cs_pass_1.execute(copy_es_to_PreStg_sql)
            
            delta_string_splitter_es = cs_pass_1.execute(string_splitter_es_diff_sql).fetchone() 
            records_processed_to_es = cs_pass_1.execute(records_processed_to_es_sql).fetchone() 

            cs_pass_1.execute("commit")
            print("Finished Pass 1")

            #print("Finished Pass 1. Processed  "+str(records_processed_to_es[0])+" records. "+str(delta_string_splitter_es[0])+" records remain to be processed.")
        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 1: cs_pass_1 failed')
            cs_pass_1.execute("rollback")
            cs_pass_1.execute(upd_prestg_error_sql)
            cs_pass_1.execute("commit")
            logger.info('Failed to copy records')
            raise e

        finally:
            cs_pass_1.close()



            ############################################################################################
            ##########                               CURSOR 2                                 ##########
            ##########                               cs_pass_2                                ##########
            ############################################################################################

        try:
            cs_pass_2.execute("Begin")
            if delta_string_splitter_es[0] > 0:
                print("Beginning Pass 2: Copying remaining files from "+string_splitter+" to - "+Prestg)
                cs_pass_2.execute(insertStringSplitter_to_Prestg_sql)
                records_processed_pass2 = cs_pass_2.execute(get_records_loaded_pass2_sql).fetchone()
                print("Finished Pass 2")
                #print("Finished Pass 2. Processed "+str(records_processed_pass2[0])+" records.")
            else:
                print("Pass 2 not needed. All records processed successfully in Pass 1.")
                
            #total_records_processed = cs.execute(get_total_records_loaded_sql).fetchone()
            #print("Total Files processed = "+str(total_files_in_s3[0]))               
            #print("Total Records processed = "+str(total_records_processed[0]))               

 
  
            # copy data into prestg
            #cs.execute(copy_s3toPreStg_sql)
            cs_pass_2.execute("commit")
        
            ######################################################
            print("HOLA")
            ########################################################

            #total_records_processed = cs.execute(get_total_records_loaded_sql).fetchone() 
            t_snowflake_copy_end = datetime.datetime.now()
        
            #Insert error records into metadata table
            #cs.execute(log_err_sql)


        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 1: cs_pass_1 failed')
            cs_pass_2.execute("rollback")
            cs_pass_2.execute(upd_prestg_error_sql)
            cs_pass_2.execute("commit")
            logger.info('Failed to copy records')
            raise e

        finally:
            cs_pass_2.close()

        get_file_and_record_count()


    ############################################################################################
    ##########                               CURSOR 3                                 ##########
    ##########                               cs_set_prestg_id                         ##########
    ############################################################################################
    try:
        cs_set_prestg_id.execute("Begin")
        logger.info("Preparing PRESTAGE records for MERGE "+upd_prestg_id_1_sql)

        t_snowflake_start = datetime.datetime.now()
        
        #Begin Transaction       
        try:
            cs_set_prestg_id.execute("Begin")
       
            # update status and exec id in prestg - Grabbing all qualifying records 
            cs_set_prestg_id.execute(upd_prestg_id_1_sql)
            total_records_processed = cs.execute(get_total_records_loaded_sql).fetchone() 
            cs_set_prestg_id.execute(upd_prestg_id_2_sql)
            cs_set_prestg_id.execute("Commit")
        

        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 3: cs_set_prestg_id failed')
            cs_set_prestg_id.execute("rollback")
            cs_set_prestg_id.execute(upd_prestg_error_sql)
            cs_set_prestg_id.execute("commit")
            raise e





        # Start pre-processing PreStage records for merge

        ############################################################################################
        ##########                               CURSOR 4                                 ##########
        ##########                               cs_passed_dup_prestg                     ##########
        ############################################################################################

        logger.info("Preparing PRESTAGE records for MERGE dedup"+dedup_prestg_status_sql)
        try:
            cs_passed_dup_prestg.execute("Begin")
            # Pass records with lower record_seq_ids and only process the one with the max value for exact duplicates
            cs_passed_dup_prestg.execute(pass_prestg_exact_dups_sql)
            cs_passed_dup_prestg.execute("Commit")


        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 4: cs_passed_dup_prestg failed')
            cs_passed_dup_prestg.execute("rollback")
            cs_passed_dup_prestg.execute(upd_prestg_error_sql)
            cs_passed_dup_prestg.execute("commit")
            raise e


        ############################################################################################
        ##########                               CURSOR 5                                 ##########
        ##########                               cs_passed_dup_stg                        ##########
        ############################################################################################

        try:
            #cs_passed_dup_stg.execute("Begin")
            # Pass prestg records that have identical payload_json in stg
            cs_passed_dup_stg.execute(pass_prestg_stg_dup_sql)
            cs_passed_dup_stg.execute("Commit")
     
        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 5: cs_passed_dup_stg failed')
            cs_passed_dup_prestg.execute("rollback")
            cs_passed_dup_prestg.execute(upd_prestg_error_sql)
            cs_passed_dup_prestg.execute("commit")
            raise e

        ############################################################################################
        ##########                               CURSOR 6                                 ##########
        ##########                               cs_lifo_fifo                             ##########
        ############################################################################################

        try:
            cs_lifo_fifo.execute("Begin")
            cs_lifo_fifo.execute(dedup_prestg_status_sql)
            cs_lifo_fifo.execute("commit")

        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 6: cs_lifo_fifo failed')
            cs_lifo_fifo.execute("rollback")
            cs_lifo_fifo.execute(upd_prestg_error_sql)
            cs_lifo_fifo.execute("commit")
            raise e


        ############################################################################################
        ##########                               CURSOR 7                                 ##########
        ##########                               cs_passed_prestg_src_ts                  ##########
        ############################################################################################

        try:
            # Pass records with earlier timestamp and deleted flag check
            cs_passed_prestg_src_ts.execute("Begin")
            cs_passed_prestg_src_ts.execute(pass_prestg_src_ts_sql)
            cs_passed_prestg_src_ts.execute("Commit")

        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 7: cs_passed_prestg_src_ts failed')
            cs_passed_prestg_src_ts.execute("rollback")
            cs_passed_prestg_src_ts.execute(upd_prestg_error_sql)
            cs_passed_prestg_src_ts.execute("commit")
            raise e


        ############################################################################################
        ##########                               CURSOR 8                                 ##########
        ##########                               cs_passed_prestg_deleted                 ##########
        ############################################################################################

        #>>> 2-May-2019 Commeting out code for pass_prestg_deleted_sql to allow processing of records with Delete flag
        #try:
        #    cs_passed_prestg_deleted.execute("Begin")
        #    cs_passed_prestg_deleted.execute(pass_prestg_deleted_sql)
        #    cs_passed_prestg_deleted.execute("Commit")

        #    t_snowflake_prep_end = datetime.datetime.now()

        #except Exception as e:
        #    logger.info(e)
        #    logger.info('[ERROR SNOWFLAKE] - CURSOR 7: cs_passed_prestg_deleted failed')
        #    cs_passed_prestg_deleted.execute("rollback")
        #    cs_passed_prestg_deleted.execute(upd_prestg_error_sql)
        #    cs_passed_prestg_deleted.execute("commit")
        #    raise e
        # <<< Fix End


        ############################################################################################
        ##########                               CURSOR 10                                ##########
        ##########                               cs_deferred_on_stg_status                ##########
        ############################################################################################

        try:
            cs_deferred_on_stg_status.execute("Begin")
            cs_deferred_on_stg_status.execute(defer_on_stg_status_sql)
            cs_deferred_on_stg_status.execute("Commit")

        except Exception as e:
            logger.info(e)
            logger.info('[ERROR SNOWFLAKE] - CURSOR 7: cs_passed_prestg_deleted failed')
            cs_deferred_on_stg_status.execute("rollback")
            cs_deferred_on_stg_status.execute(upd_prestg_error_sql)
            cs_deferred_on_stg_status.execute("commit")
            raise e


    except Exception as e:
        logger.info(e)
        logger.info('[ERROR SNOWFLAKE] - Prestage to Stage MERGE Preprocessing failed')

        cs.execute(make_execution_log_sql ('PRESTAGE-TO-STAGE-MERGE ', object_name, file_names, execution_id,
                                               '0', '0', '0', '0', 'ERROR', 'Merge failed',
                                               str(t_snowflake_start), str(t_snowflake_end)) )
        cs.execute("commit")
        raise e
    finally:
        cs_set_prestg_id.close()
        cs_passed_dup_prestg.close()
        cs_passed_dup_stg.close()
        cs_lifo_fifo.close()
        cs_passed_prestg_src_ts.close()
        cs_passed_prestg_deleted.close()
        cs_deferred_on_stg_status.close()
        cs.close()



    ############################################################################################
    ##########                               CURSOR 11                                ##########
    ##########                               cs_merge                                 ##########
    ############################################################################################

        
    #Final Merge Block
    try:
        logger.info("Starting Prestage to Stage MERGE")

        t_snowflake_merge_start = datetime.datetime.now()
            
        # Alter Session Parameter for MERGE duplicate handling 
        cs_merge.execute(alter_session_sql)
            
        cs_merge.execute("Begin") 

        # delete from Stage if OBJECT is of type PRODUCT_CLASSIFICATION, PRODUCT_BOM or PRODUCT_BOM_ITEM
        if orig_object_name == 'PRODUCT_BC':
            count = cs_merge.execute(get_prestg_count_bc_sql).fetchone()
            iCount = int(count[0])
    
            if iCount == 0:
                print("No records to process")
       
            else:
                cs_merge.execute(delete_prod_bc_stg_data_sql)


        ######################################################################################################################################################
        # execute merge
        cs_merge.execute(merge_prestg_to_stg_sql)           
        ######################################################################################################################################################
 


        logger.info("MERGE complete - starting post-processing")
           
        # on successful merge update status as completed in prestg
        logger.info("Mark PreStg records as complete")
        logger.info(upd_prestg_comp_sql)
        cs_merge.execute(upd_prestg_comp_sql)
        cs_merge.execute(insert_src_tbl_agg_sum_sql)    

        if object_name.lower() == "order":
            load_to_stg_cdc(cs_merge, execution_id, Prestg, "")

        # issue commit statement
        cs_merge.execute("Commit")

        t_snowflake_end = datetime.datetime.now()
            
        # get the count of the records
        record_count = cs_merge.execute(get_count_prestg_sql).fetchone()


        # Populate Execution Log
        cs_merge.execute(make_execution_log_sql ('PRESTAGE-TO-STAGE-MERGE ', object_name, file_names, execution_id,
                                               str(record_count[0]), str(record_count[0]), str(record_count[0]), '0', 'SUCCESS', 'Merge successful',
                                               str(t_snowflake_start), str(t_snowflake_end)) )
                        
        cs_merge.execute("Commit")

        logger.info('[SUCCESS SNOWFLAKE] - Data successfully merged into '+Stg)
            
    except Exception as e:            
        logger.info(e)
        logger.info('[ERROR SNOWFLAKE] - Merge to stage from prestage failed')            
        cs_merge.execute("rollback")
        cs_merge.execute(upd_prestg_error_sql)
        cs_merge.execute("commit")            
        raise e
        

    finally:
        cs_merge.close()
        ctx.close() 


   

# function to construct the Execution Log Insert Statement
def make_pipeline_summary_sql  (f_object_name,
                            f_edp_txn_id,
                            f_total_files_in_s3,
                            f_total_records_in_s3,
                            f_total_records_loaded_to_prestg,
                            f_total_records_in_batch,
                            f_total_records_deferred,
                            f_total_records_passed,
                            f_total_records_passed_dup_prestg,
                            f_total_records_passed_dup_stg,
                            f_total_records_loaded_to_stg,
                            f_start_ts,
                            f_end_ts):
    ''' this function will construct the insert sql stament for meta_execution_log table
        based on the parameters passed
    '''

    f_execution_summary_sql = """INSERT into METADATA.PIPELINE_EXECUTION_SUMMARY (
                             OBJECT_NAME,
                             EDP_TXN_ID,
                             TOTAL_FILES_IN_S3,
                             TOTAL_RECORDS_IN_S3,
                             TOTAL_RECORDS_LOADED_TO_PRESTG,
                             TOTAL_RECORDS_IN_BATCH,
                             TOTAL_RECORDS_DEFERRED,
                             TOTAL_RECORDS_PASSED,
                             TOTAL_RECORDS_PASSED_DUP_PRESTG,
                             TOTAL_RECORDS_PASSED_DUP_STG,
                             TOTAL_RECORDS_LOADED_TO_STG,
                             EDP_TXN_START_TIME,
                             EDP_TXN_END_TIME
                            ) values (
                             '"""+f_object_name+"""' ,
                             '"""+f_edp_txn_id+"""' ,
                             '"""+f_total_files_in_s3+"""' ,
                             '"""+f_total_records_in_s3+"""' ,
                             '"""+f_total_records_loaded_to_prestg+"""' ,
                             '"""+f_total_records_in_batch+"""' ,
                             '"""+f_total_records_deferred+"""' ,
                             '"""+f_total_records_passed+"""' ,
                             '"""+f_total_records_passed_dup_prestg+"""' ,
                             '"""+f_total_records_passed_dup_stg+"""' ,
                             '"""+f_total_records_loaded_to_stg+"""' ,
                             CURRENT_TIMESTAMP,
                             CURRENT_TIMESTAMP
                             );"""
    #logger.info (f_execution_summary_sql)
    return f_execution_summary_sql

#@fn_timer
def get_post_execution_status():

    global total_records_in_batch
    global total_records_deferred
    global total_records_passed
    global total_records_passed_dup_prestg
    global total_records_passed_dup_stg
    global total_records_loaded_to_prestg
    global total_records_loaded_to_stg
    global total_files_in_s3 
    global loaded_to_prestg

    ctx = connect_to_snowflake()
    cs_post_execution_1 = ctx.cursor()
    cs_post_execution_2 = ctx.cursor()
    cs_post_execution_3 = ctx.cursor()


    Prestg_ess = Prestg + "_ESS"
    truncate_ess_sql = "truncate TABLE " + Prestg_ess + """;"""
    t_snowflake_end = datetime.datetime.now()

    if mode.lower() == "skips3":
        total_files_in_s3 = "0"
        total_records = "0"
        total_records_loaded_to_prestg = "0"

    try:
        get_prestg_load_count_sql = "select count(*) from " +Prestg+ " where EDP_TXN_ID = '"+execution_id+"';"

        get_total_batch_count_sql = "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"';"

        get_deferred_count_sql = "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"' and STATUS = 'Deferred';"

        get_passed_count_sql = "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"' and STATUS = 'Passed';"

        get_passed_dup_prestg_count_sql = "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"' and STATUS = 'Passed_Dup_PreStg';"

        get_passed_dup_stg_count_sql = "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"' and STATUS = 'Passed_Dup_Stg';"

        get_stg_load_count_sql =  "select count(*) from " +Prestg+" where EDP_TXN_ID = '"+execution_id+"' and STATUS = 'Completed';"


        if mode.lower() != "skips3":
            total_records_loaded_to_prestg = cs_post_execution_1.execute(get_prestg_load_count_sql).fetchone()

        total_records_in_batch = cs_post_execution_1.execute(get_total_batch_count_sql).fetchone()

        total_records_deferred = cs_post_execution_1.execute(get_deferred_count_sql).fetchone()

        total_records_passed = cs_post_execution_1.execute(get_passed_count_sql).fetchone()

        total_records_passed_dup_prestg = cs_post_execution_1.execute(get_passed_dup_prestg_count_sql).fetchone()

        total_records_passed_dup_stg = cs_post_execution_1.execute(get_passed_dup_stg_count_sql).fetchone()

        total_records_loaded_to_stg = cs_post_execution_1.execute(get_stg_load_count_sql).fetchone()

        total_records = cs_post_execution_1.execute(get_total_records_sql).fetchone()


        cs_post_execution_1.execute(make_pipeline_summary_sql (object_name, execution_id, str(total_files_in_s3[0]), str(total_records[0]),
                                               str(total_records_loaded_to_prestg[0]), str(total_records_in_batch[0]), str(total_records_deferred[0]), str(total_records_passed[0]),
                                               str(total_records_passed_dup_prestg[0]), str(total_records_passed_dup_stg[0]), str(total_records_loaded_to_stg[0]),
                                               str(t_snowflake_start), str(t_snowflake_end) ) )


        #cs.execute(truncate_ess_sql)

        cs_post_execution_1.execute("Begin")

        cs_post_execution_1.execute(insert_pipeline_file_log_sql)

        cs_post_execution_1.execute(update_pipeline_log_sql)                                             	

        cs_post_execution_1.execute("Commit")

       
        if abs(total_records[0]-total_records_processed[0]) > 0:
            cs_post_execution_2.execute("Begin")
            cs_post_execution_2.execute(insert_meta_error_log_sql)
            cs_post_execution_2.execute(update_meta_error_log_sql)
            cs_post_execution_2.execute("Commit")


        cs_post_execution_3.execute("Begin")
        cs_post_execution_3.execute(truncate_es_sql)
        cs_post_execution_3.execute(truncate_string_splitter_sql)
        cs_post_execution_3.execute("Commit")



        #total_records_processed = cs_post_execution_3.execute(get_total_records_loaded_sql).fetchone()
        #loaded_to_prestg = records_processed_to_es[0] + (total_records_processed[0] - records_processed_to_es[0])
        #cs_post_execution_3.execute(update_pipeline_summary_sql)
        print("Execution Complete. Metrics are as follows")
        if mode.lower() != "skips3":
            print("Pass 1 - Process from ES: Processed  "+str(records_processed_to_es[0])+" records.")
            print("Pass 2 - Process from StringSplitter: Processed "+str(total_records_processed[0] - records_processed_to_es[0])+" records.")
            print("Total Files processed = "+str(total_files_in_s3[0]))
            print("Total Records processed (loaded to Prestg) = "+str(total_records_processed[0])+" records.")
            print("Total Records processed (including previously Deferred records) = "+str(total_records_processed[0]))
            sys.exit(0)
        else:
            print("Total Records processed (including previously Deferred records) = "+str(total_records_processed[0]))
            print("Please run the corresponding assembler(s) prior to rerunning the pipeline in skips3 mode for the same object.")
            sys.exit(0)
        cs_post_execution_3.execute("commit")


    except Exception as e:
        logger.info(e)
        logger.info('[ERROR SNOWFLAKE] - COULD NOT GET POST EXECUTION DATA')

        cs_post_execution_1.execute("rollback")
        cs_post_execution_2.execute("rollback")
        cs_post_execution_3.execute("rollback")

        raise e
    finally:
        cs_post_execution_1.close()
        cs_post_execution_2.close()
        cs_post_execution_3.close()


        ctx.close()

def checkTarFiles():
    logger.info("checking folder for zip/tar files.....")

    path = "s3://" + p_aws_s3_bucket_name + "/" + s3CurrentUri
    awscmd = "aws s3 ls " + path + " | grep \".tar\|.gz\" | awk \'{print $4}\'"

    tar_files = subprocess.getoutput(awscmd)
    return len(tar_files)

def get_edp_count_summary():
    ctx = connect_to_snowflake()
    cs = ctx.cursor()
    try:
        cs.execute("Begin")
        cs.execute(edp_count_summary_sql)
        cs.execute("commit")
    except Exception as e:
        logger.info(e)
        logger.info('[ERROR SNOWFLAKE] - COULD NOT INSERT EDP RECORD COUNT SUMMARY')
    
    finally:
        cs.close()
        ctx.close()

def get_tar_file_metrics():
    ctx = connect_to_snowflake()
    cs = ctx.cursor()
    try:
        cs.execute("Begin")
        cs.execute(get_tar_contents_sql)
        cs.execute("commit")
    except Exception as e:
        logger.info(e)
        logger.info('[ERROR SNOWFLAKE] - COULD NOT INSERT TAR CONTENTS FROM ZIP LOGS')

    finally:
        cs.close()
        ctx.close()

    
if __name__ == '__main__':
    init()
    get_metadata_params()
    archive_files(s3CurrentPath)
    #enforce_dq(s3path)
    getExecutionId()
    #stripBadLine(object_name, s3path)
 
    noData = hasData(s3CurrentPath)
    if noData == False or mode.lower() == "skips3":
        print("bucket = "+str(p_aws_s3_bucket_name)+" path = "+s3uri)
        num_tar_files = checkTarFiles()
        build_sql_stmts()
        if num_tar_files > 0:
            moveFilesToProcessingDir(s3CurrentUri,s3uri,p_aws_s3_bucket_name) 
            decompressFiles(s3uri, p_aws_s3_bucket_name)
            untarFiles(s3uri, p_aws_s3_bucket_name)
            get_tar_file_metrics()

        execute_sql()
        get_edp_count_summary()
        get_post_execution_status()

    
        logger.info('[SUCCESS SNOWFLAKE] - Prestage to Stage MERGE for ' + object_name + ' successfully completed')

    else:
        print("Exiting. No data to process for the named object")
        ctx = connect_to_snowflake()
        cs = ctx.cursor()
        cs.execute(make_pipeline_summary_sql (object_name, execution_id, str(zero) , str(zero),
                                               str(zero), str(zero), str(zero), str(zero),
                                               str(zero), str(zero), str(zero),
                                               str(t_snowflake_start), str(t_snowflake_end) ) )
        cs.execute("commit")




