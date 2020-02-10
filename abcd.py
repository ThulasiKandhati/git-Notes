#!/usr/local/bin/python3
#########################################################################################################################
# DIY_src2stg.py                                                                                                        #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Apr-2019                Initial Version                                                 #
# 1.1           Manick          May-2019                Revised split logic                                             #
# 1.2           Manick          May-2019                Add Trim while extracting filter from TD                        #
# 1.3           Manick          May-2019                Derive base table from 1-1 view for TD Data dictionary queries  #
# 1.4           Manick          May-2019                Enable JCT entry and invoke ingestion in parallel               #
# 1.5           Manick          May-2019                Write JCT entires into a file before execution                  #
# 1.6           Manick          May-2019                Insert generated JCT sql file                                   #
# 1.7           Manick          May-2019                Enable Runlog                                                   #
# 1.8           Manick          May-2019                Invoke Talend for ingestion                                     #
#                                                       Append datetime to master record for easy reprocessing          #
# 1.9           Manick          May-2019                Bug fix with incremental logic if record is huge                #
# 1.10          Manick          May-2019                Check for job group existing and create if not                  #
# 1.11          Manick          May-2019                Option to override thread count                                 #
# 1.12          Manick          May-2019                Bug fix to use large table split columns only as int & date     #
# 1.13          Manick          May-2019                Integrate SF code to merge & create stage table from slices     #
# 1.14          Manick          May-2019                Convert all hard coded params to read from config file          #
# 1.15          Manick          May-2019                Bug fix for incremental ingestion from JCT                      #
# 1.16          Manick          May-2019                Implement logic to update incremental column details            #
# 1.17          Manick          May-2019                Bug fix for incremental column update                           #
# 1.18          Manick          May-2019                Change dbc.indices to indicesv bug fix                          #
# 1.19          Manick          May-2019                Bug fix for large table with no index                           #
# 2.0           Manick          May-2019                Include Oracle logic                                            #
# 2.1           Manick          May-2019                Fix Missing logic for retry since upd inc is failing            #
# 2.2           Manick          May-2019                Bug fix with exception inside index column loop                 #
#                                                       Bug fix with long running oracle index identification query     #
#                                                       Flush logfile content to see then and their                     #
# 2.3           Manick          May-2019                Enable thread count logic to restrict number of parallel proc   #
# 2.4           Manick          May-2019                Fix bug with retry flag for multi threaded runs                 #
# 2.5           Manick          May-2019                Bug fix on updating last extract date for Oracle                #
# 2.6           Manick          May-2019                Fix bug with process submission in parallel                     #
# 2.7           Manick          May-2019                Change SF connections to use token from keeeper                 #
# 2.8           Manick          May-2019                Check and abort if metadata doesnt exist                        #
# 2.9           Manick          May-2019                Enable metadata collection for Oracle & TD db                   #
# 3.0           Manick          May-2019                Impl. of revolutionary logic on oracle splitting logic          #
# 3.1           Manick          May-2019                Treat both part & non-part oracle table same due to bug         #
# 3.2           Manick          May-2019                Bug fix with missing quote for date based sliced tables         #
# 3.3           Manick          May-2019                Bug fix with overalapping sequence post de-norm                 #
# 3.4           Manick          May-2019                Bug fix with missing schema in SF during merge process          #
# 3.5           Manick          May-2019                Remove whitespaces from input file                              #
# 3.6           Manick          May-2019                Fix bug with date addition while creating buckets               #
# 3.7           Manick          May-2019                Bug fix with reading source count from flat file type conver    #
# 3.8           Manick          May-2019                Bug fix with meta data collection invocation script             #
# 3.9           Manick          May-2019                Bug fix with timestamp range missing paranthesis                #
# 3.10          Manick          May-2019                Bug fix while checking completed jobs by removing %             #
# 3.11          Manick          May-2019                Bug fix to include Decimal type for TD                          #
# 3.12          Manick          May-2019                Bug fix for existing table with null for incremental            #
# 3.13          Manick          May-2019                Bug fix with data type while finding oracle indiex              #
# 3.14          Manick          May-2019                Bug fix when filter exists for history load                     #
# 3.15          Manick          May-2019                Bug fix when filter has split then filter is coming twice in JCT#
# 3.16          Manick          May-2019                Bug fix with thread count parameter override                    #
# 3.17          Manick          Jun-2019                Introduce split count to override program defined threadcount   #
# 3.18          Manick          Jun-2019                Bug fix with number of pieces on retry failed job               #
# 3.19          Manick          Jun-2019                Bug fix when range value comes as null                          #
# 3.20          Manick          Jun-2019                Bug fix to collect metadata against TD Tbl and not view         #
# 3.21          Manick          Jun-2019                Change in logic to handle oracle table with LOB columns         #
# 3.22          Manick          Jun-2019                Change JCT status at the end of ingestion                       #
# 3.23          Manick          Jun-2019                Add status code A while checking retrying existing JCT          #
# 3.24          Manick          Jun-2019                Re-use existing JCT entries for subsequent large ingestions     #
# 3.25          Manick          Jun-2019                Bug while upd incremental column for TD                         #
# 3.26          Manick          Jun-2019                Update JCT Inc only if it is null                               #
# 3.27          Manick          Jun-2019                Modify mount point check to consider thread and split count     #
# 3.28          Manick          Jun-2019                Bug with trim function while using timestamp columne            #
# 3.29          Manick          Jun-2019                Disable special processing for LOB since it is fixed at Talend  #
# 3.30          Manick          Jun-2019                Bug fix with threadcount for date based ingestion               #
# 3.31          Manick          Jun-2019                Option to override split column                                 #
# 3.32          Manick          Jun-2019                Update incremental from child to master only if child exists    #
# 3.33          Manick          Jun-2019                Bug fix on wrong record picked up for reprocessing              #
# 3.34          Manick          Jun-2019                Bug fix with job stream monitoring                              #
# 3.35          Manick          Jul-2019                Leave master JCT record with status code C so TES wont fail     #
# 3.36          Manick          Jul-2019                Update script to read SF token from config file                 #
# 3.37          Manick          Jul-2019                Include tbl paritition while finding tbl size to fix float error#
# 3.38          Manick          Aug-2019                Remove additional date filter in case of oracle for consistency #
# 4.0           Manick          Aug-2019                Modification to include master log, tblsize and tpt             #
# 4.1           Manick          Aug-2019                Capture exception inside SF merge exception process             #
# 4.2           Manick          Aug-2019                Include retry for child with 'P' and update job_group           #
# 4.3           Manick          Aug-2019                Bug fixed with elapsed days for TD with 7453 error              #
# 4.4           Manick          Aug-2019                Bug fix with job stream check post talend                       #
# 4.5           Manick          Aug-2019                Ping Oracle db connection inside invoketalend to keep active    #
# 4.6           Manick          Aug-2019                Get os user who submitted the job and modify thread count for TD#
# 4.7           Manick          Aug-2019                Bug fix with looping when issue encountered during min/max      #
# 4.8           Manick          Aug-2019                Bug fix with date data type split for TD wrt format             #
# 4.9           Manick          Aug-2019                Update existing DIY_Master for retry                            #
# 4.10          Manick          Aug-2019                While querying JCT check for workflow type                      #		
# 4.11          Manick          Aug-2019                On retry check nopieces based on active flag                    #
#                                                       Include exception while logging err_msg in DIY_master           #
#                                                       Convert count(*) to bigint incase of TD                         #
# 4.12          Manick          Aug-2019                Bug on incremental ingestion of large batch remove unwanted and #
# 4.13          Manick          Aug-2019                Add incremental column as part of JCT where after split         #
# 4.14          Manick          Aug-2019                Scan for ACTIVE_IND = Y before querying JCT for table ingestion #
# 4.15          Manick          Aug-2019                Add -j to trigger ingestion based on job stream id              #
#                                                       Bug fix to include 'A' while checking for retry                 #
#                                                       Bug fix to handle None when source oracel tbl has no stats      #
# 4.16          Manick          Aug-2019                Bug fix with noPieces on retry of a single thread job           #
# 4.17          Manick          Sep-2019                Enable email notification                                       #
# 4.18          Manick          Sep-2019                Capture email address and audit of emails                       #
# 4.19          Manick          Sep-2019                Bug with multiple jcts for incremental                          #
# 4.20          Manick          Sep-2019                Use temp job group during ingestion and reset once it is done   #
# 4.21          Manick          Sep-2019                Use qualify to get latest stats in case of TD for rowcount      #
# 4.22          Manick          Sep-2019                Add additional argument to push data directly to SS/BR          #
# 4.23          Manick          Sep-2019                Bug with split logic with thread override                       #
# 4.24          Manick          Sep-2019                Enable varchar split for oracle if unique and use non-null cols #
# 4.25          Manick          Sep-2019                Disable job group only if it is history load                    #
# 4.26          Manick          Sep-2019                Bug fix with existing job group for single thread ingestion     #
# 4.27		Manick		Sep-2019		mkdir if doesnt exist for LOGS_DIR				#
# 4.28          Manick          Sep-2019                Bug fix with source column format type for date                 #
# 4.29          Manick          Sep-2019                Capture role as a parameter and use that role for SF ingestion  #
# 4.30          Manick          Sep-2019                Bug with ijobGroup      for job retry                           #
# 4.31          Manick          Sep-2019                Enable Varchar field in split column search for TD              #
# 4.32          Manick          Sep-2019                Bug fix on locating talend log file                             #
# 4.33          Manick          Sep-2019                While checking metadata check if datatype is not null           #
# 4.34          Manick          Sep-2019                Read source rows from table                                     #
# 4.35          Manick          Sep-2019                Launch STG2BR added in DIY master and lines                     #
# 4.36          Manick          Sep-2019                Enabled TIMESTAMP for oracle inc column and format to get data  #
# 4.37          Manick          Sep-2019                Exclude warehouse derivation for REPLICATION_POC_WH             #
# 4.38          Manick          Sep-2019                Bug with finding FATAL error from Talend exception              #
# 4.39          Manick          Sep-2019                Handle Varchar index column to make it distinct if collides     #
# 4.40          Manick          Sep-2019                Validate incremental column name & value and send msg if null   #
# 4.41          Manick          Sep-2019                bug fix with thread/split cnt and only non null cmn for split   #
# 4.42          Manick          Sep-2019                Bug fix with talend log between FF2SfSTG and TDSrc2FF/OraSrc2FF #
#                                                       Fix split step entry only for threadcount > 1                   #
# 4.43			Manick			Sep-2019				Bug fix with EDW_DATALAKE_WH while deriving WH from ROLE		#
#########################################################################################################################
import argparse
import snowflake.connector
import cx_Oracle
import teradata
import os, re, string, subprocess, sys
from datetime import datetime
from datetime import timedelta
import time
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from ConfigObject import ConfigObject
import faulthandler; faulthandler.enable()
import math
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib

def logdata(logf,msg,printflag):
	currTime=datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	if printflag == 1:
		print(currTime + " - " + msg)
	logf.write(currTime + " - " + msg + "\n")
	logf.flush()
	os.fsync(logf.fileno())
def validate_params(itype,pfile,tableName,envType,tCnt,jobStreamid,emailAdd,destSchema,mailonSucc):
	if (itype.lower() != 'db' and itype.lower() != 'file' and itype.lower() != 'table' and itype.lower() != 'jct'):
		print ("Invalid value for argument -t. It has to be either db (if source is another database) or file (if source is flat file) or table (previously ingested table) or jct")
		sys.exit(1)
	if (itype.lower() == 'db'):
		exists=os.path.isfile(pfile)
		if not exists:
			print ("Invalid parameter file - " + pfile + ". Please check for the existence of parameter file and reinvoke the program")
			sys.exit(2)
	if (itype.lower() == 'file'):
		exists=os.path.isfile(pfile)
		if not exists:
			print("Invalid data file - " + pfile + ". Please check for existence of source file and reinvoke the program")
			sys.exit(3)
	if(itype.lower() == 'table'):
		if tableName is None:
			print ("tableName is mandtory. Please provide tableName for which data was previously ingested")
			sys.exit(4)
		if envType is None:
			print ("Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type for table - " + tableName + " for which data was previously ingested")
			sys.exit(5)
		if tableName.count('.') <2:
			print ("Table name - " + tableName + " needs to be given in db.schema.tablename format. Please check and retry")
			sys.exit(6)
	if(itype.lower() == 'jct'):
		if jobStreamid is None:
			print("Job stream id is mandtory for type jct. Please provide JCT id which is present in repository")
			sys.exit(8)
		if envType is None:
			print ("Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type")
			sys.exit(9)
	if tCnt is not None:
		try:
			tCnt=int(tCnt)
		except:
			print("Invalid value for threadcount - " + tCnt + ". Thread count needs to be a integer if you want to override. Please check and retry")
			sys.exit(7)
	if emailAdd is None:
		print ("Email address is mandatory. Please provide the same using -a option (example -a mkumarap@cisco.com)")
		sys.exit(10)
	if destSchema is not None:
		if destSchema != 'SS' and destSchema !='BR':
			print("Invalid destination DB. Valid values are only SS and BR. Please check and retry")
			sys.exit(11)
	if mailonSucc is not None:
		if mailonSucc.upper() != 'YES' and mailonSucc.upper() != 'NO':
			print("Invalid value for -o mail on Success. Valid values are only Yes and No. Please check and retry")
			sys.exit(12)
def set_env():
	os.environ['LD_LIBRARY_PATH']="/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
	os.environ['ORACLE_HOME']="/usr/cisco/packages/oracle/oracle-12.1.0.2"
def conn_2_repos(ejcfile):
	fvar=open(ejcfile,'r')
	for line in fvar:
		if not line.strip().startswith("#") and len(line.strip())>0:
			name,value=line.split(';')
			ejcparams[name]=value.strip('\n')
			#print(name)
			#print(value)
	fvar.close()
	#print(ejcparams['EJC_PASSWORD'])
def invokeTalend(envType,Talend_Scr_Path,Talend_Scr,jobGroup,jobStreamarr,jobStreammstr,logDir,mstrlogFile,Thread_count,tblSize,cursor1):
	os.environ["RUN_ENVIRONMENT"]=envType
	#logFile=logDir + jobStream + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
	#oscmd=Talend_Scr_Path + Talend_Scr + " " + jobGroup + " " + jobStream + " >> " + logFile + " 2>&1 &"
	oscmd=Talend_Scr_Path + Talend_Scr
	monJobstream=jobStreamarr[0][:-2]
	osPrcmd="ps -ef | grep " + Talend_Scr + " | grep " +  monJobstream + " | grep -v grep | wc -l"
	processes = set()
	prcnt=0
	for loopcnt in range(len(jobStreamarr)):
		prcnt += 1
		currJobstr=jobStreamarr[loopcnt]
		logdata(mstrlogFile,"Now executing below OS Command",0)
		logdata(mstrlogFile,oscmd + " " + jobGroup + " " + currJobstr,0)
		outFile=logDir + currJobstr + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
		errFile=logDir + currJobstr + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
		outf=open(outFile,'w')
		errf=open(errFile,'w')
		processes.add(subprocess.Popen([oscmd,jobGroup,currJobstr,str(tblSize)],stdout=outf,stderr=errf))
		print ("Launched " + str(prcnt) + " processes out of " + str(len(jobStreamarr)) + " total process to submit")
		logFlag=True
		while len(processes) >= Thread_count:
			cnt=int(os.popen(osPrcmd).read())
			if logFlag:
				logdata(mstrlogFile,str(cnt) + " ingestion threads are active for " + monJobstream + " ingestion stream..",1)
				logFlag=False
			os.wait()
			time.sleep(60) ## 1 minute sleep before checking for process status
			cursor1.execute("select sysdate from dual"); ## Just to keep the oracle connective live for very long running ingestions
			processes.difference_update(
             [p for p in processes if p.poll() is not None])
		time.sleep(5) ## Sleeping 5 seconds between each parallel submission
	for p in processes:
		btchsleep=0
		totsleeptime=0
		while p.poll() is None:
			cnt=int(os.popen(osPrcmd).read())
			if btchsleep ==0 :
				logdata(mstrlogFile,str(cnt) + " ingestion threads are active for " + monJobstream + " ingestion stream..",1)
				btchsleep=1
			time.sleep(60)
			cursor1.execute("select sysdate from dual"); ## Just to keep the oracle connective live for very long running ingestions
			totsleeptime+=60
			if totsleeptime >= cnt*60:
				totsleeptime=0
				btchsleep=0
			#sleeptime=cnt*60
			#time.sleep(sleeptime)
			#p.wait()
	outf.close()
	errf.close()
def createDirs(jobGroup,envType,Job_Group_Init_Scr,logDir):
	os.environ["RUN_ENVIRONMENT"]=envType
	outFile=logDir + "JobGroupInit_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
	errFile=logDir + "JobGroupInit_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
	outf=open(outFile,'w')
	errf=open(errFile,'w')
	try:
		res=subprocess.Popen([Job_Group_Init_Scr,jobGroup],stdout=outf,stderr=errf)
		res.wait()
		rc=res.returncode
		outf.close()
		errf.close()
		return rc
	except OSError:
		outf.close()
		errf.close()
		return -1
def mergeSFstgtable(dbName,schemaName,tblName,noPieces,logFile,errFile,envType,srcNumrows,cursor1,emailAdd,sfRole,sfWh):
	logdata(logFile,"Now starting the SF merge process",0)
	if configFile.Config.PRDSUFF in envType:
		sfEnv = 'cisco.us-east-1'
		#sfkeepToken='s.D3TQWMYhSw6fPdirrdIgxeiR.2LCYK'
		sfkeepToken=configFile.Config.PRDTOKEN
		sfsecPah='secret/snowflake/prd/edw_datalake_svc/key'
		sfkeepUrl='https://east.keeper.cisco.com'
	elif configFile.Config.STGSUFF in envType:
		sfEnv = 'ciscostage.us-east-1'
		#sfkeepToken='s.Jfeidwfq80GIWfF0ionQsez5.VTsyF'
		sfkeepToken=configFile.Config.STGTOKEN
		sfsecPah='secret/snowflake/stg/edw_datalake_svc/key'
		sfkeepUrl='https://alphaeast.keeper.cisco.com'
	elif configFile.Config.DEVSUFF in envType:
		sfEnv = 'ciscodev.us-east-1'
		#sfkeepToken='s.w5mVkuMEpWwckxVlWWRFapLR.VTsyF'
		sfkeepToken=configFile.Config.DEVTOKEN
		sfsecPah='secret/snowflake/dev/edw_datalake_svc/key'
		sfkeepUrl='https://alphaeast.keeper.cisco.com'
	else:
		logdata(errFile,"Invalid envtype.. May be change in config.. Check and fix",1)
		errMsg="Invalid envtype.. May be change in config.. Check and fix"
		Subject="Error:" + errMsg
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		return -1
	sfUser=configFile.Config.sfUser
	#sfWh=configFile.Config.sfWh
	#sfkeepUrl=configFile.Config.sfkeepUrl
	sfkeepName=configFile.Config.sfkeepName
	#sfsecPah=configFile.Config.sfsecPah
	#sfRole=configFile.Config.sfRole
	try:
		# Connect to Keeper to collect secrets
		client = hvac.Client(
			url=sfkeepUrl,
			namespace=sfkeepName,
			token=sfkeepToken
		)
		# Secrets are stored within the key entitled 'data'
		keeper_secrets = client.read(sfsecPah)['data']
		passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
		private_key = keeper_secrets['private_key']

		# PEM key must be byte encoded
		key = bytes(private_key, 'utf-8')

		p_key = serialization.load_pem_private_key(
			key
			, password=passphrase.encode()
			, backend=default_backend()
		)

		pkb = p_key.private_bytes(
			encoding=serialization.Encoding.DER
			, format=serialization.PrivateFormat.PKCS8
			, encryption_algorithm=serialization.NoEncryption())

		conn3 = snowflake.connector.connect(
			user=sfUser
			, account=sfEnv
			, warehouse=sfWh
			, role=sfRole
			, database=dbName
			, schema=schemaName
			#, timezone=config.config_properties.timezone
			# , password=sfpass
			, private_key=pkb
			)
		cursor3=conn3.cursor()
	except  Exception as e:
		colval="Exception occurred while establing SF connection to " + sfEnv + " account as " + sfUser + ". Please check."
		updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
		updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
		logdata(errFile,colval,1)
		logdata(errFile,str(e),1)
		errMsg=colval
		Subject="Error:" + colval
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		return -1
	if noPieces == 1:
		try:
			logdata(logFile,"Stage table is single table.. Nothing to merge.. Hence Proceeding with count check",0)
			cursor3.execute("select count(*) from " + dbName + "." + schemaName + "." + tblName)
			results=cursor3.fetchall()
			for resultsObj in results:
				sfCnt = resultsObj[0]
			logdata(logFile,"Succesfully ingested " + str(sfCnt) + " rows into " + dbName + "." + schemaName + "." + tblName + " table.",1)
			return(sfCnt)
		except Exception as e:
			if conn3:
				conn3.close()
			colval="Exception occurred while checking target table rowcount."
			updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			logdata(errFile,colval,1)
			logdata(errFile,str(e),1)
			errMsg=colval
			Subject="Error:" + colval
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			return -1
	else:
		try:
			cursor3.execute("select count(*) from " + dbName + ".information_schema.tables where regexp_like(table_name,'" + tblName + "_[0-9]{1,}')  and table_schema='STG'");
			results=cursor3.fetchall()
			for resultsObj in results:
				sfChunkCnt = resultsObj[0]
			if sfChunkCnt != noPieces :
				raise Exception ("Snowflake stage has only " + str(sfChunkCnt) + " chunks while it is suppose to have " + str(noPieces))
			logdata(logFile,"Stage table has " + str(noPieces) + " chunks.. Hence proceeding towards merging the same..",0)
			cursor3.execute("select count(*) from " + dbName + ".information_schema.tables where table_name='" + tblName + "'" + " and table_schema='" + schemaName + "'")
			results=cursor3.fetchall()
			for resultsObj in results:
				sftblCnt = resultsObj[0]
			if sftblCnt == 1:
				logdata(logFile,"Target table " + tblName + " already exists on " + dbName + "." + schemaName + ". Hence proceeding with renaming the table",0)
				cursor3.execute('Alter table ' + tblName + ' rename to "' + tblName + '.bak.' + datetime.now().strftime('%Y%m%d%H%M%S') + '"')
			cursor3.execute("show warehouses like '" + sfWh + "'")
			cursor3.execute("select $4 whsize from table(result_scan(last_query_id()))")
			results=cursor3.fetchall()
			for resultsObj in results:
				sfwhSize = resultsObj[0]
			logdata(logFile,"Warehouse " + sfWh + " is originally of size " + sfwhSize,0)
			cursor3.execute("select bytes/(1024*1024*1024) from " + dbName + ".information_schema.tables  where table_name like '" + tblName + "_1'")
			results=cursor3.fetchall()
			for resultsObj in results:
				sftblChunkSize = resultsObj[0]
			sftblSize=sftblChunkSize*noPieces
			if sftblSize >= 100:
				newsfWhSize='2X-LARGE'
				resWhSize='XXLARGE'
			elif sftblSize >= 50:
				resWhSize='XLARGE'
				newsfWhSize='X-LARGE'
			else:
				resWhSize='LARGE'
				newsfWhSize='LARGE'
			if sfWh == 'EDW_DATALAKE_WH':
				if sfwhSize.upper() != newsfWhSize:
					logdata(logFile,"Since table " + tblName + " demands warehouse of size " + newsfWhSize + " proceeding with resizing the same",0)
					cursor3.execute("ALTER WAREHOUSE " + sfWh + " SET WAREHOUSE_SIZE = '" + resWhSize + "'")
			logdata(logFile,"Now proceeding with target table merge",1)
			cursor3.execute("Create table " + tblName + " as select * from " + tblName + "_1 where 1=2")
			cursor3.execute("alter session set autocommit=false")
			for i in range(noPieces):
				cursor3.execute("select count(*) from " + tblName + "_" + str(i))
				results=cursor3.fetchall()
				for cntObj in results:
					rowCnt=cntObj[0]
				cursor1.execute("update DIY_splits set row_count=" + str(rowCnt) + " where reqid=" + str(reqid) + " and split_id=" + str(i))
				cursor1.execute("commit")
				cursor3.execute("insert into " + tblName + " (select * from " + tblName + "_" + str(i) + ")")
			cursor3.execute("commit")
			logdata(logFile,"Succesfully merged all pieces into target table " + tblName,1)
			if sfWh == 'EDW_DATALAKE_WH':
				if sfwhSize.upper() != newsfWhSize:
					logdata(logFile,"Since table merge completed reverting back warehouse to its original size",0)
					cursor3.execute("ALTER WAREHOUSE " + sfWh + " SET WAREHOUSE_SIZE = '" + sfwhSize + "'")
			cursor3.execute("select count(*) from " + dbName + "." + schemaName + "." + tblName)
			results=cursor3.fetchall()
			for resultsObj in results:
				sfCnt = resultsObj[0]
			logdata(logFile,"Succesfully ingested " + str(sfCnt) + " rows into " + dbName + "." + schemaName + "." + tblName + " table.",1)
			if sfCnt >= srcNumrows:
				logdata(logFile,"Since snowflake rowcount matches or exceeds source count proceeding with dropping chunk tables",0)
				for i in range(noPieces):
					cursor3.execute("drop table " + tblName + "_" + str(i))
			if conn3:
				conn3.close()
			return(sfCnt)
		except Exception as e:
			cursor3.execute("rollback")
			if conn3:
				conn3.close()
			colval="Exception occurred while merging target table." 
			updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			logdata(errFile,colval,1)
			logdata(errFile,str(e),1)
			errMsg=colval
			Subject="Error:" + colval
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			return -1

def updTdInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile):
	logdata(logFile,"Now checking the data type for incremental column in source db",0)
	try:
		sql19="select decode(trim(columntype),'DA','DATE','I','ID','I8','ID','I1','ID','I2','ID','TS','DATE','TZ','DATE','INVALID') from "
		sql19=sql19+"dbc.columnsv where databasename='" + sourceSchema + "' and tablename='" + sourceTable + "' and columnname='" + incColname + "'"
		cursor2.execute(sql19)
		results=cursor2.fetchall()
		for incdTypeobj in results:
			incdType=incdTypeobj[0]
		if incdType == 'INVALID':
			logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
			return -1
		try:
			logdata(logFile,"Incremental column identified is of data type - " + incdType + ".Now extracting latest value from source",0)
			sql20="select max(" + incColname + ") from " + sourceSchema + "." + sourceTable
			cursor2.execute(sql20)
			results=cursor2.fetchall()
			for resIncvalueobj in results:
				resIncvalue=resIncvalueobj[0]
			logdata(logFile,"Max value of " + incColname + " in table " + sourceSchema + "." + sourceTable + " is " + str(resIncvalue),0)
			return(incdType + "~" + str(resIncvalue))
		except Exception as e:
			logdata(errFile,"Exception occurred while pull incremental column value.",1)
			logdata(errFile,str(e),1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception occurred while checking incremental column data type.",1)
		logdata(errFile,str(e),1)
		return -1

def updOracleInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile):
	logdata(logFile,"Now checking the data type for incremental column in source db",0)
	try:
		sql19="select decode(data_type,'NUMBER','ID','DATE','DATE','TIMESTAMP(3)','DATE','TIMESTAMP(6)','DATE','INVALID') from all_tab_columns where owner='" + sourceSchema + "' and table_name='" + sourceTable + "' and column_name='" + incColname + "'"
		cursor2.execute(sql19)
		results=cursor2.fetchall()
		for incdTypeobj in results:
			incdType=incdTypeobj[0]
		if incdType != 'DATE' and incdType !='NUMBER':
			logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
			return -1
		try:
			logdata(logFile,"Incremental column identified is of data type - " + incdType + ".Now extracting latest value from source",0)
			sql20="select max(to_date(to_char(" + incColname + ",'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS')) from " + sourceSchema + "." + sourceTable
			cursor2.execute(sql20)
			results=cursor2.fetchall()
			for resIncvalueobj in results:
				resIncvalue=resIncvalueobj[0]
			logdata(logFile,"Max value of " + incColname + " in table " + sourceSchema + "." + sourceTable + " is " + str(resIncvalue),0)
			return(incdType + "~" + str(resIncvalue))
		except Exception as e:
			logdata(errFile,"Exception occurred while pull incremental column value.",1)
			logdata(errFile,str(e),1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception occurred while checking incremental column data type.",1)
		logdata(errFile,str(e),1)
		return -1

def collectMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,TD_Metadata_Scr,ORCL_Metadata_Scr,logDir):
	logdata(logFile,"Now building parameter file",0)
	parFile='/tmp/' + sourceTable + '.conf'
	try:
		paramFile=open(parFile,'w')
		paramFile.write('ENVIRONMENT_NAME;' + envType + '\n')
		paramFile.write('CONNECTION_TYPE;' + sourcedbType.upper() + '\n')
		paramFile.write('DB_INSTANCE_NAME;' + sourceDb + '\n')
		paramFile.write('SOURCE_DB_CONNECTION;' + connectionName + '\n')
		paramFile.write('DATABASE_NAME;' + sourceSchema + '\n')
		paramFile.write('TABLE_NAME;' + sourceTable + '\n')
		BATCHID=datetime.now().strftime('%Y%m%d%H%M')
		paramFile.write('BATCH_ID;' + BATCHID + '\n')
		paramFile.close()
	except Exception as e:
		logdata(errFile,"Exception encountered while building parameter file for metadata collection",1)
		logdata(errFile,str(e),1)
		return -1
	if sourcedbType.upper() == "TERADATA":
		Scr_2_Run=TD_Metadata_Scr
	elif sourcedbType.upper() == "ORACLE":
		Scr_2_Run=ORCL_Metadata_Scr
	else:
		return -1
	logdata(logFile,"Now launching talend workflow for collecting missing metadata for table - " + sourceTable,1)
	try:
		outFile=logDir + "metadata_" + sourceTable + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
		errFile=logDir + "metadata_" + sourceTable + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
		outf=open(outFile,'w')
		errf=open(errFile,'w')
		processes = set()
		inpC="--context_param ParamFileName="
		processes.add(subprocess.Popen([Scr_2_Run,"--context_param","ParamFileName=" + parFile],stdout=outf,stderr=errf))
		#processes.add(subprocess.Popen([Scr_2_Run,inpC,parFile],stdout=outf,stderr=errf))
		for p in processes:
			p.wait()
		outf.close()
		errf.close()
	except Exception as e:
		logdata(errFile,"Exception encountered while launching process for metadata collection",1)
		logdata(errFile,str(e),1)
		return -1
def updDIYmstr(reqid,updcol,colval,cursor1,errFile,logFile,emailAdd):
	try:
		if colval =='curr_date':
			updSql="update DIY_master set " + updcol + "=sysdate,last_update_time = sysdate where reqid=" + str(reqid)
		else:
			colval=colval.replace("'","''")
			updSql="update DIY_master set " + updcol + "='" + str(colval) + "',last_update_time = sysdate where reqid=" + str(reqid)
		cursor1.execute(updSql)
		cursor1.execute("commit")
	except Exception as e:
		logdata(errFile,"Exception encountered while updating DIY Master",1)
		logdata(errFile,str(e),1)
		logdata(errFile,"Command executed is -" + updSql,0)
		Subject ="Error: Exception encountered while updating DIY Master"
		errMsg=Subject + "\n\n" + str(e)
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
def insDIYlines(reqid,prephase,currphase,cursor1,errFile,emailAdd):
	try:
		insqry="insert into DIY_lines(reqid,step_name,step_start_time) values "
		insqry=insqry + "(" + str(reqid) + ",'" + currphase + "',sysdate)" 
		cursor1.execute(insqry)
		cursor1.execute("update DIY_lines set step_end_time=sysdate where reqid=" + str(reqid) + " and step_name='" + prephase + "'")
		cursor1.execute("commit")
	except Exception as e:
		logdata(errFile,"Exception encountered while inserting/updating DIY lines",1)
		logdata(errFile,str(e),1)
		Subject ="Error: Exception encountered while updating DIY lines"
		errMsg=Subject + "\n\n" + str(e)
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
def sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1):
	logdata(logFile,"Sending email to " + emailAdd + " with status - " + errMsg,0)
	msg=MIMEMultipart()
	message=errMsg
	body = MIMEText(message)
	msgfrom='sf-smart-ingestiony@cisco.com'
	tolist=emailAdd
	msg['Subject'] = Subject
	msg['From']    = msgfrom
	msg['To']      = tolist
	if os.stat(logFile.name).st_size > 0:
		file=logFile.name
		#msg.attach(MIMEText("Logfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Logfile.txt')
		msg.attach(attachment)

	if os.stat(errFile.name).st_size > 0:
		file=errFile.name
		#msg.attach(MIMEText("Errorfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Errorfile.txt')
		msg.attach(attachment)
	msg.attach(body)
	s=smtplib.SMTP("outbound.cisco.com")
	s.sendmail(msgfrom,tolist.split(","),msg.as_string())
	s.quit()
	try:
		inssql="insert into DIY_email_logs (reqid,email_address,subject,errmsg,logfile,errfile,sent_time) values "
		errMsg=errMsg.replace("'","''")
		inssql=inssql + "(" + str(reqid) + ",'" + emailAdd + "','" + Subject + "','" + errMsg + "','" + logFile.name + "','" + errFile.name + "',sysdate)"
		cursor1.execute(inssql)
		cursor1.execute('commit')
	except Exception as e:
		logdata(errFile,"Exception encountered while inserting/updating Email Log",1)
		logdata(errFile,str(e),1)
		Subject ="Error: Exception encountered while updating DIY lines"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)


def sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject):
	logdata(logFile,"Sending email to " + emailAdd + " with status - " + errMsg,0)
	msg=MIMEMultipart()
	message=errMsg
	body = MIMEText(message)
	msgfrom='sf-smart-ingestion@cisco.com'
	tolist=emailAdd
	msg['Subject'] = Subject
	msg['From']    = msgfrom
	msg['To']      = tolist
	if os.stat(logFile.name).st_size > 0:
		file=logFile.name
		#msg.attach(MIMEText("Logfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Logfile.txt')
		msg.attach(attachment)

	if os.stat(errFile.name).st_size > 0:
		file=errFile.name
		#msg.attach(MIMEText("Errorfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Errorfile.txt')
		msg.attach(attachment)
	msg.attach(body)
	s=smtplib.SMTP("outbound.cisco.com")
	s.sendmail(msgfrom,tolist.split(","),msg.as_string())
	s.quit()

def validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,TD_Metadata_Scr,ORCL_Metadata_Scr,logDir):
	logdata(logFile,"Now validating metadata to make sure all source table columns exists in repository",0)
	if sourcedbType.upper() == "TERADATA":
		try:
			if 'VWDB' in sourceSchema.upper():
				logdata(logFile,"Pulling 3NF for vw schema",0)
				sql13 = " select distinct db_schema_name from EDS_DATA_CATALOG.EDW_TD_VWDB_MAPPING where view_schema_name='" + sourceSchema + "'"
				cursor1.execute(sql13)
				results=cursor1.fetchall()
				for vwcheckobj in results:
					sourceSchema=vwcheckobj[0]
				logdata(logFile,"Found base schema as " + sourceSchema,0)
		except Exception as e:
			#print ("Error occurred while checking base table from 1-1 view")
			logdata(errFile,"Error occurred while checking table schema from view schema",1)
			#print(str(e))
			logdata(errFile,str(e),1)
			return -1
	try:
		sql1="select count(*) from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + sourceDb + "' and db_schema_name='" +  sourceSchema
		sql1=sql1+ "' and table_name='" + sourceTable + "' and environment_name='" + envType + "' and data_type is not null"
		cursor1.execute(sql1)
		results=cursor1.fetchall()
		for resMetobj in results:
			resMetcnt=resMetobj[0]
		if resMetcnt == 0:
			#return -1
			logdata(logFile,"Now attempting to collect metadata for table - " + sourceTable,0)
			collectMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,TD_Metadata_Scr,ORCL_Metadata_Scr,logDir)
			logdata(logFile,"Post collecting metadata checking for metadata in repos",0)
			try:
				cursor1.execute(sql1)
				results=cursor1.fetchall()
				for resMetobj in results:
					resMetcnt=resMetobj[0]
				if resMetcnt == 0:
					logdata(errFile,"Even though attempted to collect metadata, collect metadata failed.. Please check and take action",1)
					return -1
				else:
					return resMetcnt
			except Exception as e:
				logdata(errFile,"Exception encountered while verifying metadata existence for table post collection- " + sourceTable,1)
				logdata(errFile,"SQL query ran was " + sql1,1)
				logdata(errFile,str(e),1)
				return -1
		else:
			return resMetcnt
	except Exception as e:
		logdata(errFile,"Exception encountered while verifying metadata existence for table pre collection - " + sourceTable,1)
		logdata(errFile,"SQL query ran was " + sql1,1)
		logdata(errFile,str(e),1)
		return -1
#
## Main program starts here
#
params=[]
connstr2=''
ejcparams={}
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
reqid=str(ftime)
rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
#Talend_Scr_Path=configFile.Config.Talend_Scr_Path
Talend_Base=configFile.Config.Talend_Base
Talend_Scr_Path=Talend_Base + 'scripts/shell/'
TD_Metadata_Scr=Talend_Base + 'modules/GetTeradataMetadata/GetTeradataMetadata_run.sh'
ORCL_Metadata_Scr=Talend_Base + 'modules/GetOracleOtherMetadata/GetOracleOtherMetadata_run.sh'
Job_Group_Init_Scr=configFile.Config.Job_Group_Init_Scr
#Talend_Scr=configFile.Config.Talend_Scr
Talend_Scr='SRC2STG_TPT.sh'
Anal_Threshold=int(configFile.Config.Anal_Threshold)
Split_Threshold=int(configFile.Config.Split_Threshold)
Split_Orcl_Threshold=int(configFile.Config.Split_Orcl_Threshold)
Chunk_Orcl_Threshold=int(configFile.Config.Chunk_Orcl_Threshold)
Thread_count=int(configFile.Config.Thread_count)
Split_Lob_Threshold=int(configFile.Config.Split_Lob_Threshold)
Thread_Lob_Count=int(configFile.Config.Thread_Lob_Count)
retryFlag='FALSE'
jctCnt=0
tblSize=1
split4upd=1
parser = argparse.ArgumentParser(description='Script to Peform data ingestion into Snowflake',
    epilog='Example: python DIY.py -i ingestion_type (db/file/table/jct) -f param_file -t sourcedb.schema.tablename -e env_type -c thread_count -s split_count -n split_column -j job_stream_id -a email -r sf_role_to_use -o mail_on_success -b customJctId')
parser.add_argument('-i', '--type',required=True,
    help='What is the source for ingestion. Another database or a flat file or already ingested table ')
parser.add_argument('-f','--file',
	help='Full name of parameter file with required details for ingestion')
parser.add_argument('-t','--table',
	help='Name of already ingested sourcedb.schema.tablename')
parser.add_argument('-e','--env',
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-c','--threadcnt',
	help='Thread count if you want to override program settings')
parser.add_argument('-s','--splitcnt',
	help='Split count if you want to override program settings')
parser.add_argument('-n','--splitcmn',
	help='Split column if you want to override program settings')
parser.add_argument('-j','--jobstreamid',
	help='Existing job stream id which you want to ingest')
parser.add_argument('-a','--email',required=True,
	help='Comma seperated email list to which notification needs to be sent out on ingestion status')
parser.add_argument('-d','--schema',required=True,
	help='Destination Schema in Snowflake. Valid values are SS and BR')
parser.add_argument('-r','--role',
	help='Snowflake role to use. By default existing role from JCT will be used, incase of new JCT EDW_DATALAKE_ROLE will be used')
parser.add_argument('-o','--mailOnSucc',
	help='Send an Email on succesful completion. Valid values are Yes/No')
parser.add_argument('-b','--custJct',
	help='Custom JCT ID if you want to have one of your wish.')
args=parser.parse_args()
itype=args.type
pfile=args.file
tableName=args.table
envType=args.env
tCnt=args.threadcnt
sCnt=args.splitcnt
sCmn=args.splitcmn
jobStreamid=args.jobstreamid
emailAdd=args.email
destSchema=args.schema
sfRole=args.role
mailonSucc=args.mailOnSucc
custJct=args.custJct
talendbase=BASE_DIR + '/talend'
tesbase=talendbase + '/TES'
try:
	if not os.path.exists(os.path.dirname(LOGS_DIR)):
		os.mkdir(LOGS_DIR)
	else:
		rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
		LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
		os.mkdir(LOGS_DIR)
except Exception as e:
	time.sleep(1)
	rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
	LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
	os.mkdir(LOGS_DIR)
	pass
logFile=open(LOGS_DIR + 'main.log','w')
errFile=open(LOGS_DIR + 'main.err','w')
set_env()
ijobGroup='INGESTION_GROUP'
logdata(logFile,'Validating Parameters',0)
validate_params(itype,pfile,tableName,envType,tCnt,jobStreamid,emailAdd,destSchema,mailonSucc)
if tCnt is None:
	tCnt=Thread_count
else:
	Thread_count=int(tCnt)
if itype.lower() == 'table':
	logdata(logFile,'Ingesting data for existing JCT table',0)
	ejcfile=tesbase + '/' + envType + '/ejc'
	exists=os.path.isfile(ejcfile)
	if not exists :
		logdata(errFile,"EJC file - " + ejcfile + ' could not be located.. Please check and retry',1)
		errMsg="EJC file - " + ejcfile + ' could not be located.. Please check and retry'
		Subject="Error:" + errMsg
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
		#print ("EJC file - " + ejcfile + ' could not be located.. Please check and retry')
		sys.exit(7)
	fvar=open(ejcfile,'r')
	logdata(logFile,'Connecting to repository db',0)
	conn_2_repos(ejcfile)
	try:
		connstr = cx_Oracle.makedsn(ejcparams['EJC_HOST'], ejcparams['EJC_PORT'],service_name=ejcparams['EJC_SERVICENAME'])
		conn = cx_Oracle.connect(user=ejcparams['EJC_LOGIN'], password=ejcparams['EJC_PASSWORD'], dsn=connstr)
	except Exception as e:
		#print ("Error occurred while connecting to Repository database")
		logdata(errFile,"Error occurred while connecting to Repository database",1)
		#print(str(e))
		logdata(errFile,str(e),1)
		Subject ="Error:" + "Error occurred while connecting to Repository database"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
	tblDict=tableName.split('.')
	cursor1=conn.cursor()
	logdata(logFile,'Validating existing JCT entry',0)
	sql = "SELECT '" + envType + "',job_group_id,decode(source_db_name,'TDPROD','TERADATA','ORACLE'),source_db_name,source_schema,source_table_name,target_db_name,"
	sql = sql + "target_schema,target_table_name,source_db_connection,incremental_column_Name,nvl(where_clause,''),job_stream_id,sf_role,sf_warehouse from ( SELECT * FROM EDW_JOB_STREAMS where workflow_type='SRC2STG' "
	sql = sql + " and source_db_name='" + tblDict[0] + "' and source_schema='" + tblDict[1] + "' and source_table_name='" + tblDict[2] + "' and active_ind='Y'"
	sql = sql + " order by length(job_stream_id)) where rownum=1"
	#print ("SQL is - " + sql)
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) != 1:
		#print ("Invalid source table - " + tableName + " for ingestion. Existing JCT entry with this table not found in repository. Please check and retry")
		logdata(errFile,"Invalid source table - " + tableName + " for ingestion. Either JCT entry with this table not found or multiple entries found in repository. Please check and retry",1)
		Subject ="Error: Invalid source table - " + tableName + " for ingestion. Either JCT entry with this table not found or multiple entries found in repository. Please check and retry"
		errMsg=Subject
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		sys.exit(9)
	else:
		#print ("length of resultset is " + str(len(results)))
		#i=0
		logdata(logFile,'Building object array',0)
		for resObj in results:
			#print("resobj is " + str(resObj))
			paramarr=''
			paramarr=resObj[0]
			paramarr = paramarr + '~' + resObj[1]
			paramarr = paramarr + '~' + resObj[2]
			paramarr = paramarr + '~' + resObj[3]
			paramarr = paramarr + '~' + resObj[4]
			paramarr = paramarr + '~' + resObj[5]
			paramarr = paramarr + '~' + resObj[6]
			paramarr = paramarr + '~' + resObj[7]
			paramarr = paramarr + '~' + resObj[8]
			paramarr = paramarr + '~' + resObj[9]
			if resObj[10] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[10]
			if resObj[11] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[11]
			params.append(paramarr.split('~'))
			jobStreamid=resObj[12]
			if sfRole is None:
				sfRole=resObj[13]
			sfWh=resObj[14]
			#i+=i
		#print(params)
		#sys.exit(0)
# End of indentation for ingestion for existing table		
if itype.lower() == 'jct':
	logdata(logFile,'Ingesting data for existing JCT entry',0)
	ejcfile=tesbase + '/' + envType + '/ejc'
	exists=os.path.isfile(ejcfile)
	if not exists :
		logdata(errFile,"EJC file - " + ejcfile + ' could not be located.. Please check and retry',1)
		Subject ="Error: EJC file - " + ejcfile + ' could not be located.. Please check and retry'
		errMsg=Subject
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
		#print ("EJC file - " + ejcfile + ' could not be located.. Please check and retry')
		sys.exit(7)
	fvar=open(ejcfile,'r')
	logdata(logFile,'Connecting to repository db',0)
	conn_2_repos(ejcfile)
	try:
		connstr = cx_Oracle.makedsn(ejcparams['EJC_HOST'], ejcparams['EJC_PORT'],service_name=ejcparams['EJC_SERVICENAME'])
		conn = cx_Oracle.connect(user=ejcparams['EJC_LOGIN'], password=ejcparams['EJC_PASSWORD'], dsn=connstr)
	except Exception as e:
		#print ("Error occurred while connecting to Repository database")
		logdata(errFile,"Error occurred while connecting to Repository database",1)
		#print(str(e))
		logdata(errFile,str(e),1)
		Subject ="Error: Error occurred while connecting to Repository database"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
	#tblDict=tableName.split('.')
	cursor1=conn.cursor()
	logdata(logFile,'Validating existing JCT entry',0)
	sql = "SELECT '" + envType + "',job_group_id,decode(source_db_name,'TDPROD','TERADATA','ORACLE'),source_db_name,source_schema,source_table_name,target_db_name,"
	sql = sql + "target_schema,target_table_name,source_db_connection,incremental_column_Name,nvl(where_clause,''),sf_role,sf_warehouse from ( SELECT * FROM EDW_JOB_STREAMS where workflow_type='SRC2STG' "
	sql = sql + " and active_ind='Y' and job_stream_id='" + jobStreamid + "')"
	#print ("SQL is - " + sql)
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) != 1:
		#print ("Invalid source table - " + tableName + " for ingestion. Existing JCT entry with this table not found in repository. Please check and retry")
		logdata(errFile,"Invalid JCT - " + jobStreamid + " for ingestion. Either JCT entry not found in repository or not in active status. Please check and retry",1)
		Subject ="Error: Invalid JCT - " + jobStreamid + " for ingestion. Either JCT entry not found in repository or not in active status. Please check and retry"
		errMsg=Subject
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		sys.exit(9)
	else:
		#print ("length of resultset is " + str(len(results)))
		#i=0
		logdata(logFile,'Building object array',0)
		for resObj in results:
			#print("resobj is " + str(resObj))
			paramarr=''
			paramarr=resObj[0]
			paramarr = paramarr + '~' + resObj[1]
			paramarr = paramarr + '~' + resObj[2]
			paramarr = paramarr + '~' + resObj[3]
			paramarr = paramarr + '~' + resObj[4]
			paramarr = paramarr + '~' + resObj[5]
			paramarr = paramarr + '~' + resObj[6]
			paramarr = paramarr + '~' + resObj[7]
			paramarr = paramarr + '~' + resObj[8]
			paramarr = paramarr + '~' + resObj[9]
			if resObj[10] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[10]
			if resObj[11] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[11]
			params.append(paramarr.split('~'))
			if sfRole is None:
				sfRole=resObj[12]
			sfWh=resObj[13]
			#i+=i
		#print(params)
		#sys.exit(0)
# End of indentation for ingestion for existing job stream		
if itype.lower() == 'db':
	logdata(logFile,'Ingesting data from source database',0)
	vfile=open(pfile,"r")
	for line in vfile:
		if not line.strip().startswith("#"):
			if len(line.strip())>0:
				params.append(line.split('~'))
	vfile.close()
	logdata(logFile,'Building object array',0)
	for i in range(len(params)):
		for j  in range(len(params[i])-2):
			if params[i][j]=="":
				#print ("Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one")
				logdata(errFile,"Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one",1)
				Subject ="Error: Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				sys.exit(8)
	if sfRole is None:
		sfRole='EDW_DATALAKE_ROLE'
		sfWh='EDW_DATALAKE_WH'
# End of indentation for ingestion from source db
if itype.lower() == 'db' or itype.lower() == 'table' or itype.lower()=='jct':
	logdata(logFile,'Inside main loop',0)
	tblCntvar=0
	eType='ALL_DATA'
	if sfRole is None:
		sfRole='EDW_DATALAKE_ROLE'
		sfWh='EDW_DATALAKE_WH'
	else:
		if sfWh != 'EDW_DATALAKE_WH' and sfWh != 'REPLICATION_POC_WH' and sfRole != 'EDW_DATALAKE_ROLE':
			whSuffix=sfRole.replace('EDW_','').replace('_ETL_ROLE','')
			sfWh='EDW_I_' + whSuffix + '_WH'

	for i in range(len(params)):
		reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
		envType=params[i][0]
		SuccFlag=False
		stg2brFail=False
		wfType='SRC2STG'
		existFlag='FALSE'
		retryFlag='FALSE'
		spaceCheck=False
		lastUpdColFlag=False
		if sCnt is None:
			threadcount=1
			threadoverride=False
		else:
			try:
				threadcount=int(sCnt)
			except:
				print("Invalid value for Splitcount - " + sCnt + ". Split count needs to be a integer if you want to override. Please check and retry")
				sys.exit(8)
			threadoverride=True
		stageMnt='/apps/edwsfdata/' + envType
		jobGroup=params[i][1].strip()
		sourcedbType=params[i][2].strip()
		sourceDb=params[i][3].strip()
		sourceSchema=params[i][4].strip()
		sourceTable=params[i][5].strip()
		targetDb=params[i][6].strip()
		targetSchema=params[i][7].strip()
		targetTable=params[i][8].strip()
		connectionName=params[i][9].strip()
		incColname=params[i][10].strip()
		whereClause=params[i][11].strip('\n')
		if jobStreamid is None:
			if custJct is not None:
				jobStream=custJct
			else:
				jobStream = 'JOB_' + wfType + '_' + sourceDb + '_' + sourceTable
		else:
			jobStream=jobStreamid
		#if sourcedbType.upper() == 'ORACLE':
		#	Talend_Scr='SRC2STG_ORCL.sh'
		#elif sourcedbType.upper() == 'TERADATA':
		#	Talend_Scr='SRC2STG_TPT.sh'
		logdata(logFile,'Processing ingestion for - ' + jobStream,0)
		#logdata(errFile,'Processing ingestion for - ' + jobStream,0)
		if itype.lower() == 'db':
			ejcfile=tesbase + '/' + envType + '/ejc'
			exists=os.path.isfile(ejcfile)
			if not exists :
				logdata(errFile,"EJC file - " + ejcfile + ' could not be located.. Please check and retry',1)
				Subject ="Error: EJC file - " + ejcfile + ' could not be located.. Please check and retry'
				errMsg=Subject 
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				#print ("EJC file - " + ejcfile + ' could not be located.. Please check and retry')
				continue
			#print ("Now triggering data ingestion for " + sourceDb + "." + sourceSchema + "." + sourceTable)	
			logdata(logFile,"Now triggering data ingestion for " + sourceDb + "." + sourceSchema + "." + sourceTable,1)
			conn_2_repos(ejcfile)
			try:
				connstr = cx_Oracle.makedsn(ejcparams['EJC_HOST'], ejcparams['EJC_PORT'],service_name=ejcparams['EJC_SERVICENAME'])
				conn = cx_Oracle.connect(user=ejcparams['EJC_LOGIN'], password=ejcparams['EJC_PASSWORD'], dsn=connstr)
			except Exception as e:
				#print ("Error occurred while connecting to Repository database")
				logdata(errFile,"Error occurred while connecting to Repository database",1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Error occurred while connecting to Repository database"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		try:
			srcdbParams={}
			cursor1=conn.cursor()
			logdata(logFile,'Gathering source db connection detais from repository',0)
			sql1="select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and upper(parameter_category)='" + sourcedbType.upper() + "' and parameter_type ='" + connectionName + "'"
			#print(sql1)
			cursor1.execute(sql1)
			results=cursor1.fetchall()
			if len(results) == 0:
				#print ("Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion")
				logdata(errFile,"Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion",1)
				Subject ="Error: Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			for obj in results:
				param=obj[0]
				value=obj[1]
				srcdbParams[param]=value
			#print(srcdbParams)
		except  Exception as e:
			#print("Exception occurred while selecting source db connection details")
			logdata(errFile,"Exception occurred while selecting source db connection details",1)
			#print(str(e))
			logdata(errFile,str(e),1)
			Subject ="Error: Exception occurred while selecting source db connection details"
			errMsg=Subject + "\n\n" + str(e)
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		try:
			sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
			cursor1.execute(sql2)
			results=cursor1.fetchall()
			for passobj in results:
				srcPassword=passobj[0]
		except Exception as e:
			#print ("Exception occurred while decoding source login password")
			logdata(errFile,"Exception occurred while decoding source login password",1)
			#print(str(e))
			logdata(errFile,str(e),1)
			Subject ="Error: Exception occurred while decoding source login password"
			errMsg=Subject + "\n\n" + str(e)
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			continue
		try:
			logdata(logFile,"Checking for existing JOb group entry for Job group - " + jobGroup,0)
			sql17="Select count(*) from edw_job_groups where job_group_id='" + jobGroup + "'"
			cursor1.execute(sql17)
			results=cursor1.fetchall()
			for resCntobj in results:
				resCnt=resCntobj[0]
			if resCnt == 0:
				logdata(logFile,"Missing job group - " + jobGroup + " for " + envType + ". Hence creating it now",1)
				sql18="insert into edw_job_groups values ('" + jobGroup + "','" + jobGroup + "','Y','P','created through API for ingesting " + jobStream + "',NULL,'Y','A',sysdate,'API',sysdate,'API')"
				cursor1.execute(sql18)
				cursor1.execute('commit')
				logdata(logFile,"Now invoking shell script to create directory structure required for ingestion",0)
				resDir=createDirs(jobGroup,envType,Talend_Scr_Path + Job_Group_Init_Scr,LOGS_DIR)
				if resDir != 0:
					logdata(errFile,"Creating job group directories on host failed.. Hence aborting ingestion for jobstream " + jobStream,1)
					Subject ="Error: Creating job group directories on host failed.. Hence aborting ingestion for jobstream " + jobStream
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'checkpoint').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'code').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'config/archive').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'data/inbox').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'data/archive').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'logs/archive').read()
		except Exception as e:
			logdata(errFile,"Exception occurred while making new job group entry.. Hence aborting ingestion for jobstream " + jobStream,1)
			#print(str(e))
			logdata(errFile,str(e),1)
			Subject ="Error: Exception occurred while making new job group entry.. Hence aborting ingestion for jobstream " + jobStream
			errMsg=Subject + "\n\n" + str(e)
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			continue
		try:
			logdata(logFile,"Checking for existing Ingestion JOb group entry for Job group - " + ijobGroup,0)
			sql17="Select count(*) from edw_job_groups where job_group_id='" + ijobGroup + "'"
			cursor1.execute(sql17)
			results=cursor1.fetchall()
			for resCntobj in results:
				resCnt=resCntobj[0]
			if resCnt == 0:
				logdata(logFile,"Missing job group - " + ijobGroup + " for " + envType + ". Hence creating it now",1)
				sql18="insert into edw_job_groups values ('" + ijobGroup + "','" + ijobGroup + "','Y','P','created through API',NULL,'Y','A',sysdate,'API',sysdate,'API')"
				cursor1.execute(sql18)
				cursor1.execute('commit')
				logdata(logFile,"Now invoking shell script to create directory structure required for ingestion",0)
				resDir=createDirs(ijobGroup,envType,Talend_Scr_Path + Job_Group_Init_Scr,LOGS_DIR)
				if resDir != 0:
					logdata(errFile,"Creating job group directories on host failed.. Hence aborting ingestion for jobstream " + jobStream,1)
					Subject ="Error: Creating job group directories on host failed.. Hence aborting ingestion for jobstream " + jobStream
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'checkpoint').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'code').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'config/archive').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'data/inbox').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'data/archive').read()
			out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'logs/archive').read()
		except Exception as e:
			logdata(errFile,"Exception occurred while making ingestion job group entry.. Hence aborting ingestion for jobstream " + jobStream,1)
			#print(str(e))
			logdata(errFile,str(e),1)
			Subject ="Error: Exception occurred while making ingestion job group entry.. Hence aborting ingestion for jobstream " + jobStream
			errMsg=Subject + "\n\n" + str(e)
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			continue
		try:
			logdata(logFile,"Checking for existing JCT entry with job_stream_id - " + jobStream,0)
			#sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "%' and RUN_STATUS in ('R')"
			sql3="SELECT count(*) from EDW_JOB_STREAMS where (JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS in ('R','A'))"
			sql3=sql3 + " or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and RUN_STATUS in ('R','P')) and workflow_type='SRC2STG' and active_ind='Y'"
			cursor1.execute(sql3)
			results=cursor1.fetchall()
			for cntObj in results:
				jobCnt=cntObj[0]
			if jobCnt > 0:
				#print("Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now")
				logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now",1)
				retryFlag='TRUE'
				sql33="select max(reqid) from DIY_MASTER where source_table='" + sourceTable + "' and source_db_name='" + sourceDb + "' and source_schema='" + sourceSchema + "'"
				cursor1.execute(sql33)
				results=cursor1.fetchall()
				for maxreqidObj in results:
					reqid=maxreqidObj[0]
				if len(results) == 0 or reqid is None:
					reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
					retflg='F'
					currstat='INIT'
					currOsUser=os.getlogin()
					currHostName=os.uname()[1]
					if threadcount == 1:
						tCnt=1
					sqlins="insert into DIY_master(reqid,Ingestion_type,logs_dir,param_file,env_type,job_group,source_db_name,source_db_type,source_schema,source_table,target_schema,"
					sqlins=sqlins + "target_table,Thread_count,split_count,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4) values "
					sqlins=sqlins + " (" + reqid + ","
					sqlins=sqlins + "'" + itype.upper() + "',"
					sqlins=sqlins + "'" + LOGS_DIR + "',"
					if pfile is not None:
						sqlins=sqlins + "'" + pfile + "',"
					else:
						sqlins=sqlins + " NULL,"
					sqlins=sqlins + "'" + envType.upper() + "',"
					sqlins=sqlins + "'" + jobGroup + "',"
					sqlins=sqlins + "'" + sourceDb + "',"
					sqlins=sqlins + "'" + sourcedbType + "',"
					sqlins=sqlins + "'" + sourceSchema + "',"
					sqlins=sqlins + "'" + sourceTable + "',"
					sqlins=sqlins + "'" + targetSchema + "',"
					sqlins=sqlins + "'" + targetTable + "',"
					sqlins=sqlins + str(tCnt) + ","
					sqlins=sqlins + str(threadcount) + ","
					#sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "')"
					sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + jobStream + "','" + currHostName + "')"
					try:
						cursor1.execute(sqlins)
						cursor1.execute('commit')
					except Exception as e:
						logdata(errFile,"Exception occurred while inserting DIY master record",1)
						logdata(errFile,sqlins,0)
						logdata(errFile,str(e),1)
						Subject ="Error: Exception occurred while inserting DIY master record"
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
		except  Exception as e:
			#print ("Exception occurred while checking job control run status")
			logdata(errFile,"Exception occurred while checking job control run status",1)
			#print(str(e))
			logdata(errFile,str(e),1)
			Subject ="Error: Exception occurred while checking job control run status"
			errMsg=Subject + "\n\n" + str(e)
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			continue
		if retryFlag=="TRUE":
			retflg='T'
			currstat='Split_Phase'
			currOsUser=os.getlogin()
			currHostName=os.uname()[1]
			if threadcount == 1:
				tCnt=1
			sqlupd="update DIY_master set logs_dir='" + LOGS_DIR + "',job_group='" + jobGroup + "',target_schema='" + targetSchema + "',target_table='" + targetTable + "'"
			sqlupd=sqlupd + ",Thread_count=" + str(tCnt)  + ",retryflag='T',attribute1='" + currOsUser + "',err_msg=NULL,attribute2='" + emailAdd + "'"
			sqlupd=sqlupd + ",attribute4='" + currHostName + "' where reqid=" + str(reqid)
			try:
				cursor1.execute(sqlupd)
				cursor1.execute('commit')
			except Exception as e:
				logdata(errFile,"Exception occurred while updating DIY master record",1)
				logdata(errFile,sqlupd,0)
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while updating DIY master record"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
		else:
			retflg='F'
			currstat='INIT'
			currOsUser=os.getlogin()
			currHostName=os.uname()[1]
			if threadcount == 1:
				tCnt=1
			sqlins="insert into DIY_master(reqid,Ingestion_type,logs_dir,param_file,env_type,job_group,source_db_name,source_db_type,source_schema,source_table,target_schema,"
			sqlins=sqlins + "target_table,Thread_count,split_count,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4) values "
			sqlins=sqlins + " (" + reqid + ","
			sqlins=sqlins + "'" + itype.upper() + "',"
			sqlins=sqlins + "'" + LOGS_DIR + "',"
			if pfile is not None:
				sqlins=sqlins + "'" + pfile + "',"
			else:
				sqlins=sqlins + " NULL,"
			sqlins=sqlins + "'" + envType.upper() + "',"
			sqlins=sqlins + "'" + jobGroup + "',"
			sqlins=sqlins + "'" + sourceDb + "',"
			sqlins=sqlins + "'" + sourcedbType + "',"
			sqlins=sqlins + "'" + sourceSchema + "',"
			sqlins=sqlins + "'" + sourceTable + "',"
			sqlins=sqlins + "'" + targetSchema + "',"
			sqlins=sqlins + "'" + targetTable + "',"
			sqlins=sqlins + str(tCnt) + ","
			sqlins=sqlins + str(threadcount) + ","
			#sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "')"
			sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + jobStream + "','" + currHostName + "')"
			try:
				cursor1.execute(sqlins)
				cursor1.execute('commit')
			except Exception as e:
				logdata(errFile,"Exception occurred while inserting DIY master record",1)
				logdata(errFile,sqlins,0)
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while inserting DIY master record"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
		if retryFlag !='TRUE':
			try:
				logdata(logFile,"Checking for pending JCT entry with job_stream_id - " + jobStream,0)
				sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS in ('P','A') and workflow_type='SRC2STG' and active_ind='Y'"
				cursor1.execute(sql3)
				results=cursor1.fetchall()
				for cntObj in results:
					jobCnt=cntObj[0]
				if jobCnt > 0:
					#print("Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now")
					logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under Pending status.. Hence triggering ingestion now",1)
					existFlag='TRUE'
					ijobGroup=jobGroup
			except  Exception as e:
				#print ("Exception occurred while checking job control run status")
				logdata(errFile,"Exception occurred while checking job control pending status",1)
				logdata(errFile,sql3,0)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while checking job control pending status"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			try:
				if jobCnt == 0:
					logdata(logFile,"Checking if existing job stream - " + jobStream + " is in completed status",0)
					sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS='C' and workflow_type='SRC2STG'"
					cursor1.execute(sql3)
					results=cursor1.fetchall()
					for resultsObj in results:
						jctCnt = resultsObj[0]
					if jctCnt > 0:
						#print("Job control entry - " + jobStream + " already exists on repository under Completed status.. Please re-initialize the job and retry")
						logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under Completed status.. Please re-initialize the job and retry",1)
						continue
			except  Exception as e:
				logdata(errFile,"Exception occurred while checking job control completion status",1)
				logdata(errFile,sql3,0)
				#print ("Exception occurred while checking job control completion status")
				logdata(errFile,str(e),1)
				Subject ="Error:Exception occurred while checking job control completion status"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
				#print(str(e))
		metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,TD_Metadata_Scr,ORCL_Metadata_Scr,LOGS_DIR)
		if metRc == -1:
			colval="Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
			updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			logdata(errFile,colval,1)
			Subject ="Error: Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
			errMsg=Subject
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			continue
		else:
			updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
			insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
			logdata(logFile,"Found - " + str(metRc) + " columns for table - " + sourceTable + " from metadata table",0)
		if retryFlag == 'FALSE':
			try:
				logdata(logFile,"Checking whether extraction is History or incremental",0)
				sql4="SELECT distinct nvl(EXTRACT_TYPE,'ALL_DATA') from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
				cursor1.execute(sql4)
				results=cursor1.fetchall()
				if len(results) > 0:
					for etypeObj in results:
						eType=etypeObj[0]
				else:
					eType='ALL_DATA'
			except  Exception as e:
				colval="Exception occurred while checking job control extract type"
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				#print ("Exception occurred while checking job control extract type")
				logdata(errFile,sql4,0)
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while checking job control extract type"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
				#print(str(e))
			updDIYmstr(reqid,'load_type',eType,cursor1,errFile,logFile,emailAdd)
			if eType != 'ALL_DATA':
				logdata(logFile,"Checking incremental column",0)
				try:
					sql5="SELECT distinct INCREMENTAL_COLUMN_NAME from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
					cursor1.execute(sql5)
					results=cursor1.fetchall()
					for incColObj in results:
						incCol=incColObj[0]
					logdata(logFile,"Identified incremental column as - " + incCol,0)
				except  Exception as e:
					colval="Exception occurred while checking job control incremental column." 
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)
					logdata(errFile,sql5,0)
					#print ("Exception occurred while checking job control incremental column")
					logdata(errFile,str(e),1)
					Subject ="Error: Exception occurred while checking job control incremental column."
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
					#print(str(e))
			if sourcedbType.upper() == 'ORACLE':

				logdata(logFile,"Invoking data ingestion engine for Oracle db",0)
				if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db' or itype.lower() == 'jct':
					tblCntvar=1
					logdata(logFile,"Gathering source db (" + srcdbParams['SOURCE_HOST'] + ") connection details from repos db",0)
					try:
						connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
						conn2 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr2)
						cursor2=conn2.cursor()
					except Exception as e:
						colval="Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
						updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while connecting to source database")
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
						#print(str(e))
					try:
						logdata(logFile,"Checking if passed object is view",0)
						sql13 = "select count(1) from all_tables where table_name='" + sourceTable + "' and owner='" + sourceSchema + "'"
						cursor2.execute(sql13)
						results=cursor2.fetchall()
						for vwcheckobj in results:
							tblCnt=vwcheckobj[0]
						if tblCnt == '0':
							#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
							logdata(errFile,"Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested",1)
							Subject ="Error: Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							srcBaseschema = sourceSchema
							srcBasetable = sourceTable
							logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
					except Exception as e:
						#print ("Error occurred while checking base table from 1-1 view")
						colval="Error occurred while validating requested table for ingestion."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print(str(e))
						logdata(errFile,sql13,0)
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while validating requested table for ingestion."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					try:
						logdata(logFile,"Checking if passed table is partitioned",0)
						sql21="select count(1) from all_tab_partitions where table_name='" + sourceTable + "' and table_owner='" + sourceSchema + "'"
						cursor2.execute(sql21)
						results=cursor2.fetchall()
						for partcheckobj in results:
							partCnt=partcheckobj[0]
						if int(partCnt) == 0:
							logdata(logFile,"Source Oracle object - " + sourceSchema + "." + sourceTable + " is not partitioned",0)
						else:
							logdata(logFile,"Source Oracle object - " + sourceSchema + "." + sourceTable + " partitioned and has " + str(partCnt) + " partitions",0)
					except Exception as e:
						logdata(errFile,"Error occurred while checking table partitions",1)
						logdata(errFile,sql21,0)
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while checking table partitions"
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue

					if eType == 'ALL_DATA' or len(eType) == 0:
						logdata(logFile,"Invoking a history load",0)
						updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
						if len(whereClause) == 0:
							try:
								logdata(logFile,"Since this is Full load with no filters checking space availability before triggering ingestion",0)
								if partCnt == 0:
									sql16="select sum(bytes)/1024 from dba_segments where owner='" + srcBaseschema + "' and segment_name='" + srcBasetable + "' and segment_type like 'TABLE'" 
								else:
									sql16="select sum(bytes)/1024 from dba_segments where owner='" + srcBaseschema + "' and segment_name='" + srcBasetable + "' and segment_type like 'TABLE%'"
								cursor2.execute(sql16)
								results=cursor2.fetchall()
								for tblsizeObj in results:
										tblSize=tblsizeObj[0]
								if len(results) == 0 or tblSize is None:
									logdata(logFile,"Could not Calculate space required for " + srcBaseschema + "." + srcBasetable + ". Hence ignoring space validation and proceeding further",1)
								else:
									osFreespacecmd="df " + stageMnt +" | tail -1 | awk '{print $4}'"
									freeSpace=int(os.popen(osFreespacecmd).read())
									spaceCheck=True
									#if freeSpace < tblSize*2:
									#	logdata(errFile,"Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " table.. Hence aborting ingestion",1)
									#	continue
									updDIYmstr(reqid,'tbl_size',str(tblSize),cursor1,errFile,logFile,emailAdd)
							except Exception as e:
								logdata(logFile,"Issue occurred while pulling mount free space.. Ignoring this check and continuing..",0)
								logdata(logFile,str(e),1)
							logdata(logFile,"Querying metadata for source table rowcount based on stats",0)
							try:
								sql5="SELECT num_rows as maxrowcount,round((sysdate-last_analyzed)) elapsed_days from all_tables where owner='" + srcBaseschema + "' and table_name='" + srcBasetable + "'"
								cursor2.execute(sql5)
								results=cursor2.fetchall()
								statCount=len(results)
								if (statCount != 0):
									for srcanalobj in results:
										srclastAnal=srcanalobj[1]
										srcNumrows=srcanalobj[0]
										logdata(logFile,"Source table has " + str(srcNumrows) + " rows based on stats which is " + str(srclastAnal) + " days old",0)
								if (statCount == 0 or srcNumrows is None) :
									#print ("Source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + ".")
									logdata(logFile,"Since source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + " proceeding with row count.",1)
									try:
										sql6="select count(*) from " + sourceSchema + "." + sourceTable
										cursor2.execute(sql6)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
										logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
									except Exception as e:
										colval="Error occurred while collecting source table row count."
										updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,"Error occurred while collecting source table row count1",1)
										logdata(errFile,sql6,0)
										#print ("Error occurred while collecting source table row count1")
										logdata(errFile,str(e),1)
										Subject ="Error: Error occurred while collecting source table row count."
										errMsg=Subject + "\n\n" + str(e)
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
										#print(str(e))
							except Exception as e:
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table stats",1)
								logdata(errFile,sql6,0)
								#print ("Error occurred while collecting source table stats")
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
								#print(str(e))
							try:
								if (statCount > 0 and srclastAnal is not None ):
									if srclastAnal > Anal_Threshold:
										logdata(logFile,"Since table stats are not upto date pulling table count from table",0)
										sql6="select count(*) from " + sourceSchema + "." + sourceTable
										cursor2.execute(sql6)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
										logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
							except Exception as e:
								#print ("Error occurred while collecting source table row count2")
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count2",1)
								logdata(errFile,sql6,0)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							logdata(logFile,"Since source table have filters enabled count directly from table",0)
							try:
								sql6="select count(*) from " + sourceSchema + "." + sourceTable + " where " + whereClause
								cursor2.execute(sql6)
								results=cursor2.fetchall()
								for srcCntobj in results:
									srcNumrows=srcCntobj[0]
								logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
							except Exception as e:
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count3",1)
								logdata(errFile,sql6,0)
								#print ("Error occurred while collecting source table row count3")
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								#print(str(e))
								continue
					else:
						logdata(logFile,"Pulling incremental column data",0)
						updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
						try:
							sql8 = "SELECT distinct decode(EXTRACT_TYPE,'DATE',to_char(to_extract_dtm,'MM/DD/YYYY HH24:MI:SS'),'ID',to_char(to_extract_id)) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
							cursor1.execute(sql8)
							results = cursor1.fetchall()
							for lastextobj in results:
								lastextVal=lastextobj[0]
							if incColname is None:
								sql9=''
								raise Exception("Missing incremental column for JCT - " + jobStream + ". Please check and fix..")
							if lastextVal is None:
								sql9=''
								raise Exception("Missing incremental column value for JCT - " + jobStream + ". Please check and fix..")
							if eType == 'DATE':
								logdata(logFile,"Pulling count from source based on last extract date - " + str(lastextVal),0)
								sql9 ="SELECT count(*) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"
							elif eType == 'ID':
								logdata(logFile,"Pulling count from source table based on last extract id - " + str(lastextVal),0)
								sql9 ="SELECT count(*) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > " + str(lastextVal) 
							else:
								#print("Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry")
								colval="Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
								updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								Subject ="Error: Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
								errMsg=Subject
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
							if len(whereClause) > 0:
								logdata(logFile,"Filter existing.. Applying the same before pulling rowcount",0)
								sql9 = sql9 + " AND " + whereClause
							cursor2.execute(sql9)
							results=cursor2.fetchall()
							for srcCntobj in results:
								srcNumrows=srcCntobj[0]
							logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows for ingestion",0)
						except Exception as e:
							#print ("Error occurred while collecting source table row count4")
							colval="Error occurred while collecting source table row count."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,"Error occurred while collecting source table row count4",1)
							if sql9 is not None:
								logdata(errFile,sql9,0)
							#print(str(e))
							logdata(errFile,str(e),1)
							Subject ="Error: Error occurred while collecting source table row count."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					#print("Source table count is - " + str(srcNumrows))
					try:
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						fSrccnt = open(BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable, 'w')
						fSrccnt.write(str(srcNumrows))
						fSrccnt.close()
					except Exception as e:
						logdata(errFile,"Issue occurred while writing source count to config file-" + BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable,1)
						logdata(errFile,str(e),1)
					#continue
					#lobcolCnt=0
					#try:
					#	logdata(logFile,"Checking for existence of LOB columns",0)
					#	cursor2.execute("select count(1) from all_tab_columns where data_type like '%LOB%' and owner='" + sourceSchema + "' and table_name='" + sourceTable + "'")
					#	results=cursor2.fetchall()
					#	for lobcntObj in results:
					#		lobcolCnt=lobcntObj[0]
					#	if lobcolCnt > 0:
					#		logdata(logFile,"Table " + sourceSchema + "." + sourceTable + " has " + str(lobcolCnt) + " LOB columns.. Hence performing special processing",0)
					#except Exception as e:
					#	logdata(errFile,"Issue occurred while checking existinence of LOB column",1)
					#	logdata(errFile,str(e),1)
					if not threadoverride:
						#if lobcolCnt == 0:
						if srcNumrows >= Split_Orcl_Threshold:
							logdata(logFile,"Qualified rows demands divide and conquer type ingestion",0)
							if srcNumrows >= Split_Threshold and srcNumrows < (Split_Threshold*5):
								threadcount=2
							elif srcNumrows > (Split_Threshold*5) and srcNumrows < (Split_Threshold*10):
								threadcount=3
							elif srcNumrows > (Split_Threshold*10) and srcNumrows < (Split_Threshold*50):
								threadcount=5
							else:
								threadcount=10
							#updDIYmstr(reqid,'Thread_count',str(threadcount),cursor1,errFile,logFile,emailAdd)
						#else:
						#	if srcNumrows > Split_Lob_Threshold:
						#		threadcount=round(srcNumrows/Split_Lob_Threshold)
						#		Thread_count=Thread_Lob_Count
						#	else:
						#		threadcount=1
					#if tCnt is not None:
						#logdata(logFile,"Since thread count override is specified using " + tCnt + " threads for ingestion",0)
						#threadcount=tCnt
						## New logic to check the mount space
					if threadcount > 1:
						updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
						logdata(logFile,"Going with " + str(threadcount) + " parallel threads",0)
						indName=''
						try:
							if sCmn is None:
								logdata(logFile,"Pulling column information for parallizing extraction",0)
								sql10="select index_name from (select count(*),a.index_name from all_indexes a,all_ind_columns b , all_tab_columns c where a.index_name=b.index_name "
								sql10 = sql10 + "and a.table_name='" + sourceTable + "' and a.owner='" + sourceSchema + "' and a.owner=b.table_owner "
								sql10 = sql10 + "and b.column_name=c.column_name and a.table_name=c.table_name and a.owner=c.owner and data_type in ('DATE','NUMBER','VARCHAR2') "
								sql10 = sql10 + "and a.uniqueness='UNIQUE' group by a.index_name order by 1) where rownum=1"
								cursor2.execute(sql10)
								results=cursor2.fetchall()
								if len(results) > 0:
									for indobjname in results:
										indName=indobjname[0]
									sql10="select * from "
									sql10 = sql10 + "(select a.column_name,a.data_type from all_tab_columns a, all_ind_columns b "
									sql10 = sql10 + "where b.index_name='" + indName + "' and b.column_name=a.column_name and a.table_name='" + sourceTable + "' and a.owner='" + sourceSchema + "' "
									sql10 = sql10 + "and a.data_type in ('DATE','NUMBER','VARCHAR2') and a.table_name=b.table_name and nullable='N' order by  num_distinct desc, column_id asc) where rownum=1"
									cursor2.execute(sql10)
									results=cursor2.fetchall()
									if len(results) > 0:
										for indobj in results:
											srcIndcol=indobj[0]
											surcIndColtype=indobj[1]
								else:
									logdata(logFile,"Could not find any unique index on source table for splitting.. Hence proceeding with column which has most unique value",1)
									sql22= "select * from (select column_name,data_type from all_tab_columns where table_name='" + sourceTable + "'" 
									sql22 = sql22 + " and owner='" + sourceSchema + "' and data_type in ('DATE','NUMBER','VARCHAR2') and nullable='N' order by num_distinct desc, column_id asc) where rownum=1"
									cursor2.execute(sql22)
									results=cursor2.fetchall()
									for indobj in results:
										srcIndcol=indobj[0]
										surcIndColtype=indobj[1]
									if len(results) == 0:
										logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
										Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
									else:
										logdata(logFile,"Pulling non-unique index name",0)
										sql10="select index_name from all_ind_columns where table_name='" + sourceTable + "' and table_owner='" + sourceSchema + "' and column_name='" +  srcIndcol + "'"
										cursor2.execute(sql10)
										results=cursor2.fetchall()
										for indobjname in results:
											indName=indobjname[0]
							else:
								logdata(logFile,"Using the over-ride column for parallelizing extraction",0)
								srcIndcol=sCmn
								cursor2.execute("select data_type from all_tab_columns where table_name='" + sourceTable + "' and owner='" + sourceSchema + "' and column_name='" + sCmn + "'")
								results=cursor2.fetchall()
								if len(results) == 0:
									colval="Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
									updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									Subject ="Error: Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
									errMsg=Subject
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								else:
									for colObj in results:
										surcIndColtype=colObj[0]
							logdata(logFile,"Now going to slice table - " + srcBasetable + " based on column - " + srcIndcol,0)
							updDIYmstr(reqid,'split_column',srcIndcol,cursor1,errFile,logFile,emailAdd)
							#updDIYmstr(reqid,'split_count',str(threadcount),cursor1,errFile,logFile,emailAdd)
							#sql11="select " ## fill in for partitions
							loopcnt=0
							splitQrydict={}
							if srcNumrows < Chunk_Orcl_Threshold:
								if spaceCheck:
									logdata(logFile,"Mount point space check",0)
									if freeSpace < ((float(tblSize)/threadcount)*Thread_count)*2*1.5:
										colval="Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion"
										updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)
										Subject ="Error: Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
								logdata(logFile,"Splitting only based on identified incremental column",0)
								updDIYmstr(reqid,'split_count',str(threadcount),cursor1,errFile,logFile,emailAdd)
								split4upd=threadcount
								try:
									sql23="select minval,maxval from "
									if surcIndColtype == 'DATE':
										sql23=sql23+"(select to_char(min(" + srcIndcol + "),'MM/DD/YYYY HH24:MI:SS') minval,to_char(max(" + srcIndcol +"),'MM/DD/YYYY HH24:MI:SS') maxval,bucket from "
									else:
										sql23=sql23+"(select min(" + srcIndcol + ") minval,max(" + srcIndcol +") maxval,bucket from "
									sql23=sql23+"(select  /*+ parallel_index( A " + indName +" 16) parallel(A 16)  */ " + srcIndcol + ", NTILE(" + str(threadcount) + ") "
									sql23=sql23+"OVER (ORDER BY " + srcIndcol + ") AS bucket FROM " + sourceSchema + "." + sourceTable + " A"
									if len(whereClause) > 0:
										sql23=sql23 + " where " + whereClause
										if len(incColname) > 0:
											if eType == 'DATE':
												sql23 = sql23 + ' AND ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql23 = sql23 + ' AND ' + incColname + " > " + str(lastextVal) 
									else:
										if len(incColname) > 0:
											if eType == 'DATE':
												sql23 = sql23 + ' WHERE ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql23 = sql23 + ' WHERE ' + incColname + " > " + str(lastextVal) 
									sql23 =sql23 + " )group by bucket order by bucket)"
									#sql23="select /*+ parallel(a,10) */ min(" + srcIndcol + "),max(" + srcIndcol + ") from " + sourceSchema + "." + sourceTable + " a"
									cursor2.execute(sql23)
									loopcnt=0
									results=cursor2.fetchall()
									prevmaValue=''
									for minmaxobj in results:
										minValue=minmaxobj[0]
										if minValue != None:
											if minValue == prevmaValue:
												if surcIndColtype == 'NUMBER':
													minValue=minValue+1
												elif surcIndColtype == 'DATE':
													minValueF=datetime.strptime(minValue,'%m/%d/%Y %H:%M:%S')
													minValueF=minValueF+timedelta(seconds=1)
													minValue=datetime.strftime(minValueF,'%m/%d/%Y %H:%M:%S')
												elif surcIndColtype == 'VARCHAR2':
													minValue=re.sub(minValue[-1],chr(ord(minValue[-1])+1),minValue) ## To add 1 ascii to last character to make string distinct
											maxValue=minmaxobj[1]
											prevmaValue=maxValue
											if surcIndColtype == 'NUMBER':
												splitQrydict[loopcnt]=srcIndcol + " between " + str(minValue) + " and " + str(maxValue)
											elif surcIndColtype == 'DATE':
												splitQrydict[loopcnt]=srcIndcol + " between to_date(''" + str(minValue) + "'',''MM/DD/YYYY HH24:MI:SS'')" + " and to_date(''" + str(maxValue) + "'',''MM/DD/YYYY HH24:MI:SS'')"
											elif surcIndColtype == 'VARCHAR2':
												splitQrydict[loopcnt]=srcIndcol + " between ''" + str(minValue) + "'' and ''" + str(maxValue) + "''"
											spinsqry="insert into DIY_splits (reqid,table_name,split_column,split_id,min_value,max_value) values "
											spinsqry=spinsqry + "(" + reqid + ",'" + sourceTable + "','" + srcIndcol + "'," + str(loopcnt) + ",'" + str(minValue) + "','" + str(maxValue) + "')"
											try:
												cursor1.execute(spinsqry)
												cursor1.execute("Commit")
											except Exception as e:
												logdata(errFile,"Issue encountered while Inserting Split data to db for table " + sourceTable ,1)
												logdata(errFile,spinsqry,0)
												logdata(errFile,str(e),1)
											loopcnt += 1
										else:
											threadcount=threadcount-1
								except Exception as e:
									colval="Issue encountered while fetching min/max value of column " + srcIndcol + " in table " + sourceSchema + "." + sourceTable + "."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									logdata(errFile,sql23,0)
									logdata(errFile,str(e),1)
									Subject ="Error: Issue encountered while fetching min/max value of column " + srcIndcol + " in table " + sourceSchema + "." + sourceTable + "."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
							## Below block is if srNumrows >=Chunk_Orcl_Threshold
							else:
								#logdata(logFile,"Pulling most unique date column for data consistency",0)
								#try:
								#	sql26= "select * from (select a.column_name,a.data_type,b.index_name from all_tab_columns a,all_ind_columns b where a.table_name='" + sourceTable + "'" 
								#	sql26 = sql26 + " and a.owner='" + sourceSchema + "' and a.data_type in ('DATE') and a.table_name=b.table_name and a.owner=b.table_owner and a.column_name=b.column_name "
								#	sql26 = sql26 + " order by num_distinct desc, column_id asc) where rownum=1"
								#	cursor2.execute(sql26)
								#	results=cursor2.fetchall()
								#	for indobj in results:
								#		lastUpdcol=indobj[0]
								#		dateIndcol=indobj[2]
								#	if len(results) == 0:
								#		lastUpdColFlag=False
								#	else:
								#		lastUpdColFlag=True
								#	if lastUpdColFlag:
								#		cursor2.execute("select to_char(trunc(sysdate),'MM/DD/YYYY') from dual")
								#		results=cursor2.fetchall()
								#		for currDateobj in results:
								#			currDate = currDateobj[0]
								#except Exception as e:
								#	logdata(errFile,"Issue encountered while fetching most unique date column for table " + sourceSchema + "." + sourceTable,1)
								#	logdata(errFile,sql26,0)
								#	logdata(errFile,str(e),1)
								logdata(logFile,"Now splitting table",1)
								try:
									chunkcount=10
									if not threadoverride:
										if srcNumrows >= Chunk_Orcl_Threshold and srcNumrows < (Chunk_Orcl_Threshold*2):
											chunkcount=20
										elif srcNumrows >= (Chunk_Orcl_Threshold*2) and srcNumrows < (Chunk_Orcl_Threshold*3):
											chunkcount=30
										elif srcNumrows >= (Chunk_Orcl_Threshold*3) and srcNumrows < (Chunk_Orcl_Threshold*5):
											chunkcount=50
										elif srcNumrows >= (Chunk_Orcl_Threshold*5):
											chunkcount=100
									else:
										chunkcount=threadcount
								except Exception as e:
									colval="Issue encountered while deriving chunkcount."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									logdata(errFile,str(e),1)
									Subject ="Error: Issue encountered while deriving chunkcount."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								loopcnt=0
								updDIYmstr(reqid,'split_count',str(chunkcount),cursor1,errFile,logFile,emailAdd)
								split4upd=chunkcount
								if spaceCheck:
									logdata(logFile,"Mount point space check",0)
									if freeSpace < ((float(tblSize)/chunkcount)*Thread_count)*2*1.5:
										colval="Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion"
										updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)
										Subject ="Error: Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
								#if lastUpdColFlag:										
								#	try:
								#		sql23="select minval,maxval from "
								#		if surcIndColtype == 'NUMBER':
								#			sql23=sql23+"(select min(" + srcIndcol + ") minval,max(" + srcIndcol +") maxval,bucket from "
								#		elif surcIndColtype == 'DATE':
								#			sql23=sql23+"(select to_char(min(" + srcIndcol + "),'MM/DD/YYYY HH24:MI:SS') minval,to_char(max(" + srcIndcol +"),'MM/DD/YYYY HH24:MI:SS') maxval,bucket from "
								#		sql23=sql23+"(select  /*+ parallel_index( A " + indName +" 16) parallel(A 16)  */ " + srcIndcol + ", NTILE(" + str(chunkcount) + ") "
								#		sql23=sql23+"OVER (ORDER BY " + srcIndcol + ") AS bucket FROM " + sourceSchema + "." + sourceTable + " A"
								#		if len(whereClause) > 0:
								#			sql23=sql23 + " where " + whereClause
								#			if len(incColname) > 0:
								#				if etype == 'DATE':
								#					sql23 = sql23 + ' AND ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
								#				elif  eType == 'ID':
								#					sql23 = sql23 + ' AND ' + incColname + " > " + str(lastextVal) 
								#		else:
								#			if len(incColname) > 0:
								#				if eType == 'DATE':
								#					sql23 = sql23 + ' WHERE ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
								#				elif  eType == 'ID':
								#					sql23 = sql23 + ' WHERE ' + incColname + " > " + str(lastextVal) 
								#		sql23 =sql23 + " )group by bucket order by bucket)"
								#		cursor2.execute(sql23)
								#		results=cursor2.fetchall()
								#		prevmaValue=''
								#		for minmaxobj in results:
								#			minValue=minmaxobj[0]
								#			if minValue != None:
								#				if minValue == prevmaValue:
								#					if surcIndColtype == 'NUMBER':
								#						minValue=minValue+1
								#					elif surcIndColtype == 'DATE':
								#						minValueF=datetime.strptime(minValue,'%m/%d/%Y %H:%M:%S')
								#						minValueF=minValueF+timedelta(seconds=1)
								#						minValue=datetime.strftime(minValueF,'%m/%d/%Y %H:%M:%S')
								#				maxValue=minmaxobj[1]
								#				prevmaValue=maxValue
								#				if surcIndColtype == "NUMBER":
								#					splitQrydict[loopcnt]= lastUpdcol + "< to_date(''" + currDate + "'',''MM/DD/YYYY'') AND " +  srcIndcol + " between " + str(minValue) + " and " + str(maxValue)
								#				elif surcIndColtype == 'DATE':
								#					splitQrydict[loopcnt]=lastUpdcol + "< to_date(''" + currDate + "'',''MM/DD/YYYY'') AND " +  srcIndcol + " between to_date(''" + str(minValue) + "'',''MM/DD/YYYY HH24:MI:SS'')" + " and to_date(''" + str(maxValue) + "'',''MM/DD/YYYY HH24:MI:SS'')"
								#				else:
								#					raise Exception ("Unsupported data type for table " + srcIndcol + " in table " + sourceSchema + "." + sourceTable )
								#				loopcnt += 1
								#			else:
								#				threadcount=threadcount-1
								#	except Exception as e:
								#		logdata(errFile,"Issue encountered while fetching min/max value of column " + srcIndcol + " in table " + sourceSchema + "." + sourceTable ,1)
								#		logdata(errFile,sql23,0)
								#		logdata(errFile,str(e),1)
								#		raise Exception ("Issue encountered while fetching min/max value of column " + srcIndcol + " in table " + sourceSchema + "." + sourceTable )
								#		continue
								#else:
								#	logdata(logFile,"No date column found. So trying to split the table just based on unique column",0)
								try:
									sql23="select minval,maxval from "
									if surcIndColtype == 'NUMBER':
										sql23=sql23+"(select min(" + srcIndcol + ") minval,max(" + srcIndcol +") maxval,bucket from "
									elif surcIndColtype == 'DATE':
										sql23=sql23+"(select to_char(min(" + srcIndcol + "),'MM/DD/YYYY HH24:MI:SS') minval,to_char(max(" + srcIndcol +"),'MM/DD/YYYY HH24:MI:SS') maxval,bucket from "
									sql23=sql23+"(select  /*+ parallel_index( A " + indName +" 16) parallel(A 16)  */ " + srcIndcol + ", NTILE(" + str(chunkcount) + ") "
									sql23=sql23+"OVER (ORDER BY " + srcIndcol + ") AS bucket FROM " + sourceSchema + "." + sourceTable + " A"
									if len(whereClause) > 0:
										sql23=sql23 + " where " + whereClause
										if len(incColname) > 0:
											if eType == 'DATE':
												sql23 = sql23 + ' AND ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql23 = sql23 + ' AND ' + incColname + " > " + str(lastextVal) 
									else:
										if len(incColname) > 0:
											if eType == 'DATE':
												sql23 = sql23 + ' WHERE ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql23 = sql23 + ' WHERE ' + incColname + " > " + str(lastextVal) 
									sql23 =sql23 + " )group by bucket order by bucket)"
									cursor2.execute(sql23)
									results=cursor2.fetchall()
									loopcnt=0
									prevmaValue=''
									for minmaxobj in results:
										minValue=minmaxobj[0]
										if minValue != None:
											if minValue == prevmaValue:
												if surcIndColtype == 'NUMBER':
													minValue=minValue+1
												elif surcIndColtype == 'DATE':
													minValueF=datetime.strptime(minValue,'%m/%d/%Y %H:%M:%S')
													minValueF=minValueF+timedelta(seconds=1)
													minValue=datetime.strftime(minValueF,'%m/%d/%Y %H:%M:%S')
												elif surcIndColtype == 'VARCHAR2':
													minValue=re.sub(minValue[-1],chr(ord(minValue[-1])+1),minValue) ## To add 1 ascii to last character to make string distinct
											maxValue=minmaxobj[1]
											prevmaValue=maxValue
											if surcIndColtype == "NUMBER":
												splitQrydict[loopcnt]= srcIndcol + " between " + str(minValue) + " and " + str(maxValue)
											elif surcIndColtype == 'DATE':
												splitQrydict[loopcnt]= srcIndcol + " between to_date(''" + str(minValue) + "'',''MM/DD/YYYY HH24:MI:SS'')" + " and to_date(''" + str(maxValue) + "'',''MM/DD/YYYY HH24:MI:SS'')"
											elif surcIndColtype == "VARCHAR2":
												splitQrydict[loopcnt]= srcIndcol + " between ''" + str(minValue) + "'' and ''" + str(maxValue) + "''"
											else:
												raise Exception ("Unsupported data type for table " + srcIndcol + " in table " + sourceSchema + "." + sourceTable )
											spinsqry="insert into DIY_splits (reqid,table_name,split_column,split_id,min_value,max_value) values "
											spinsqry=spinsqry + "(" + reqid + ",'" + sourceTable + "','" + srcIndcol + "'," + str(loopcnt) + ",'" + str(minValue) + "','" + str(maxValue) + "')"
											try:
												cursor1.execute(spinsqry)
												cursor1.execute("Commit")
											except Exception as e:
												logdata(errFile,"Issue encountered while Inserting Split data to db for table " + sourceTable ,1)
												logdata(errFile,spinsqry,0)
												logdata(errFile,str(e),1)
											loopcnt += 1
										else:
											threadcount=threadcount-1
								except Exception as e:
									logdata(errFile,"Error occurred while splitting table without date column.. Hence skipping this ingestion",1)
									logdata(errFile,str(e),1)
									raise Exception ("Error occurred while splitting table without date column.. Hence skipping this ingestion")
									continue
							## End of if/else block for srNumrows < or > Chunk_Orcl_Threshold
							logdata(logFile,"Completed pulling the range partitions for data pull",0)
						except Exception as e:
							colval="Error occurred while gathering columns for source table."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							#print ("Error occurred while gathering columns for source table")
							logdata(errFile,colval,1)
							#print(str(e))
							logdata(errFile,str(e),1)
							Subject ="Error: Error occurred while gathering columns for source table."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						## End of indentation for multi thread based ingestion
					else:
						splitQrydict={}
						#splitQrydict[0]="select * from " + sourceSchema + "." + sourceTable 
						if len(whereClause) > 0:
							splitQrydict[0] = whereClause
						else:
							splitQrydict[0]=""
				insQryDict=[]
				jobStreamarr=[]
				#jobStreammstr=jobStream + "_" + str(ftime)
				jobStreammstr=jobStream
				if not threadoverride:
					if srcNumrows >= Split_Orcl_Threshold:
						if len(splitQrydict) <2:
							colval="Table identified as large, but bug detected while deriving splitting logic.. Hence giving up now.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Table identified as large, but bug detected while deriving splitting logic.. Hence giving up now.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
				if len(splitQrydict) > 1:
					jobStreammstr=jobStream + "_" + str(ftime)
					jobstreamid=jobStream
					compCode='A'
				else:
					jobStreammstr=jobStream
					jobstreamid=jobStream
					jobStreamarr.append(jobStreammstr)
					compCode='P'
				if existFlag == 'FALSE':
					InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
					InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
					InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
					InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
					InsQry += " values "
					InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
					InsQry += "','" + sourceTable + "','" + whereClause + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
					InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole + "',NULL"
					InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
					#print (InsQry)
					logdata(logFile,"Master record for insertion",0)
					#logdata(logFile,InsQry,0)
					insQryDict.append(InsQry)
				if existFlag == 'TRUE':
					if len(splitQrydict) > 1:
						logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
						try:
							UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
							UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "'"
							cursor1.execute(UpdQry)
							cursor1.execute('commit')
						except Exception as e:
							colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							logdata(errFile,InsQry,0)
							logdata(errFile,str(e),1)
							Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					else:
						cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
						cursor1.execute("commit")
				if len(splitQrydict) > 1:
					logdata(logFile,"Child records for insertion",0)
					for key,value in splitQrydict.items():
						if len(whereClause) > 0:
							if srcIndcol + " " in whereClause:
								whereClausenew = value
							else:
								whereClausenew = whereClause + ' and ' + value
						else:
							whereClausenew = value
						## Change 4.13
						if len(incColname) > 0:
							if eType == 'DATE':
								whereClausenew = whereClausenew + ' AND ' + incColname + " > to_date(''" + lastextVal + "'',''MM/DD/YYYY HH24:MI:SS'')"  ## check to_date syntax
							elif  eType == 'ID':
								whereClausenew = whereClausenew + ' AND ' + incColname + " > " + str(lastextVal) 
						#jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
						InsQry += " values "
						InsQry += "('" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + ijobGroup + "','" + str(key)  
						InsQry += "','Y','P','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "',' " + whereClausenew + " ','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable + "_" + str(key) + "','" + sfWh + "','" + sfRole + "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
						try:
							cursor1.execute("select job_stream_id from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and job_stream_seq_num=" + str(key) + " and run_status='C' and active_ind='Y' and workflow_type='SRC2STG' order by create_date")
							results=cursor1.fetchall()
							if len(results) == 0:
								insQryDict.append(InsQry)
								jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
							else:
								for existObj in results:
									jctExist=existObj[0]
								if len(incColname) > 0:
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClausenew + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "'"
									updQry = updQry + ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
								else:
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClausenew + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "'"
									updQry = updQry+ ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
								insQryDict.append(updQry)
								jobStreamarr.append(jctExist)
						except Exception as e:
							logdata(logFile,"Issue encountered while checking existing JCT child process",1)
							logdata(errFile,str(e),1)
							insQryDict.append(InsQry)
						#print (InsQry)
						#logdata(logFile,InsQry,0)
						#insQryDict.append(InsQry)
			elif sourcedbType.upper() == 'TERADATA':
				logdata(logFile,"Invoking data ingestion engine for Teradata db",0)
				if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db'  or itype.lower() == 'jct':
					tblCntvar=1
					logdata(logFile,"Gathering source TD db connection details from repos db",0)
					try:
						udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False,odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",logDir="/apps/edwsfdata/python/pylogs/logs")
						#udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False)
						conn2 = udaexec.connect(method="odbc", DSN="tdprodetloffload")
						cursor2 = conn2.cursor()
					except Exception as e:
						colval="Error occurred while connecting to source database."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while connecting to source database")
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while connecting to source database."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
						#print(str(e))
					try:
						logdata(logFile,"Checking and invoking base table details if passed object is view",0)
						sql13 = " exec PERFMETRICSDB.BASEVIEWCHECK ('" + sourceSchema + "','" + sourceTable + "')"
						cursor2.execute(sql13)
						results=cursor2.fetchall()
						for vwcheckobj in results:
							tblName=vwcheckobj[0]
						if tblName == '-1':
							#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
							logdata(errFile,"Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested",1)
							Subject ="Error: Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							objArray = tblName.split(".")
							srcBaseschema = objArray[0]
							srcBasetable = objArray[1]
							logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
					except Exception as e:
						colval="Error occurred while checking base table from 1-1 view."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while checking base table from 1-1 view")
						#print(str(e))
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while checking base table from 1-1 view."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					if eType == 'ALL_DATA' or len(eType) == 0:
						logdata(logFile,"Invoking a history load",0)
						updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
						if len(whereClause) == 0:
							try:
								logdata(logFile,"Since this is Full load with no filters checking space availability before triggering ingestion",0)
								sql16="select sum(currentperm)/1024 from dbc.tablesizev where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'" 
								cursor2.execute(sql16)
								results=cursor2.fetchall()
								if len(results) == 0:
									logdata(errFile,"Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream",1)
									Subject ="Error: Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream"
									errMsg=Subject
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								else:
									for tblsizeObj in results:
										tblSize=tblsizeObj[0]
									osFreespacecmd="df " + stageMnt +" | tail -1 | awk '{print $4}'"
									freeSpace=int(os.popen(osFreespacecmd).read())
									spaceCheck=True
									#if freeSpace < tblSize*2:
									#	logdata(errFile,"Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " table.. Hence aborting ingestion",1)
									#	continue
								updDIYmstr(reqid,'tbl_size',str(tblSize),cursor1,errFile,logFile,emailAdd)
							except Exception as e:
								logdata(errFile,"Issue occurred while checking mount free space.. Ignoring this check and continuing..",0)
								logdata(errFile,str(e),1)
							logdata(logFile,"Querying metadata for source table rowcount based on stats",0)
							try:
								#sql5="SELECT max(rowcount) as maxrowcount,((current_timestamp - max(lastcollecttimestamp)) HOUR(4) TO SECOND) (NAMED  ElapsedTime),Extract(HOUR From elapsedtime)/24 elapsed_days"
								#sql5 = sql5 + " from dbc.statsv having maxrowcount is not null  where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'" 
								#sql5="SELECT max(rowcount) as maxrowcount,extract (day from ((current_timestamp - max(lastcollecttimestamp)) day(4) TO SECOND(6)))  (NAMED  elapsed_days) elapsed_days"
								#sql5 = sql5 + " from dbc.statsv having maxrowcount is not null  where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'" 
								sql5 = "SELECT	rowcount, extract (day from ((current_timestamp - (lastcollecttimestamp)) day(4) TO SECOND(6)))  (NAMED  elapsed_days) elapsed_days"
								sql5 = sql5 + " from	dbc.statsv where	databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'"
								sql5 = sql5 + " qualify row_number() over ( partition by databasename, tablename order by lastcollecttimestamp desc ) = 1"
								cursor2.execute(sql5)
								results=cursor2.fetchall()
								statCount=len(results)
								if (statCount == 0):
									#print ("Source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + ".")
									logdata(logFile,"Since source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + " proceeding with row count.",1)
									try:
										sql6="select count(*) from " + sourceSchema + "." + sourceTable
										cursor2.execute(sql6)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
										logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
									except Exception as e:
										colval="Error occurred while collecting source table row count."
										updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,"Error occurred while collecting source table row count1",1)
										logdata(errFile,sql5,0)
										#print ("Error occurred while collecting source table row count1")
										logdata(errFile,str(e),1)
										Subject ="Error: Error occurred while collecting source table row count."
										errMsg=Subject + "\n\n" + str(e)
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
										#print(str(e))
								else:
									for srcanalobj in results:
										#srclastAnal=srcanalobj[2]
										srclastAnal=srcanalobj[1]
										srcNumrows=srcanalobj[0]
										logdata(logFile,"Source table has " + str(srcNumrows) + " rows based on stats gathered which is " + str(srclastAnal) + " days old",0)
							except Exception as e:
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table stats",1)
								#print ("Error occurred while collecting source table stats")
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table stats."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
								#print(str(e))
							try:
								if (statCount > 0):
									if srclastAnal > Anal_Threshold:
										logdata(logFile,"Since table stats are not upto date pulling table count from table",0)
										sql6="select cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable
										cursor2.execute(sql6)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
										logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
							except Exception as e:
								#print ("Error occurred while collecting source table row count2")
								updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count2",1)
								logdata(errFile,sql6,0)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							logdata(logFile,"Since source table dont have stats enabled pulling count directly from table",0)
							try:
								sql6="select cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + whereClause
								cursor2.execute(sql6)
								results=cursor2.fetchall()
								for srcCntobj in results:
									srcNumrows=srcCntobj[0]
								logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
							except Exception as e:
								updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count3",1)
								logdata(errFile,sql6,0)
								#print ("Error occurred while collecting source table row count3")
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								#print(str(e))
								continue
					else:
						logdata(logFile,"Pulling incremental column data",0)
						updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
						try:
							sql8 = "SELECT distinct decode(EXTRACT_TYPE,'DATE',to_char(to_extract_dtm,'MM/DD/YYYY HH24:MI:SS'),'ID',to_char(to_extract_id)) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
							cursor1.execute(sql8)
							results = cursor1.fetchall()
							for lastextobj in results:
								lastextVal=lastextobj[0]
							if incColname is None:
								sql9=''
								raise Exception("Missing incremental column for JCT - " + jobStream + ". Please check and fix..")
							if lastextVal is None:
								sql9=''
								raise Exception("Missing incremental column value for JCT - " + jobStream + ". Please check and fix..")
							if eType == 'DATE':
								logdata(logFile,"Pulling count from source based on last extract date - " + str(lastextVal),0)
								sql9 ="SELECT cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"
							elif eType == 'ID':
								logdata(logFile,"Pulling count from source table based on last extract id - " + str(lastextVal),0)
								sql9 ="SELECT cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > " + str(lastextVal) 
							else:
								#print("Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry")
								colval="Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
								updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								Subject ="Error: Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
								errMsg=Subject
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
							if len(whereClause) > 0:
								logdata(logFile,"Filter existing.. Applying the same before pulling rowcount",0)
								sql9 = sql9 + " AND " + whereClause
							cursor2.execute(sql9)
							results=cursor2.fetchall()
							for srcCntobj in results:
								srcNumrows=srcCntobj[0]
							logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows for ingestion",0)
						except Exception as e:
							#print ("Error occurred while collecting source table row count4")
							colval="Error occurred while collecting source table row count."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,"Error occurred while collecting source table row count4",1)
							if sql9 is not None:
								logdata(errFile,sql9,0)
							#print(str(e))
							logdata(errFile,str(e),1)
							Subject ="Error: Error occurred while collecting source table row count."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					#print("Source table count is - " + str(srcNumrows))
					try:
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						fSrccnt = open(BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable, 'w')
						fSrccnt.write(str(srcNumrows))
						fSrccnt.close()
					except Exception as e:
						logdata(errFile,"Issue occurred while writing source count to config file-" + BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable,1)
						Subject ="Error: Issue occurred while writing source count to config file-" + BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					if not threadoverride:
						if srcNumrows >= Split_Threshold:
							logdata(logFile,"Qualified rows demands divide and conquer type ingestion",0)
							if srcNumrows >= Split_Threshold and srcNumrows < (Split_Threshold*100):
								threadcount=2
							elif srcNumrows > (Split_Threshold*100) and srcNumrows < (Split_Threshold*500):
								threadcount=3
							elif srcNumrows > (Split_Threshold*500):
								threadcount=5
							#elif srcNumrows > (Split_Threshold*50):
							#	threadcount=10
					#if tCnt is not None:
					#	logdata(logFile,"Since thread count override is specified using " + tCnt + " threads for ingestion",0)
					#	threadcount=tCnt
					## New logic to check the mount space
					if spaceCheck:
						logdata(logFile,"Mount point space check",0)
						if freeSpace < ((float(tblSize)/threadcount)*Thread_count)*2*1.5:
							colval="Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " splitting into " + str(threadcount) + " chunks and being loaded using " + str(Thread_count) + "  threads.. Hence aborting ingestion."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					if threadcount > 1:
						updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
						logdata(logFile,"Going with " + str(threadcount) + " parallel threads",0)
						try:
							if sCmn is None:
								#TD Integer limit  2,147,483,647
								logdata(logFile,"Pulling column information for parallelizing extraction",0)
								#sql10="select trim(columnname) from dbc.indices where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "' and indexType IN ('P', 'Q') and ColumnPosition=1"
								sql10="select top 1 trim(a.columnname),trim(b.columntype),trim(b.columnformat) from dbc.indicesv a ,dbc.columnsv b where a.databasename='" + srcBaseschema + "' and a.tablename='" + srcBasetable
								sql10= sql10 + "' and  a.indexType IN ('P', 'Q') and a.databasename=b.databasename and a.tablename=b.tablename and a.columnname=b.columnname  and nullable='N' "
								sql10= sql10 + " and columntype in ('DA','I','I8','I1','I2','TS','TZ','D') order by ColumnPosition"
								cursor2.execute(sql10)
								results=cursor2.fetchall()
								for indobj in results:
									srcIndcol=indobj[0]
									surcIndColtype=indobj[1]
									surcIndColFormat=indobj[2]
								if len(results) == 0:
									sql10="select top 1 trim(a.columnname),trim(b.columntype),trim(b.columnformat) from dbc.indicesv a ,dbc.columnsv b where a.databasename='" + srcBaseschema + "' and a.tablename='" + srcBasetable
									sql10= sql10 + "' and  a.indexType IN ('P', 'Q') and a.databasename=b.databasename and a.tablename=b.tablename and a.columnname=b.columnname  and nullable='N' "
									sql10= sql10 + " and columntype in ('CV') order by ColumnPosition"
									cursor2.execute(sql10)
									results=cursor2.fetchall()
									for indobj in results:
										srcIndcol=indobj[0]
										surcIndColtype=indobj[1]
										surcIndColFormat=indobj[2]
									if len(results) == 0:
										colval="Could not find any index on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
										updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)
										Subject ="Error: Could not find any index on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
							else:
									logdata(logFile,"Using the over-ride column for parallelizing extraction",0)
									srcIndcol=sCmn
									cursor2.execute("select trim(columntype),trim(columnformat) from dbc.columnsv where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "' and columnname='" + sCmn + "'")
									results=cursor2.fetchall()
									if len(results) == 0:
										colval="Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
										updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)
										Subject ="Error: Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
									else:
										for colObj in results:
											surcIndColtype=colObj[0]
											surcIndColFormat=colObj[1]
							logdata(logFile,"Now going to slice table - " + srcBasetable + " based on column - " + srcIndcol,0)
							updDIYmstr(reqid,'split_column',srcIndcol,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'split_count',str(threadcount),cursor1,errFile,logFile,emailAdd)
							split4upd=threadcount
							#sql11="select " ## fill in for partitions
							loopcnt=0
							splitQrydict={}
							while loopcnt < threadcount:
								#tempcnt=loopcnt+1
								#splitQrydict[loopcnt]=splitqry + " from ( select t.*,ntile(" + str(threadcount) + ") over(order by NULL) as num from " + sourceSchema + "." + sourceTable + " t) t1 where t1.num=" + str(tempcnt)
								try:
									if surcIndColtype in ('TS','TZ'):
										sql12="select (min(" + srcIndcol + ")),(max(" + srcIndcol + ")) from ("
									else:
										sql12="select trim(min(" + srcIndcol + ")),trim(max(" + srcIndcol + ")) from ("
									if len(whereClause) > 0:
										sql12 = sql12 + "select (CAST(RANK() OVER (ORDER BY " + srcIndcol + ") AS BIGINT) - 1) * " + str(threadcount) + " / CAST(COUNT(*) OVER() AS BIGINT ) bucket, a.* from " + sourceSchema + "." + sourceTable + " A where " + whereClause
										if len(incColname) > 0:
											if eType == 'DATE':
												sql12 = sql12 + ' AND ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql12 = sql12 + ' AND ' + incColname + " > " + str(lastextVal) 
									else:
										#sql12 = sql12 + "select QUANTILE(" + str(threadcount) + "," + srcIndcol + ") bucket, a.* from " + sourceSchema + "." + sourceTable + " A"
										sql12 = sql12 + "select (CAST(RANK() OVER (ORDER BY " + srcIndcol + ") AS BIGINT) - 1) * " + str(threadcount) + " / CAST(COUNT(*) OVER() AS BIGINT ) bucket, a.* from " + sourceSchema + "." + sourceTable + " A"
										if len(incColname) > 0:
											if eType == 'DATE':
												sql12 = sql12 + ' WHERE ' + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"  ## check to_date syntax
											elif  eType == 'ID':
												sql12 = sql12 + ' WHERE ' + incColname + " > " + str(lastextVal) 
									sql12 = sql12 + ") AA where aa.bucket=" + str(loopcnt)
									cursor2.execute(sql12)
									results=cursor2.fetchall()
									prevmaValue=''
									for minmaxobj in results:
										minValue=minmaxobj[0]
										if minValue == prevmaValue:
											if surcIndColtype == 'I':
												minValue=minValue+1
											elif surcIndColtype == 'DA':
												minValueF=datetime.strptime(minValue,'%m/%d/%Y %H:%M:%S')
												minValueF=minValueF+timedelta(seconds=1)
												minValue=datetime.strftime(minValueF,'%m/%d/%Y %H:%M:%S')
											elif surcIndColtype == 'CV':
												minValue=re.sub(minValue[-1],chr(ord(minValue[-1])+1),minValue) ## To add 1 ascii to last character to make string distinct
										maxValue=minmaxobj[1]
										prevmaValue=maxValue
								except Exception as e:
									colval="Error occurred while collecting min max value for thread - " + str(loopcnt) + "."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									logdata(errFile,sql12,0)
									#print ("Error occurred while collecting min max value for thread - " + loopcnt)
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting min max value for thread - " + str(loopcnt) + "."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									#print(str(e))
									#continue
									raise Exception(str(e))
								if minValue != None:
									if surcIndColtype == 'I':
										splitQrydict[loopcnt]=srcIndcol + " between " + str(minValue) + " and " + str(maxValue)
									elif surcIndColtype == 'DA':
										splitQrydict[loopcnt]=srcIndcol + " between to_date(''" + str(minValue) + "'',''" + surcIndColFormat + "'') and to_date(''" + str(maxValue) + "'',''" + surcIndColFormat + "'')"
									else:
										splitQrydict[loopcnt]=srcIndcol + " between ''" + str(minValue) + "'' and ''" + str(maxValue) + "''"
									spinsqry="insert into DIY_splits (reqid,table_name,split_column,split_id,min_value,max_value) values "
									spinsqry=spinsqry + "(" + reqid + ",'" + sourceTable + "','" + srcIndcol + "'," + str(loopcnt) + ",'" + str(minValue) + "','" + str(maxValue) + "')"
									try:
										cursor1.execute(spinsqry)
										cursor1.execute("Commit")
									except Exception as e:
										logdata(errFile,"Issue encountered while Inserting Split data to db for table " + sourceTable ,1)
										logdata(errFile,spinsqry,0)
										logdata(errFile,str(e),1)
									loopcnt += 1
								else:
									threadcount=threadcount-1
							logdata(logFile,"Completed pulling the range partitions for data pull",0)
						except Exception as e:
							#print ("Error occurred while gathering columns for source table")
							colval="Error occurred while gathering columns for source table."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							#print(str(e))
							logdata(errFile,str(e),1)
							Subject ="Error: Error occurred while gathering columns for source table."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					else:
						splitQrydict={}
						#splitQrydict[0]="select * from " + sourceSchema + "." + sourceTable 
						if len(whereClause) > 0:
							splitQrydict[0] = whereClause
						else:
							splitQrydict[0]=""
				insQryDict=[]
				jobStreamarr=[]
				#jobStreammstr=jobStream + "_" + str(ftime)
				jobStreammstr=jobStream
				if len(splitQrydict) > 1:
					jobStreammstr=jobStream + "_" + str(ftime)
					jobstreamid=jobStream
					compCode='A'
				else:
					jobStreammstr=jobStream
					jobstreamid=jobStream
					jobStreamarr.append(jobStreammstr)
					compCode='P'
				if existFlag == 'FALSE':
					InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
					InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
					InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
					InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
					InsQry += " values "
					InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
					InsQry += "','" + sourceTable + "','" + whereClause + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
					InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole+ "',NULL"
					InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
					#print (InsQry)
					logdata(logFile,"Master record for insertion",0)
					#logdata(logFile,InsQry,0)
					insQryDict.append(InsQry)
				if existFlag == 'TRUE':
					if len(splitQrydict) > 1:
						logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
						try:
							UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
							UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "' and workflow_type='SRC2STG'"
							cursor1.execute(UpdQry)
							cursor1.execute('commit')
						except Exception as e:
							colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							logdata(errFile,InsQry,0)
							logdata(errFile,str(e),1)
							Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
					else:
						cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
						cursor1.execute("commit")
				if len(splitQrydict) > 1:
					logdata(logFile,"Child records for insertion",0)
					for key,value in splitQrydict.items():
						if len(whereClause) > 0:
							if srcIndcol + " " in whereClause:
								whereClausenew = value
							else:
								whereClausenew = whereClause + ' and ' + value
						else:
							whereClausenew = value
						## Change 4.13
						if len(incColname) > 0:
							if eType == 'DATE':
								whereClausenew = whereClausenew + ' AND ' + incColname + " > to_date(''" + lastextVal + "'',''MM/DD/YYYY HH24:MI:SS'')"  ## check to_date syntax
							elif  eType == 'ID':
								whereClausenew = whereClausenew + ' AND ' + incColname + " > " + str(lastextVal) 
						#jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
						InsQry += " values "
						InsQry += "('" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + ijobGroup + "','" + str(key)  
						InsQry += "','Y','P','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "',' " + whereClausenew + " ','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable + "_" + str(key) + "','" + sfWh + "','" + sfRole + "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
						try:
							cursor1.execute("select job_stream_id from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and job_stream_seq_num=" + str(key) + " and run_status='C' and active_ind='Y' and workflow_type='SRC2STG' order by create_date")
							results=cursor1.fetchall()
							if len(results) == 0:
								insQryDict.append(InsQry)
								jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
							else:
								for existObj in results:
									jctExist=existObj[0]
								if len(incColname) > 0:
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClausenew + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "'"
									updQry = updQry + ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
								else:
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClausenew + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "'"
									updQry = updQry+ ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
								insQryDict.append(updQry)
								jobStreamarr.append(jctExist)
						except Exception as e:
							logdata(logFile,"Issue encountered while checking existing JCT child process",1)
							logdata(errFile,str(e),1)
							insQryDict.append(InsQry)
						#print (InsQry)
						#logdata(logFile,InsQry,0)
						#insQryDict.append(InsQry)
			if len(insQryDict) > 0:
				logdata(logFile,"Building JCT insert sql",0)
				jctFile = open(LOGS_DIR+'jctinsert.sql','w')
				for value in insQryDict:
					jctFile.write(value + ';\n')
				jctFile.write('commit')
				jctFile.close()
				## Execute generated SQL file
				jctFilef = open(LOGS_DIR+'jctinsert.sql')
				full_sql = jctFilef.read()
				jctFilef.close()
				sql_commands = full_sql.split(';\n')
				logdata(logFile,"Inserting JCT sqls",0)
				try:
					for sql_command in sql_commands:
						#print ("executing sql command - " + sql_command)
						cursor1.execute(sql_command)
				except Exception as e:
					logdata(logFile,"Rolling back inserted data due to exception",1)
					logdata(errFile,sql_command,0)
					cursor1.execute('rollback')
					colval="Error occurred while inserting sqls into JCT."
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)
					#print ("Error occurred while inserting sqls into JCT")
					logdata(errFile,str(e),1)
					Subject ="Error: Error occurred while inserting sqls into JCT."
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
					#print(str(e))
			#if conn2:
			#	conn2.close()
			## End of indentation for Teradata type ingestion
		## End of indentation for retryflag=false
		else:
			logdata(logFile,"Now retrying existing jobstream - " + jobStream,1)
			jobStreamarr=[]
			try:
				sql14="select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='P' and workflow_type='SRC2STG'"
				cursor1.execute(sql14)
				results=cursor1.fetchall()
				if len(results) == 1:
					for resjobstr in results:
						jobStreammstr=resjobstr[0]
						ijobGroup=resjobstr[1]
					jobStreamarr.append(jobStreammstr)
				else:
					sql15="select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='A' and workflow_type='SRC2STG'"
					cursor1.execute(sql15)
					results=cursor1.fetchall()
					if len(results) == 0:
						cursor1.execute("select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='R' and workflow_type='SRC2STG'")
						results=cursor1.fetchall()
						if len(results) == 0:
							logdata(logFile,"Could not find suitable job with job_stream - " + jobStream + " for retrying..",1)
							continue
						else:
							for resjobstr in results:
								jobStreammstr=resjobstr[0]
								ijobGroup=resjobstr[1]
							jobStreamarr.append(jobStreammstr)
					else:
						for resultobjstr in results:
							jobStreammstr=resultobjstr[0]
							ijobGroup=resultobjstr[1]
						#sql16="select job_stream_id from edw_job_streams where job_stream_id like '" + jobStreammstr + "%' and run_status in ('P','R')"
						sql16="SELECT job_stream_id,job_group_id from EDW_JOB_STREAMS where (JOB_STREAM_ID like '"   + jobStreammstr + "' and RUN_STATUS in ('P','R'))"
						sql16=sql16 + " or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and RUN_STATUS in ('P','R')) and workflow_type='SRC2STG' and active_ind='Y'"
						cursor1.execute(sql16)
						results=cursor1.fetchall()
						for resobjarr in results:
							jobStreamarr.append(resobjarr[0])
							ijobGroup=resobjarr[1]
			except Exception as e:
				colval="Error occurred while selecting JCT for retrying."
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				logdata(errFile,sql14,0)
				#print ("Error occurred while inserting sqls into JCT")
				logdata(errFile,str(e),1)
				Subject ="Error: Error occurred while selecting JCT for retrying."
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			try:
				#srcNumrows=int(open(BASE_DIR + '/' + envType +  '/' + ijobGroup  + '/config/' + sourceDb + '.' + sourceSchema + '.' + sourceTable).read())
				cursor1.execute("select src_rows from diy_master where reqid=" + str(reqid))
				results=cursor1.fetchall()
				for rowObj in results:
					srcNumrows=rowObj[0]
			except Exception as e:
				logdata(logFile,"Could not get source count during job retry",1)
		## End of indentation for retryflag=True
		if len(incColname) == 0:
			logdata(logFile,"Since no incremental column is passed " + sourceTable + " table will undergo full load every time",1)
		else:
			if sourcedbType.upper() == 'TERADATA':
				if len(connstr2) == 0:
					logdata(logFile,"Gathering source db (" + srcdbParams['SOURCE_HOST'] + ") connection details from repos db",0)
					try:
						#udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False)
						udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False,odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",runNumberFile="/apps/edwsfdata/python/scripts/.runNumber",logDir="/apps/edwsfdata/python/scripts/logs")
						conn2 = udaexec.connect(method="odbc", DSN="tdprodetloffload")
						cursor2 = conn2.cursor()
					except Exception as e:
						colval="Error occurred while connecting to source database."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while connecting to source database")
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while connecting to source database."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					try:
						logdata(logFile,"Checking and invoking base table details if passed object is view",0)
						sql13 = " exec PERFMETRICSDB.BASEVIEWCHECK ('" + sourceSchema + "','" + sourceTable + "')"
						cursor2.execute(sql13)
						results=cursor2.fetchall()
						for vwcheckobj in results:
							tblName=vwcheckobj[0]
						if tblName == '-1':
							#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
							colval="Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							objArray = tblName.split(".")
							srcBaseschema = objArray[0]
							srcBasetable = objArray[1]
							logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
					except Exception as e:
						#print ("Error occurred while checking base table from 1-1 view")
						colval="Error occurred while checking base table from 1-1 view."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print(str(e))
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while checking base table from 1-1 view."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
				incRetcd=updTdInccol(srcBaseschema,srcBasetable,cursor2,jobStream,incColname,cursor1,logFile,errFile)
				if conn2:
					conn2.close()
			elif sourcedbType.upper() == 'ORACLE':
				if len(connstr2) == 0:
					logdata(logFile,"Gathering source db (" + srcdbParams['SOURCE_HOST'] + ") connection details from repos db",0)
					try:
						connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
						conn2 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr2)
						cursor2=conn2.cursor()
					except Exception as e:
						colval="Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while connecting to source database")
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					try:
						logdata(logFile,"Checking if passed object is view",0)
						sql13 = "select count(1) from all_tables where table_name='" + sourceTable + "' and owner='" + sourceSchema + "'"
						cursor2.execute(sql13)
						results=cursor2.fetchall()
						for vwcheckobj in results:
							tblCnt=vwcheckobj[0]
						if tblCnt == '0':
							#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
							colval="Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested"
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							srcBaseschema = sourceSchema
							srcBasetable = sourceTable
							logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
					except Exception as e:
						#print ("Error occurred while checking base table from 1-1 view")
						colval="Error occurred while validating requested table for ingestion."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print(str(e))
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while validating requested table for ingestion."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
				incRetcd=updOracleInccol(srcBaseschema,srcBasetable,cursor2,jobStream,incColname,cursor1,logFile,errFile)
				if conn2:
					conn2.close()
		updDIYmstr(reqid,'current_phase','Launch_Talend',cursor1,errFile,logFile,emailAdd)
		if threadcount > 1:
			insDIYlines(reqid,'Split_Phase','Launch_Talend',cursor1,errFile,emailAdd)
		else:
			insDIYlines(reqid,'Invoking_Data_Load','Launch_Talend',cursor1,errFile,emailAdd)
		if len(jobStreamarr) > 1:
			if Thread_count > len(jobStreamarr):
				thread4upd=len(jobStreamarr)
				Thread_count=len(jobStreamarr)
			else:
				thread4upd=Thread_count
		else:
			thread4upd=1
		if retryFlag == 'FALSE':
			updDIYmstr(reqid,'Thread_count',str(thread4upd),cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'split_count',str(split4upd),cursor1,errFile,logFile,emailAdd)
		else:
			if tCnt != Thread_count and thread4upd > 1:
				updDIYmstr(reqid,'Thread_count',str(tCnt),cursor1,errFile,logFile,emailAdd)
		invokeTalend(envType,Talend_Scr_Path,Talend_Scr,ijobGroup,jobStreamarr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1)
		logdata(logFile,"Done with " + str(i+1) + " record.. Proceeding to next ..",0)
		logdata(logFile,"-----------------------------------------------------------------------------------------------------------------------------------------",0)
		## Check for completeness
		logdata(logFile,"Check for successfull ingestion for jobstream - " + jobStreammstr,0)
		if retryFlag == 'FALSE':
			noPieces=len(jobStreamarr)
		else:
			try:
				#cursor1.execute("select max(job_stream_seq_num)+1 from edw_job_streams where job_stream_id like '" + jobStream +"%'")
				cursor1.execute("SELECT max(job_stream_seq_num)+1 from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'  and active_ind='Y'")
				results=cursor1.fetchall()
				for nopiecur in results:
					noPieces=nopiecur[0]
				if len(results) == 0 or noPieces is None :
					noPieces=1
		
			except Exception as e:
				colval="Error occurred while pulling piece count for merging.. Please check and merge manually."
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				logdata(errFile,str(e),1)
				Subject ="Error: Error occurred while pulling piece count for merging.. Please check and merge manually."
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
		try:
			if len(jobStreamarr) > 1:
				sql16="select job_stream_id from edw_job_streams where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and run_status in ('P','R') and workflow_type='SRC2STG'"
				cursor1.execute(sql16)
				results=cursor1.fetchall()
				if len(results) == 0:
					try:
						logdata(logFile,"Successful completion of all child threads for " + sourceSchema + "." + sourceTable,1)
						cursor1.execute("Update edw_job_streams set run_status='C' where job_stream_id='" + jobStream + "' and run_status='A' and workflow_type='SRC2STG'")
						cursor1.execute("commit")
						SuccFlag=True
					except Exception as e:
						logdata(errFile,"Could not update master record for jobstream " + jobStreammstr + " though all child threads completed. Please check and update manually",1)
						logdata(errFile,str(e),1)
						Subject ="Error: Could not update master record for jobstream " + jobStreammstr + " though all child threads completed. Please check and update manually"
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				else:
					logdata(logFile,"Some of the child thread for jobstream " + jobStreammstr + " is not in completed status.. Hence retrying the same now..",1)
					newjobstreamArr=[]
					for newresultsobj in results:
						newjobstreamArr.append(newresultsobj[0])
					invokeTalend(envType,Talend_Scr_Path,Talend_Scr,ijobGroup,newjobstreamArr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1)
					logdata(logFile,"Checking for successful ingestion post retry for jobstream - " + jobStreammstr,0)
					cursor1.execute(sql16)
					results=cursor1.fetchall()
					if len(results) == 0:
						try:
							logdata(logFile,"Successful completion of all child threads for " + sourceSchema + "." + sourceTable,1)
							cursor1.execute("Update edw_job_streams set run_status='C' where job_stream_id='" + jobStream + "' and run_status='A' and workflow_type='SRC2STG'")
							cursor1.execute("commit")
							SuccFlag=True
						except Exception as e:
							logdata(errFile,"Could not update master record post retry for jobstream  " + jobStreammstr + " though all child threads completed. Please check and update manually",1)
							logdata(errFile,str(e),1)
							Subject ="Error: Could not update master record post retry for jobstream  " + jobStreammstr + " though all child threads completed. Please check and update manually"
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					else:
						logdata(errFile,"Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now..",1)
						Subject ="Error: Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now.."
						logdata(errFile,"Please find below the talend exception",1)
						findCmd='ls -rt /apps/edwsfdata/' + envType + '/' + ijobGroup + '/logs/*' + sourceTable + "* | egrep 'FF2SfSTG|OraSrc2FF|TDSrc2FF' | tail -1"
						talendLog=os.popen(findCmd).read().rstrip('\n')
						if len(talendLog) > 0:
							errStline=os.popen(" grep -n '\[FATAL\]' " + talendLog + ' | cut -d":" -f1 | tail -1').read().rstrip('\n')
							if len(str(errStline)) == 0:
								talendErr=os.popen('cat ' + talendLog + ' | tail -50').read()
								if len(talendErr) > 5:
									logdata(errFile,'Please find below the talend error',1)
									logdata(errFile,talendErr,1)
							else:
								errendLine=os.popen("wc -l " + talendLog + " | awk '{print $1}' ").read().rstrip('\n')
								lines2Grep=int(errendLine)-int(errStline)
								talendErr=os.popen('cat ' + talendLog + ' | tail -' + str(lines2Grep)).read()
								if len(talendErr) > 5:
									logdata(errFile,'Please find below the talend error',1)
									logdata(errFile,talendErr,1)
						else:
							logdata(errFile,"Could not locate talend log file",0)

						errMsg=Subject
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			else:
				## Its single stream ingestion
				sql16="select job_stream_id from edw_job_streams where job_stream_id like '" + jobStreammstr + "' and run_status in ('P','R') and workflow_type='SRC2STG'"
				cursor1.execute(sql16)
				results=cursor1.fetchall()
				if len(results) == 0:
					logdata(logFile,"Successful completion of data ingestion for " + sourceSchema + "." + sourceTable,1)
					SuccFlag=True
				else:
					logdata(logFile,"Jobstream " + jobStreammstr + " is not in compelted status.. Hence retrying the same now..",1)
					invokeTalend(envType,Talend_Scr_Path,Talend_Scr,ijobGroup,jobStreamarr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1)
					logdata(logFile,"Checking for successful ingestion post retry for jobstream - " + jobStreammstr,0)
					cursor1.execute(sql16)
					results=cursor1.fetchall()
					if len(results) == 0:
						logdata(logFile,"Successful completion of data ingestion for " + sourceSchema + "." + sourceTable,1)
						SuccFlag=True
					else:
						logdata(errFile,"Ingestion for jobstream " + jobStreammstr + " is not in completed status even after retrying.. Hence giving up now..",1)
						Subject ="Error: Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now.."
						logdata(errFile,"Please find below the talend exception",1)
						findCmd='ls -rt /apps/edwsfdata/' + envType + '/' + ijobGroup + '/logs/*' + sourceTable + "* | egrep 'FF2SfSTG|OraSrc2FF|TDSrc2FF' | tail -1"
						talendLog=os.popen(findCmd).read().rstrip('\n')
						if len(talendLog) > 0:
							errStline=os.popen(" grep -n '\[FATAL\]' " + talendLog + ' | cut -d":" -f1 | tail -1').read().rstrip('\n')
							if len(str(errStline)) == 0:
								talendErr=os.popen('cat ' + talendLog + ' | tail -50').read()
								if len(talendErr) > 5:
									logdata(errFile,'Please find below the talend error',1)
									logdata(errFile,talendErr,1)
							else:
								errendLine=os.popen("wc -l " + talendLog + " | awk '{print $1}' ").read().rstrip('\n')
								lines2Grep=int(errendLine)-int(errStline)
								talendErr=os.popen('cat ' + talendLog + ' | tail -' + str(lines2Grep)).read()
								if len(talendErr) > 5:
									logdata(errFile,'Please find below the talend error',1)
									logdata(errFile,talendErr,1)
						else:
							logdata(errFile,"Could not locate talend log file",0)
						errMsg=Subject
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		except Exception as e:
			logdata(errFile,"Could not query jobstream " + jobStreammstr + " and child for completeness",1)
			logdata(errFile,str(e),1)
			Subject ="Error: Could not query jobstream " + jobStreammstr + " and child for completeness"
			errMsg=Subject
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		if SuccFlag:
			updDIYmstr(reqid,'current_phase','Start_SF_Merge',cursor1,errFile,logFile,emailAdd)
			insDIYlines(reqid,'Launch_Talend','Start_SF_Merge',cursor1,errFile,emailAdd)
			logdata(logFile,"Since Data ingestion to SF completed succesfully now proceeding with stage merge and/or rowcount validation process",1)
			sfRowcnt=mergeSFstgtable(targetDb,targetSchema,targetTable,noPieces,logFile,errFile,envType,srcNumrows,cursor1,emailAdd,sfRole,sfWh)
			updDIYmstr(reqid,'tgt_rows',str(sfRowcnt),cursor1,errFile,logFile,emailAdd)
			if sfRowcnt == -1:
				logdata(errFile,"Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care",1)
				errMsg="Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care"
				Subject="Error: Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care"
				SuccFlag=False
			else:
				if sfRowcnt >= srcNumrows:
					logdata(logFile,"Succesfully validated SF ingestion. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows.",1)
					Subject = "Success: Succesfully completed ingesting data into " + targetDb + "." + targetSchema + "." + targetTable
					errMsg = "Succesfully validated SF ingestion. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows. Please review attached logs for details."
				else:
					logdata(errFile,"Row count mismatch between source and target. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows.",1)
					Subject = "Warning: Ingestion completed for " + targetDb + "." + targetSchema + "." + targetTable + ". However rowcount mismatch exists. Please check and take action."
					errMsg = "Row count mismatch between source and target. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows."
		if SuccFlag:
			try:
				logdata(logFile,"Now resetting job group and setting active ind to N for master job stream",1)
				if eType == 'ALL_DATA' and existFlag == 'FALSE':
					sql="update edw_job_streams set job_group_id='" + jobGroup + "',active_ind='N' where job_stream_id='" + jobStream + "'"
				else:
					sql="update edw_job_streams set job_group_id='" + jobGroup + "' where job_stream_id='" + jobStream + "'"
				cursor1.execute(sql)
				cursor1.execute('commit')
			except Exception as e:
				colval="Could not reset jobgroup for jobstream " + jobStream + ". Please check and update."
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)							
				logdata(errFile,str(e),1)
				continue
			if destSchema is not None:
				try:
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'current_phase','Launch_STG2BR',cursor1,errFile,logFile,emailAdd)
					insDIYlines(reqid,'Start_SF_Merge','Launch_STG2BR',cursor1,errFile,emailAdd)
					logdata(logFile,"Now launching stg2br script to push data from stg to ss/br",1)
					stg2brout = os.popen("/apps/edwsfdata/python/scripts/DIY_stg2br.py -e " + envType + " -j " + jobStream + " -a " + emailAdd + " -d " + destSchema).read()
					logdata(logFile,"Completed launching stg2br. Please find below the output",1)
					print(stg2brout)
					if "Error occurred during step" in stg2brout:
						stg2brFail=True
					else:
						stg2brFail=False
				except Exception as e:
					logdata(errFile,"Below exception occurred while launching stg2br for jobstream - " + jobStream,0)
					logdata(errFile,str(e),0)

			if len(incColname) > 0:
				results=cursor1.execute("select to_extract_dtm,to_extract_id from edw_job_streams where job_stream_id='" + jobStream + "' and workflow_type='SRC2STG'")
				for lastExtobj in results:
					lastExtdtm=lastExtobj[0]
					lastExtid=lastExtobj[1]
				if (lastExtdtm is None) and (lastExtid is None):
					if incRetcd == -1:
						logdata(errFile,"Exception occurred while pulling value for incremental column update.. So check and update manually",1)
						continue
					else:
						try:
							intdType=incRetcd.split('~')[0]
							intValue=incRetcd.split('~')[1]
							logdata(logFile,"Now updating current incremental column details for subsequent loads",1)
							if intdType == 'DATE':
								updSql="update  edw_job_streams set from_extract_dtm=to_date('" + str(intValue) + "','yyyy-mm-dd hh24:mi:ss'),to_extract_dtm=to_date('" 
								updSql=updSql +  str(intValue) + "','yyyy-mm-dd hh24:mi:ss'), extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
								updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
							elif intdType == 'ID':
								updSql="update  edw_job_streams set from_extract_id=" + str(intValue) + ",to_extract_id=" + str(intValue)
								updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
								updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
							else:
								logdata(errFile,"Could not update incremental data for jobstream " + jobStream + " post ingestion.",1)
								logdata(errFile,str(e),1)
								continue
							cursor1.execute(updSql)
							cursor1.execute("commit")
						except Exception as e:
							colval="Could not update incremental data for jobstream " + jobStream + ". Please check and update."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)							
							logdata(errFile,str(e),1)
							continue
				else:
					if incRetcd == -1:
						logdata(errFile,"Exception occurred while pulling value for incremental column update.. So check and update manually",1)
						continue
					else:
						try:
							intdType=incRetcd.split('~')[0]
							intValue=incRetcd.split('~')[1]
							logdata(logFile,"Now updating previous and current incremental column details for subsequent loads",1)
							if intdType == 'DATE':
								if lastUpdColFlag:
									updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_DTM=FROM_EXTRACT_DTM,PREVIOUS_TO_EXTRACT_DTM=TO_EXTRACT_DTM"
									updSql=updSql + ",from_extract_dtm=TO_EXTRACT_DTM,to_extract_dtm=to_date('" + str(intValue) + "','MM/DD/YYYY') "
									updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
									updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
								else:
									updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_DTM=FROM_EXTRACT_DTM,PREVIOUS_TO_EXTRACT_DTM=TO_EXTRACT_DTM"
									updSql=updSql + ",from_extract_dtm=TO_EXTRACT_DTM,to_extract_dtm=to_date('" + str(intValue) + "','yyyy-mm-dd hh24:mi:ss') "
									updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
									updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
							elif intdType == 'ID':
								updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_ID=FROM_EXTRACT_ID,PREVIOUS_TO_EXTRACT_ID=TO_EXTRACT_ID"
								updSql=updSql + ",from_extract_id=TO_EXTRACT_ID,to_extract_id=" + str(intValue)
								updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
								updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
							else:
								logdata(errFile,"Could not update incremental data for jobstream " + jobStream + " post ingestion.",1)
								logdata(errFile,str(e),1)
								continue
							cursor1.execute(updSql)
							cursor1.execute("commit")
						except Exception as e:
							colval="Could not update incremental data for jobstream " + jobStream + ". Please check and update."
							updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)							
							logdata(errFile,str(e),1)
							continue
			## Re-initialize job groups
			try:
				logdata(logFile,"Now re-initializing master job stream and deactivating child job streams for subsequent runs",1)
				#cursor1.execute("update edw_job_streams set run_status='P' where job_stream_id='" + jobStream + "'")
				cursor1.execute("update edw_job_streams set active_ind='N' where run_status='C' and regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'")
				#if len(splitQrydict) > 1:
				if len(jobStreamarr) > 1:
					updIncqry="update edw_job_streams set (previous_from_extract_dtm,from_extract_dtm,previous_to_extract_dtm,to_extract_dtm,previous_from_extract_id,from_extract_id,previous_to_extract_id,to_extract_id)="
					updIncqry+="(select previous_from_extract_dtm,from_extract_dtm,previous_to_extract_dtm,to_extract_dtm,previous_from_extract_id,from_extract_id,previous_to_extract_id,to_extract_id from edw_job_streams where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and run_status='P') where workflow_type='SRC2STG'"
					updIncqry+=" and active_ind='N' and regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'"
					cursor1.execute(updIncqry)
				cursor1.execute("commit")
			except Exception as e:
				colval="Could not re-initialize jobstream " + jobStream + " for subsequent runs. Please check and update."
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				logdata(errFile,str(e),1)
				continue
			if not stg2brFail:
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
				insDIYlines(reqid,'Launch_STG2BR','Completed_Ingestion',cursor1,errFile,emailAdd)
			else:
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'current_phase','SRC2STG_Success_But_STG2BR_Fail',cursor1,errFile,logFile,emailAdd)
				insDIYlines(reqid,'Launch_STG2BR','Completed_Ingestion',cursor1,errFile,emailAdd)
			if mailonSucc is not None:
				if mailonSucc.upper() == 'YES':
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
	#End of For loop indentation
#End of For DB/Table based ingestion indentation
logdata(logFile,"Closing Connections",0)
if conn:
	conn.close()
logdata(logFile,"Done with ingestion",1)
logFile.close()
errFile.close()
## update extract_type at the end



