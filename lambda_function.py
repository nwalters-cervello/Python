import snowflake.connector
import os
import pandas as pd
import smtplib
import boto3
import logging

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SNS client for Ireland region
session = boto3.Session(
    region_name="eu-west-1"
)
sns_client = session.client('sns')




#Specify that /tmp/ should be used (only useable folder on AWS Lambda)
os.chdir('/tmp')


#specify/decode environment variables (defined in AWS UI)
#NB: unhash the ENCYPTED_ variables (+ comment out equivalent os.environ vars) if encryption enabled in AWS

ACCOUNT="wf13296.eu-west-1"
USER="APATEL_CVL"
PASSWORD="Cervello@123"
SCHEMA="PUBLIC"
WAREHOUSE="CERVELLO_TEST"
DATABASE="GDW_DEV"


def lambda_handler(event, context):


    # Connecting to Snowflake using the default authenticator
    cnx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT)

    #Initialise Snowflake
    cnx.cursor().execute("USE warehouse CERVELLO_TEST")
    cnx.cursor().execute("USE GDW_DEV.PUBLIC")
    cnx.cursor().execute("USE role SYSADMIN")
    cnx.cursor().execute("alter warehouse CERVELLO_TEST resume IF SUSPENDED;")


query_string1 = "SET New_week = (SELECT W2.Accounting_Week_Num FROM CZ_PERSIST.NFS_DBO_Accounting_Week W1, CZ_PERSIST.NFS_DBO_Accounting_Week W2, (SELECT MIN(Accounting_Week_Num) AS Min_Week FROM CZ_PERSIST.NFS_DBO_Agency_Table WHERE (Agency_Status_Type_Code='OP')) AS XXX WHERE XXX.Min_Week = W1.Accounting_Week_Num AND W2.Week_Count = (W1.Week_Count - 1));"
query_string2 = "SET Curr_Week = (SELECT MAX(Week_Num) FROM CZ_REPORTING.BIT_F_AGREEMENTWEEKHISTORY);"
query_string3 = "SELECT $Curr_Week;"
query_string4 = "SELECT CASE WHEN $New_week > $Curr_Week THEN 1 ELSE 0 END AS flag;"
query_string5 = "SELECT $flag;"
try:
    for i in range(1,5):
        ex = 'query_string'+str(i)
        cnx.cursor.execute(ex)
        flagVal = int(cnx.cursor.execute(query_string5).fetchone())

finally:
    cnx.cursor.close()
if flagVal == 1:
    print("Run")
elif flagVal == 0:
    print("Skip")
else:
    print("Unexpected flag value")


