import csv
import os
import glob
import requests
import psycopg2
import boto3
import logging
import sys
import time
import datetime
from datetime import datetime
from datetime import date, timedelta
import dateutil

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import String
#today = datetime.date.today()
#d = today - timedelta(days = 1)
d = datetime.now()
formatD = d.strftime("%Y%m%d")

ord_list = list(range(48,58,1)) + list(range(65,91,1)) + [95] + list(range(97,123,1))

def populate_df(seperator: str ='\t', encoding: str = 'utf8', quote_char: str = '"', quoting_lev: int =0):
    
    
    # S3 access keys and bucket name
    access_key: str = '' 
    secret_key: str = ''
    region_name: str = ''
    bucket_name: str = ''
    
        
    #Set up session to get object from S3 bucket
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
    
    #results = s3_client.list_objects(Bucket='cso-data', Prefix='acled_raw/ACLED')
    #key_value = results['Contents'][0].get('Key') #finally :-)
    
    #####Code to get latest modified date for file in s3########
    s3 = boto3.resource('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

    my_bucket = s3.Bucket('cso-data')

    last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)
    for file in my_bucket.objects.all():
    # print(file.key)
        file_date = file.last_modified.replace(tzinfo=None)
        if last_modified_date < file_date:
            last_modified_date = file_date
    #a = file.key
    #print(a)
    #print(last_modified_date)

    # you can have more than one file with this date, so you must iterate again
    for file in my_bucket.objects.all():
        if file.last_modified.replace(tzinfo=None) == last_modified_date:
            key_value = file.key
            print(key_value)
            
            #print(last_modified_date)
 
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key_value)
    
    files = pd.read_csv(response['Body'], quotechar=quote_char, quoting=quoting_lev)
      
    inp_cols = []
    file_count = 1
    
    
    for filename in files:
        engine = 'c'
        if len(seperator) > 1: engine = 'python'
        read_df = files
    # setup column list & df
        if file_count == 1:
            inp_cols = list(read_df.columns.values)
            staging_df = read_df.copy()
            read_df = None
   
    return staging_df
 
 
def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date. credit to Alex Riley, https://stackoverflow.com/a/25341965
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        dateutil.parser.parse(string, fuzzy=fuzzy)
        # recognize date or datetime
        if re.match('^(?:\d{1}|\d{2}|\d{4})[^0-9a-zA-Z](\d{1,4})[^0-9a-zA-Z](?:\d{1}|\d{2}|\d{4})([^0-9a-zA-Z]\d{2}[^0-9a-zA-Z]\d{2}[^0-9a-zA-Z]\d{2}[^0-9a-zA-Z]\d{2,5})?$', string):
            return True
    except ValueError:
        return False


def to_date(input_df, date_resp = 'coerce'):
    """try to convert to datetime any columns with 'dt' or 'date' at beginning or end of name or with a regex date in [0] 
    Parameters
    ----------
    input_df : pd.DataFrame
        a pandas dataframe
    date_resp : str
        see pd.to_datetime param errors for details
    Returns
    ----------
    input_df: pd.DataFrame: 
        DataFrame with recognized date columns converted to datetime
    """
    def convert_todate(ser, date_resp=date_resp):
        try:
            if any([piece for piece in ['dt', 'date'] if re.match('^{0}|.*{0}$'.format(piece), ser.name.lower())])\
                    and (is_date(str(ser[0]).strip())):
                while True:
                    try:
                        ser = pd.to_datetime(ser, infer_datetime_format=True, errors=date_resp)
                        print("Attempted to correct {} to datetime - did it work? {}\n"
                              .format(ser.name, ser.dtype.kind == 'M'))  # 'M' is numpy dtype for datetime
                    except:
                        date_resp = input("""Unable to convert :\n\n{} \n\nto datetime\nEnter `raise` if you've fixed the source data and would like to retry 
                        `ignore` to allow string instead, `coerce` to allow loss of non=datetime data & force conversion, or `quit`""".format(ser))
                        if date_resp == 'quit': sys.exit(1)
                        continue
                    break
        except:
            print("""Data appeared to be date but is likely not:\n\n{} """.format(ser))
        return ser
    
    print("attempting to fix dates")
    input_df = input_df.apply(convert_todate, axis=0)

    print("done fixing dates")
    
    return input_df


def str_lengths(input_df: pd.DataFrame):
    dtype_dict = dict()
    def max_len(label, content):
        if content.dtype == 'object':
            char_len = content.apply(str).map(len).max()
            dtype_dict[label] = String(char_len)
    for label, content in input_df.items():
        max_len(label, content)
    return dtype_dict
    
                           
def clean_cols4sql(input_df):
    # remove whitespace, replace with underscores
    input_df.columns = input_df.columns.str.strip()
    input_df.columns = input_df.columns.str.replace('[\s_]+', '_')
    input_df.columns = input_df.columns.str.lower()
    # leave only letters, numbers and underscores
    for j in range(len(input_df.columns.values)):
        input_df.columns.values[j] = "".join(i for i in input_df.columns.values[j] if ord(i) in ord_list)
    # make duplicate column names unique
    input_df.columns = pd.io.parsers.ParserBase({'names':input_df.columns})._maybe_dedup_names(input_df.columns)
    # reserved redshift words
    reserved_list = ['AES128', 'AES256', 'ALL', 'ALLOWOVERWRITE', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'AUTHORIZATION', 'AZ64', 'BACKUP', 'BETWEEN', 'BINARY', 'BLANKSASNULL', 'BOTH', 'BYTEDICT', 'BZIP2', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'CONSTRAINT', 'CREATE', 'CREDENTIALS', 'CROSS', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURRENT_USER_ID', 'DEFAULT', 'DEFERRABLE', 'DEFLATE', 'DEFRAG', 'DELTA', 'DELTA32K', 'DESC', 'DISABLE', 'DISTINCT', 'DO', 'ELSE', 'EMPTYASNULL', 'ENABLE', 'ENCODE', 'ENCRYPT', 'ENCRYPTION', 'END', 'EXCEPT', 'EXPLICIT', 'FALSE', 'FOR', 'FOREIGN', 'FREEZE', 'FROM', 'FULL', 'GLOBALDICT256', 'GLOBALDICT64K', 'GRANT', 'GROUP', 'GZIP', 'HAVING', 'IDENTITY', 'IGNORE', 'ILIKE', 'IN', 'INITIALLY', 'INNER', 'INTERSECT', 'INTO', 'IS', 'ISNULL', 'JOIN', 'LANGUAGE', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP', 'LUN', 'LUNS', 'LZO', 'LZOP', 'MINUS', 'MOSTLY13', 'MOSTLY32', 'MOSTLY8', 'NATURAL', 'NEW', 'NOT', 'NOTNULL', 'NULL', 'NULLS', 'OFF', 'OFFLINE', 'OFFSET', 'OID', 'OLD', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER', 'OUTER', 'OVERLAPS', 'PARALLEL', 'PARTITION', 'PERCENT', 'PERMISSIONS', 'PLACING', 'PRIMARY', 'RAW', 'READRATIO', 'RECOVER', 'REFERENCES', 'RESPECT', 'REJECTLOG', 'RESORT', 'RESTORE', 'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SNAPSHOT ', 'SOME', 'SYSDATE', 'SYSTEM', 'TABLE', 'TAG', 'TDES', 'TEXT255', 'TEXT32K', 'THEN', 'TIMESTAMP', 'TO', 'TOP', 'TRAILING', 'TRUE', 'TRUNCATECOLUMNS', 'UNION', 'UNIQUE', 'USER', 'USING', 'VERBOSE', 'WALLET', 'WHEN', 'WHERE', 'WITH', 'WITHOUT']
    reserved_dict = dict(zip(reserved_list, [i + '_' for i in reserved_list]))
    # for i in reserved_list:  # this acts directly on the statement - screws up on the 'create table part :/
    #     sql_stmnt = re.sub(r'\b({})\b'.format(i), i.lower() + '_', sql_stmnt, flags=re.IGNORECASE)
    input_df.columns = input_df.columns.str.upper()
    input_df.rename(columns=reserved_dict, inplace=True)
    return input_df


def clean_data(input_df: pd.DataFrame):
    # clean whitespace & sql invalid unicode 
    unis = '\x00-\x7F'
    transtab = str.maketrans(dict.fromkeys(unis, '..'))
    
    def rem_ws_unis(ser):
        if ser.dtype == 'object':
            ser = pd.Series([np.nan if pd.isnull(x) else \
                    (np.nan if isinstance(x, str) and x.isspace() else 
                    ("".join([i if ord(i) in ord_list else '...' for i in x])) if isinstance(x, str) else
                    x) for x in ser])
        return ser
    input_df = input_df.apply(rem_ws_unis, axis=0)
    
    return input_df


class DBC:
    
    def __init__(self,
        # AWS Redshift database
        dbname='gec_dev_db',
        host: str = 'url.us-gov-west-1.redshift.amazonaws.com',
        port: str = '',
        user: str = '',
        password: str = '',
        #schema: str = 'public',
        # S3 access keys and bucket name
        access_key: str = '', 
        secret_key: str = '',
        region_name: str = '',
        bucket_name: str = ''
    ):
        self._conn = psycopg2.connect(dbname=dbname, 
            host=host, port=port, user=user, password= password)
        self._cursor = self._conn.cursor()
        self.sa_conn = create_engine("{0}://{1}:{2}@{3}:{4}/{5}".format('postgres', user, password, host, port, dbname))
        # sa.create_engine('postgres://[LOGIN]:[PWORD]@shippy.cx6x1vnxlk55.us-west-2.redshift.amazonaws.com:5439/dbname')
        self.s3_client = boto3.client('s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name)
        self.access_key = access_key
        self.secret_key = secret_key

    
    # redshift functions
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def sql_drop_table(self, table_name):
            drop_statement = """DROP TABLE IF EXISTS {} ;""".format(table_name)
            self.cursor.execute(drop_statement)
            print('{} has been dropped'.format(table_name))
         
        
    def sql_create_table(self, input_df, table_name, dtype_dict):
        schema_name = None
        schema_txt = "table_schema = '{}' and ".format(schema_name)
        if '.' in table_name:
            schema_name = table_name.split('.')[0]
            schema_txt = "table_schema = '{}' and ".format(schema_name)
            table_name = table_name.split('.')[1]
        self.cursor.execute(
            "select count(*) from information_schema.tables \
            where {} table_name='{}'".format(schema_txt, table_name))
            
        if self.cursor.fetchone()[0]>0:
            print('table already exists')
            
        else:
            if schema_name: table_name = schema_name+'.'+table_name
            create_table_statement = pd.io.sql.get_schema(input_df, '|'+table_name+'|', con=self.sa_conn, dtype=dtype_dict)
            create_table_statement = create_table_statement.replace('"|','').replace('|"','')
            print('creating table: \n\t', create_table_statement)
            self.execute(create_table_statement)
        
    def sql_count_check(self, table_name, when=''):        
        #self._cur.execute("begin;")
        sql_row_count="""select count(*) from {} ;""".format(table_name)
        self.cursor.execute(sql_row_count)
        results = self.cursor.fetchone()
        id = results[0]
        print('Number of rows in {} is: '.format(table_name) ,id)
             

        # s3 functions
    def create_bucket(self, bucket_name):
        # session = boto3.session.Session() #        current_region = session.region_name
        current_region = 'us-gov-west-1'
        print('gonna create bucket')
        bucket_response = self.s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
            'LocationConstraint': current_region})
        print('bucket created: ', bucket_name, current_region)
        return bucket_name, bucket_response

        
    #def csv_to_s3(self, file_name, bucket_name='cso-data'):
        #file = open(file_name, 'rb')
        #self.s3_client.upload_fileobj(file, bucket_name, 'mpc_test/' + file_name)
        #print("file [{0}] successfully loaded to s3 bucket [{1}]".format(file_name,bucket_name))
        
            # between s3 and redshift
    def s3_to_redshift(self, file_name, table_name, delimiter=','):        
        copy_statement="""copy {0} from '{1}' \
        credentials 'aws_access_key_id={2};aws_secret_access_key={3}' \
        DELIMITER '{4}' csv DATEFORMAT 'auto' BLANKSASNULL FILLRECORD \
        EMPTYASNULL ACCEPTINVCHARS NULL AS 'null_string' IGNOREHEADER 1;"""\
        .format(table_name, file_name, self.access_key, self.secret_key, delimiter)
        # print(copy_statement)
        self.cursor.execute(copy_statement)

    def dir_to_redshift(self, table_name,
        bucket_name: str = 'cso-data' 
        #dir=os.path.join(os.getcwd(),'./data'), 
        #recurse = True
    ):
            # S3 access keys and bucket name
        access_key: str = '' 
        secret_key: str = ''
        region_name: str = ''
        bucket_name: str = ''
            
        #s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        #results = s3_client.list_objects(Bucket='cso-data', Prefix='acled_raw/ACLED')
        #files = results['Contents'][0].get('Key') #finally :-)
        
            #####Code to get latest modified date for file in s3########

        s3 = boto3.resource('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

        my_bucket = s3.Bucket('cso-data')

        last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)
        for file in my_bucket.objects.all():
            # print(file.key)
            file_date = file.last_modified.replace(tzinfo=None)
            if last_modified_date < file_date:
                last_modified_date = file_date
                #a = file.key
                #print(a)
        #print(last_modified_date)

        # you can have more than one file with this date, so you must iterate again
        for file in my_bucket.objects.all():
            if file.last_modified.replace(tzinfo=None) == last_modified_date:
                files = file.key
                print(files)
        
        
        
        #os.chdir(dir)
        #files = glob.glob('**/*', recursive=recurse)
        #exts = ['.txt', '.csv']
        #files = [file for file in files if any([ext for ext in exts if file.endswith(ext)])]
        #for file_name in files:
            #self.csv_to_s3(file_name, bucket_name)
        self.s3_to_redshift('s3://' + bucket_name + '/'+ files, table_name)
        #print('{0} files in {1} processed {2}recursively'.format(len(files), dir, '' if recurse else 'non-'))
        print('Data has been copied to {}'.format(table_name))
    

def main(table_name: str = 'scriptload_{}'.format(formatD), bucket_name = 'cso-data'):
    #table_name = 'cso.mpc_test'
    with DBC() as dbc: 
        #print(datetime.now().strftime('%Y%m%d_%H%M%S'))
        print('starting_{}'.format(formatD))
        dbc.sql_drop_table(table_name)
        input_df = populate_df()
        input_df = to_date(input_df)
        input_df = clean_cols4sql(input_df)
        dtype_dict = str_lengths(clean_data(input_df))
        dbc.sql_create_table(input_df, table_name, dtype_dict)
        dbc.dir_to_redshift(table_name)
        dbc.sql_count_check(table_name)
        #print('finished:',datetime.now().strftime('%Y%m%d_%H%M%S'))
        print('finished_{}'.format(formatD))


if __name__ == '__main__':
    main(sys.argv[1:])
    
