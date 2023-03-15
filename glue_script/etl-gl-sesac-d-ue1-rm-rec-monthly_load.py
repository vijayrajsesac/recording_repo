"""
__script__		: etl-gl-sesac-d-ue1-rm-rec-monthly_load
__description__ : Pyspark script to read tsv files from S3 bucket and write into Oracle Table and use it for Analytics and other
                  transformations as per business.
__author__		: Vijay Raj

"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
import boto3

def get_connection_details(glue_client, conn_name, driver):
    """
    This function reads the JDBC connection properties and returns them in dictionary object.
    Parameters:
        glue_client         :   Public endpoint for Glue Services.
        conn_name           :   Connection Object name where all the connection related properties are stored.
        driver              :   Driver Name.
        partition_threshold :   Number of Rows to be kept in a partition.
    Returns:
        conn_info           :   Dictionary Object with all the connection related values.
    """
    response = glue_client.get_connection(Name=conn_name)
    properties = response['Connection']['ConnectionProperties']
    conn_info = {'CONN_URL': properties['JDBC_CONNECTION_URL'],
                 'USERNAME': properties['USERNAME'],
                 'PASSWORD': properties['PASSWORD'],
                 'DRIVER': driver}
    return conn_info


def read_and_verify(spark, conn_info, logger, tb_name, current_date, filename, df_count):
    """
    This function reads the data from oracle table to verify newly written data.
    Parameters:
        spark       :   Spark Session Object.
        conn_info   :   Connection Object name where all the connection related properties are stored.
        logger      :   Logger Object.
        tb_name     :   Table Name from where data is to be read.
        rec_data_df :   dataframe of loaded tsv file.
    Returns:
        None
    """
    # Compare tsv count with Oracle table read count
    query = f'''(SELECT STG_ID, FILE_NAME, CREATION_DATE from ''' + tb_name + ''')'''
    count_df = spark.read \
            .format('jdbc') \
            .option('url', conn_info['CONN_URL']) \
            .option("dbtable", query) \
            .option("user", conn_info['USERNAME']) \
            .option("password", conn_info['PASSWORD']) \
            .option("driver", conn_info['DRIVER']) \
            .option("numPartitions", 8) \
            .option("fetchsize", "10000") \
            .load()
    count_df_res = count_df.filter((F.col('file_name') == filename) & (F.col('creation_date') == current_date))
    row_count = count_df_res.count()
    logger.info('ORACLE_TBL_ROW_COUNT: ' + str(row_count) + ' | TSV_ROW_COUNT: ' + str(df_count))
    return row_count


def insert_log(spark, filename, sender, df_count, row_count, logDataSchema):
    """
    This function vaerifies the data wriiten to oracle table and writes the result to logging table.
    Parameters:
        spark         :   Spark Session Object.
        filename      :   Uploaded filename.
        sender        :   Sender ID.
        df_count      :   Dataframe rows count.
        row_count     :   No. of rows inserted into oracle table.
        logDataSchema :   Schema for log table.
    Returns:
        None
    """
    try:
        # Log Table Data
        log_data = [(filename, int(sender), df_count, row_count)]
        log_df = spark.createDataFrame(data=log_data,schema=logDataSchema)
        log_df = log_df.withColumn("creation_date", F.current_date())
        log_df.show(truncate=False)
        log_df.write.format('jdbc').option('driver',conn_info['DRIVER']).option('url',conn_info['CONN_URL']).option('user',conn_info['USERNAME']).option('password',conn_info['PASSWORD']).mode('append').option('dbtable',log_table).save()
        # log_df.write.format('jdbc').option('driver',conn_info['DRIVER']).option('url',conn_info['CONN_URL']).option('user',conn_info['USERNAME']).option('password',conn_info['PASSWORD']).mode('overwrite').option('dbtable',log_table).save()
    except Exception as error:
        logger.error(error)
        

## @params: [JOB_NAME] .
args = getResolvedOptions(sys.argv, ['JOB_NAME','sender','path','sender_ids','bucket','table','log_table'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)

# print(args['sender'])
# print(args['path'])
# print(args['bucket'])

sender = args['sender']
path = args['path']
sender_ids = args['sender_ids']
bucket = args['bucket']
table = args['table']
log_table = args['log_table']

driver = 'oracle.jdbc.driver.OracleDriver'
connection = 'cn-gl-sesac-d-ue1-rmd-jdbc-oracle'
final_path = 's3://' + bucket + '/' + path

sender_list = sender_ids.split(',')

customSchema = StructType([ \
    StructField("hfa_song_code",StringType(),True), \
    StructField("track_id",StringType(),True), \
    StructField("title",StringType(),True), \
    StructField("artist", StringType(), True), \
    StructField("album", StringType(), True), \
    StructField("isrc", StringType(), True), \
    StructField("writer", StringType(), True), \
    StructField("upc", StringType(), True), \
    StructField("composition_title", StringType(), True), \
    StructField("matched_amount", FloatType(), True), \
    StructField("unmatched_amount", FloatType(), True), \
    StructField("recording_title", StringType(), True), \
    StructField("recording_full_title", StringType(), True), \
    StructField("recording_version_title", StringType(), True), \
    StructField("dsp", StringType(), True), \
    StructField("dsp_m_number", IntegerType(), True), \
    StructField("label", StringType(), True), \
    StructField("catalog_number", StringType(), True), \
    StructField("sender_resource_id", IntegerType(), True), \
    StructField("sender_id", IntegerType(), True)
  ])

logDataSchema = StructType([ \
    StructField("file_name",StringType(),True), \
    StructField("sender_id",IntegerType(),True), \
    StructField("file_record_count", IntegerType(), True), \
    StructField("table_record_total", IntegerType(), True)
  ])


logger.info('SENDER_ID :' + str(sender))
logger.info('UPLOADED_FILE_PATH :' + str(final_path))
filename = path.split('/')[4].split('=')[1] + '_' + path.split('/')[5]
logger.info('FILENAME :' + filename)

rec_data_cnt = 0

try:
    # rec_data_df = spark.read.schema(customSchema).csv(final_path, sep=r'\t', header=False)
    rec_data_df = spark.read.schema(customSchema).option("multiline", True).option("quote", "\"").option("escape", "\"").csv(final_path, sep=r'\t', header=False)
    rec_data_df = rec_data_df.withColumn("creation_date", F.current_date())
    rec_data_df = rec_data_df.withColumn("file_name", F.lit(filename))
    rec_data_df = rec_data_df.withColumn("catalog_number", F.trim(F.col("catalog_number")))
    
    # getting connection user name & password to pass to jdbc connection
    client = boto3.client('glue')
    conn_info = get_connection_details(client, connection, driver)
    logger.info(str(conn_info))
    
    rec_data_cnt = rec_data_df.count()
    
    rec_data_df.write.format('jdbc').option('driver',conn_info['DRIVER']).option('url',conn_info['CONN_URL']).option('user',conn_info['USERNAME']).option('password',conn_info['PASSWORD']).mode('append').option('dbtable',table).save()
    
    # rec_data_df.write.format('jdbc').option("numPartitions", 12).option("isolationLevel", "NONE").option('driver',conn_info['DRIVER']).option('url',conn_info['CONN_URL']).option('user',conn_info['USERNAME']).option('password',conn_info['PASSWORD']).mode('append').option('dbtable',table).save()
    
    # rec_data_df.write.format('jdbc').option('driver',conn_info['DRIVER']).option('url',conn_info['CONN_URL']).option('user',conn_info['USERNAME']).option('password',conn_info['PASSWORD']).mode('overwrite').option('dbtable',table).save()
    
    logger.info('-- DATA FILE WRITE SUCCEEDED --')
except Exception as error:
    logger.error(error)
finally:
    # Verification Function
    logger.info('-- VERIFICATION FUNCTIONALITY FOR LOGGING STARTED --')
    row_count = read_and_verify(spark, conn_info, logger, table, F.current_date(), filename, rec_data_cnt)
    insert_log(spark, filename, sender, rec_data_cnt, row_count, logDataSchema)
    logger.info('-- FINALLY EXECUTION COMPLETED --')
