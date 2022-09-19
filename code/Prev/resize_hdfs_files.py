from unedic_data_pipeline.monitoring.hive_monitoring import *
from unedic_data_pipeline.monitoring.monitoring import *
from unedic_data_pipeline.monitoring.file_monitoring import *
from unedic_data_pipeline.monitoring.df_monitoring import * 

from pyspark.sql import SparkSession
import sys
sys.path.append('/home/cdsw/code')
from utils import * 
from monLogger import * 

## définition de la db et de la table
db_name = "raw_reporting_treso"
table_name="treso_dea_prev"                                         ######### CHANGE HERE #########

## création du fichier log
log_path = "/home/cdsw/logs/resize"
log_file_name_prefix = "hdfs_resize"
log = get_or_create_logger(log_path,log_file_name_prefix)

## création de la session spark
applicationName = "resize small files"
spark = SparkSession\
        .builder\
        .appName(applicationName)\
        .getOrCreate()


resize_hive_table_files(spark, db_name, table_name, log )