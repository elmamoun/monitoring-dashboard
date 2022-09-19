import sys
sys.path.append('/home/cdsw/code')
import numpy as np
import pandas as pd
from datetime import datetime
from monLogger import *
from importlib import reload
from pyspark.sql import SparkSession
startTime = datetime.now()  

from unedic_data_pipeline.load import * 

application_name = "chargement_donnees_sage"
spark_session = SparkSession\
        .builder\
        .appName(application_name)\
        .getOrCreate() 

dbname = "raw_reporting_treso"
tbname = "export_sage"
ecriture = "append"
dp= "/data/raw_layer"

spdata = spark_session.sql('SELECT * FROM raw_reporting_treso.export_sage_code_flux')
create_table(spark_session,depot =dp ,spark_data_frame =spdata,db_name =dbname,table_name=tbname,logger=None)
spdata = spark_session.sql('SELECT * FROM raw_reporting_treso.export_sage_code_flux_historique')
add_data_to_table(spark_session,spark_data_frame=spdata,db_name=dbname,table_name= tbname,mode_ecriture = ecriture)  




