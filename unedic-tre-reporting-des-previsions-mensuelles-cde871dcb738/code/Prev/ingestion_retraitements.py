import sys
sys.path.append('/home/cdsw/code')
import pandas as pd 
import numpy as np 
from utils import * 
import configparser
from pyspark.sql import SparkSession
import ast
from monLogger import * 
import json 
from Prev.single_sheet_process import prevision_data_ingestion_single_sheet 
from Prev.multiple_sheet_process import prevision_data_ingestion_multiple_sheet, traiter_retraitement
import warnings
warnings.filterwarnings("ignore")


from unedic_data_pipeline.load import create_table


############ list des fichiers à traiter ############

emplacement = "/mnt/Plateforme/1 - Collecte des données sources auprès des OP/19 - Tresorerie/Prévisions DEA mensualisées"
extension = "*.xlsx"
list_filename = get_files_in_directory(emplacement,extension)
dft_files = [e for e in list_filename if 'DFT' in e]


############ session spark et variable de create table ############

applicationName = "chargement des données de retraitements "
spark = SparkSession\
        .builder\
        .appName(applicationName)\
        .getOrCreate()

depot = "/data/raw_layer"
db_name = "raw_reporting_treso"
table_name="retraitement_dft"

########## Création de l'objet de logging ##########

log_path = "/home/cdsw/logs/retraitement"
log_file_name_prefix = "chargement_retraitements"
log = get_or_create_logger(log_path,log_file_name_prefix)
log.info(f"------------------------DEBUT DE CHARGEMENT DES FICHIERS-----------------------------------")

        
########## créer les pandas dataframe final contenant tous le retraitements réalisés du côté de la DFT ##########

pandas_db = pd.DataFrame()

for filename in dft_files[3:]:
    pandas_db = pd.concat([pandas_db, traiter_retraitement(filename, log)])


########## charger les données dans une table Hive ##########
load_aggregate_table_to_hdfs(spark, depot, db_name, table_name, pandas_db, log)




######## resize les fichier crées 