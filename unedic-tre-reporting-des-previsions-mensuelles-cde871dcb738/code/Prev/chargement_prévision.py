import sys
sys.path.append('/home/cdsw/code')
import pandas as pd 
import numpy as np 
from utils import * 
import configparser
from pyspark.sql import SparkSession
import ast
from monLogger import * 
import re
import json 
from Prev.single_sheet_process import prevision_data_ingestion_single_sheet 
from Prev.multiple_sheet_process import prevision_data_ingestion_multiple_sheet
import warnings
warnings.filterwarnings("ignore")

#---------------------------------------------------------------------------------------------------------#
#-------------------------------configuration du fichier de config----------------------------------------#
#---------------------------------------------------------------------------------------------------------#

config = configparser.ConfigParser(interpolation= None)
config.read('config.ini')
normal_values_nature_montant = ast.literal_eval(config.get("normal_values", "nature_montant"))
processed_files = ast.literal_eval(config.get("treatment", "processed_files"))

#---------------------------------------------------------------------------------------------------------#
#----------------------choix de l'emplacement de récupération des fichiers sources------------------------#
#---------------------------------------------------------------------------------------------------------#

emplacement = "/mnt/Plateforme/1 - Collecte des données sources auprès des OP/19 - Tresorerie/Prévisions DEA mensualisées/ajout_plateforme"
extension = "*.xlsx"


list_filename = get_files_in_directory(emplacement,extension)
single_sheet_files = list()
multiple_sheet_files = list()

#---------------------------------------------------------------------------------------------------------#
#---------------------------------Génération de la session Spark------------------------------------------#
#---------------------------------------------------------------------------------------------------------#

applicationName = "chargement_prévision_trésorerie"
spark = SparkSession\
        .builder\
        .appName(applicationName)\
        .getOrCreate()
#---------------------------------------------------------------------------------------------------------#
#-----------------------------------Création de l'objet de logging----------------------------------------# 
#---------------------------------------------------------------------------------------------------------#

log_path = "/home/cdsw/logs/Prev"
log_file_name_prefix = "chargement_prevision"
logger = get_or_create_logger(log_path,log_file_name_prefix)
logger.info(f"------------------------DEBUT DE CHARGEMENT DES FICHIERS-----------------------------------")

#---------------------------------------------------------------------------------------------------------#
#------------------------------------Choix de la table d'écriture-----------------------------------------#
#---------------------------------------------------------------------------------------------------------#

depot = "/data/raw_layer"
db_name = "raw_reporting_treso"
table_name="treso_dea_prev"


startTime = datetime.now()
to_process = [e for e in list_filename if e.split("ajout_plateforme/")[1] not in processed_files]
#---------------------------------------------------------------------------------------------------------#
#------------------répartir les fichier à traiter selon le nombre des feuilles excel----------------------#
#---------------------------------------------------------------------------------------------------------#
for filename in to_process :
    if len(openpyxl.load_workbook(filename).sheetnames) == 1:
        logger.info(f"Le fichier {filename.split('plateforme/')[1]} a été ajouté comme fichier à feuille unique")
        single_sheet_files.append(filename)
    else:
        multiple_sheet_files.append(filename)
        logger.info(f"Le fichier {filename.split('plateforme/')[1]} a été ajouté comme fichier à multiple feuilles")


#---------------------------------------------------------------------------------------------------------#
#------------------------------supression de la table existante-------------------------------------------#         
#---------------------------------------------------------------------------------------------------------#

logger.info("supression de l'ancienne table HIVE")
# drop_table(spark,depot,db_name,table_name,logger)

#---------------------------------------------------------------------------------------------------------#
#--------------------Ecriture des tables Hive depuis les fichiers excel-----------------------------------# 
#---------------------------------------------------------------------------------------------------------#

for filename in single_sheet_files : 
    try:
        prevision_data_ingestion_single_sheet(spark, depot, db_name, table_name, filename, extension, normal_values_nature_montant, logger)
        processed_files.append(filename.split('plateforme/')[1])
    except: 
        logger.info(f"Le fichier {filename.split('plateforme/')[1]} a failé")

for filename in multiple_sheet_files : 
    try:
        prevision_data_ingestion_multiple_sheet(spark, depot, db_name, table_name, filename, extension, normal_values_nature_montant, logger)
        processed_files.append(filename.split('plateforme/')[1])
    except:
        logger.info(f"Le fichier {filename.split('plateforme/')[1]} a failé")

#Update the processed files list
procfiles = config["treatment"]
procfiles["processed_files"] = str(processed_files)

#Write changes back to file
with open('config.ini', 'w') as configfile:
    config.write(configfile)


logger.info("durée de traitement = "+ str(datetime.now()-startTime))   
logger.info(f"------------------------FIN DE CHARGEMENT DES FICHIERS-----------------------------------")


