#!pip3 install openpyxl
import xlrd
from  pyspark.sql.functions import input_file_name,lit,udf,expr,col,isnan,when
from datetime import  datetime
import subprocess
import os
import pandas as pd
from glob import glob
import dateutil
from dateutil.parser import parse
import math
import numpy as np
import openpyxl



"""-------------------------------------------------------------------------
Obtention de la date d'extraction d'un fichier:
AAAA-MM-DD
----------------------------------------------------------------------------""" 


def get_date_extraction_m_yy(filename):
    """
    La fonction permet à partir du nom du fichier de récupérer la date de prévision
    
    params:
        -str :filename : nom du fichier sur lequel on souhaite effectuer l'opération
    """
    
    filename = filename.split('mensualisées/')[1]
    filename = filename.replace(".", " ")
    filename = filename.replace("-", " ")
    date = ""
    for e in filename:
        try : 
            e = int(e)
        except: 
            e =e 
        if e == " " :
            date += e 
        elif type(e) == int:
            date += str(e)     
    try :
        if type(int(date)) == int:
            year = int('20' + date.split('20')[1])
            month = int(date.split('20')[0][2:4])
            day = int(date.split('20')[0][:2])

            date = str(year) +' '+ str(month) + ' '+ str(day) 

    except :
        pass
    return dateutil.parser.parse(date)


"""
DATA QUALITY: 
Cette fonction permet de vérifier sur une base de 3 colonnes si les données ont été récupérées correctement depuis les fichiers excel
params : 
 - df : dataframe dont on souhaite vérifier la qualité 
 - normal_values_nature_montant : une liste de chaines de caractères qui reprend toutes les valeurs possible de la colonne nature montant, 
 toute valeur qui ne fait pas partie de cette liste sera tagé anormale. 

"""


def data_quality(df,normal_values_nature_montant):
    """  
    Cette fonction permet de vérifier le résultat de la transformation des fichiers excel
    params: 
    - df: dataframe à vérifier 
    - normal_values_nature_montant : la liste des valeurs attendues pour la colonne 'nature_montant' 
    """
    clean_data = dict()
    
    # vérifier la colonne mois_nom
    normal_values_mois_nom = ['total', 'janvier','février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'aout',
                              'septembre', 'octobre', 'novembre', 'décembre']
    somme = 0 
    for value in normal_values_mois_nom:
        somme += df['mois_nom'].str.contains(value, case = False).sum()
    clean_data['mois_nom'] = (somme == len(df))
    
    # vérifier la colonne type montant 
    somme= df['type_montant'].str.contains('réalisé', case = False).sum() +\
    df['type_montant'].str.contains('estimé', case = False).sum() +\
    df['type_montant'].str.contains('prévu', case = False).sum() + \
    df['type_montant'].str.contains('observé', case = False).sum() 
    clean_data['type montant'] = (somme == len(df))
    
    # vérifier la colonne nature montant 
    
    somme = df['nature_montant'].isin(normal_values_nature_montant).sum()
    clean_data['nature montant']  = (somme == len(df))
    
    clean_data_ = True
    for e in list(list(clean_data.values())):
        clean_data_ = clean_data_ * e 
    
    return clean_data, clean_data_



"""-------------------------------------------------------------------------
Traitement des données:

Cette fonction permet de récupérer les éléments successifs d'une liste python 
----------------------------------------------------------------------------""" 


def successive_numbers(r):
    f = list()
    for i in range(len(r)-1):
        if r[i+1] - r[i] ==1:
            f.append(r[i])
            f.append(r[i+1])
        else:
            break
    return list(set(f))


def check_all_unnamed(df):
    unnamed = [e for e in df.columns if 'Unnamed' in e ] 
    return len(unnamed) == len(df.columns) 

"""-------------------------------------------------------------------------
Traitement des données:

Cette fonction retourne les index de lignes et de colonnes qui délimitent le tableau souhaité 
----------------------------------------------------------------------------"""     




def check_index(df): 
    dff =df.copy()
    ## récupérer la ligne d'arrêt en parcourant les colonnes jusqu'aà trouver 'emprunts'
    
    ## log warning si le len de la liste est différent de 1 #######################
    dff['trash'] = np.nan

    try : 
        index_1 = list()
        index_2 = list()
        column = 0
        while (index_1 == [] and index_2 == [] and column <3):
            index_1 = [  i for i in range(len( dff[dff.columns[column]].str.contains('solde financier', case =False ).tolist()))
                     if       dff[dff.columns[column]].str.contains('solde financier', case =False ).tolist()[i] == True ]
            index_2 = [  i for i in range(len( dff[dff.columns[column]].str.contains('variation de trésorerie', case =False ).tolist()))
                     if       dff[dff.columns[column]].str.contains('variation de trésorerie', case =False ).tolist()[i] == True ]

            column += 1 
        column -= 1

        index = [e for e in [index_1,index_2] if len(e)>0][0]
        stop_l = index[0]+2


        ## récupérer la colonne d'arrêt en cherchant les colonnes vides qui délimitent le tableau 
        df_ = dff[:stop_l] 
        df_.loc[-1] = df_.columns
        float_l = []
        useful_l = []
        for i in range(len(df_.columns)):
            if column_is_float(df_, df_.columns[i]):
                float_l.append(i)
            if column_not_useful(df_, df_.columns[i]):
                useful_l.append(i)

        float_l= successive_numbers(float_l)
        useful_l = [e for e in useful_l if e > 10]
        stop_c = (float_l[0]-1,min( float_l[-1]+1, useful_l[0] ))

        return (stop_l,stop_c,column)
    
    except: 

        print('Le fichier est déjà traité.') 
        return (len(df), [0,len(df.columns)],0)

        
"""-------------------------------------------------------------------------
Traitement des données:

Cette fonction permet de dire si la colonne est utile ou pas
----------------------------------------------------------------------------"""     

    
def column_not_useful(df,column):
    months = ['janvier', 'février','mars','avril','mai','juin','juillet','août','septembre','octobre','novembre', 'décembre',
         'total']
    for v in months:
        if (True in df[column].str.contains(v, case = False).tolist()):
            return False
    return True
    
    
"""-------------------------------------------------------------------------
Traitement des données:

Cette fonction permet de dire si la colonne est majoritairement représenté par des valeurs réelles ou des strings/null
----------------------------------------------------------------------------"""     
    
def column_is_float(df, column):
    df[column] = df[column].astype('float', errors= 'ignore' )
    df[column].replace( np.nan, 'null',inplace = True)
    float_number, str_number, nan_number = 0, 0,0
    for i in range(len(df)):
        if (type(df.iloc[i][column]) == float) :
            float_number += 1
        elif( type(df.iloc[i][column]) == str) :
            if df.iloc[i][column] != 'null':
                str_number += 1
            else: 
                nan_number += 1 

    return( float_number > str_number) & (float_number > nan_number)





"""-------------------------------------------------------------------------
Récupération des fichiers:

Cette fonction permet de récupérer les fichiers depuis l'emplacement réseau
----------------------------------------------------------------------------""" 

def get_files_in_directory(emplacement,extension):
    filenames = [file
                   for path, subdir, files in os.walk(emplacement)
                   for file in glob(os.path.join(path, extension))]
    return filenames


"""-------------------------------------------------------------------------
Récupération des fichiers:

Cette fonction permet de charger les fichiers csv en pandas dataframe 
----------------------------------------------------------------------------""" 
def load_csv_files_from_directory(filename):
    df= pd.read_csv(filename,sep=';')
    return df



"""-------------------------------------------------------------------------
Récupération des fichiers:

Cette fonction permet de charger les fichiers xlsx en pandas dataframe 
----------------------------------------------------------------------------""" 
def load_xlsx_files_from_directory(filename):
    
    df = pd.read_excel(filename, engine="openpyxl")
    return df 



"""-------------------------------------------------------------------------
Conversion des labels en niveaux:

Cette fonction permet de convertir les labels en niveaux
----------------------------------------------------------------------------"""     
    
    
def label_niv (row,niv1,niv2,niv3):
    for x in niv1:
        if row[0] == x :
            return 'niv01'
    for x in niv2:
        if row[0] == x :
            return 'niv02'
    for x in niv3 :
        if row[0] == x :
            return 'niv03'
    if row[0] == None :
        return ''
    
    
    

"""-------------------------------------------------------------------------
Traitement des chaines de caractères :

Cette fonction permet de traiter les problèmes fréquemment rencontrés dans le nom des colonnes
----------------------------------------------------------------------------""" 
def traiter_string(s,logger = None):
  if logger:
    logger.info("--- traiter_string debut ---")
  retour= s.lower()\
          .strip()\
          .replace(" ", "_")\
          .replace("'", "_")\
          .replace("-", "_")\
          .replace("é", "e")\
          .replace("è", "e")\
          .replace("...","")\
          .replace("(","")\
          .replace(")","")\
          .replace("…","")\
          .replace("ç", "c")\
          .replace(".", "")\
          .replace("/", "_")\
          .replace("___", "_")\
          .replace("__", "_")\
          .replace("à","")
  if logger:
    logger.info("--------------------------------------------------------------------------------")
    logger.info("           Dans traiter_string "+s+" ===> "+ retour)
    logger.info("---------------------------------------------------------------------------------")
  return retour

"""-------------------------------------------------------------------------
Suppression de table : 

cette fonction permet de supprimer une table hive et ses fichiers associés
----------------------------------------------------------------------------"""    
def drop_table(spark,depot,db_name,table_name,logger):
  try: 
    logger.info("drop_table ::: db_name == "+db_name+" table_name == "+table_name)
    process = subprocess.Popen(["hadoop", "fs", "-rm","-r","-skipTrash", depot+"/"+db_name+"/"+table_name],stdout=subprocess.PIPE)
    process.wait()
    spark.sql("DROP TABLE IF EXISTS "+db_name+"."+table_name)
  except Exception as err:
    logger.error("Problème lors du traitement de la db_name "+db_name+", la table "+table_name+" dans la fonction : drop_table ::: "+str(err))  

    
"""-------------------------------------------------------------------------
Traitement des valeurs manquantes :

cette fonction permet de remplacer les Nan par des None. 
Les None correspondent à des NULL dans HUE
----------------------------------------------------------------------------"""      
def replace_nan(df,log):
    log.info("replace_nan :::: debut ::: nbline === "+str(df.count()))
    columns = df.columns
    for column in columns:
      df = df.withColumn(column,when(col(column)=='nan',None).otherwise(col(column)))
      df = df.withColumn(column,when(isnan(col(column)),None).otherwise(col(column)))
      df = df.withColumn("DATE_EXTRACTION",lit(datetime.now().strftime("%Y-%d-%m")))
    log.info("replace_nan :::: fin ::: nbline === "+str(df.count()))
    return df
  
"""---------------Cette fonction créée la table info_tables dans hdfs-----------------------------"""
#def load_info_tables_to_hdfs(spark_session,depot,db_name,table_name,df,schema,logger):
def load_table_to_hdfs(spark_session,depot,db_name,table_name,df,logger):
  df.columns = [traiter_string(s).upper() for s in df.columns]
  logger.info("load_data ::: df.columns == "+str(df.columns))
  #---------------------------------------------
  # on convertit toutes les colonnes en string
  #---------------------------------------------
  df = df[df.columns].astype(str).replace(to_replace='\n', value=' ', regex=True)
    #spark_data_frame = spark_session.createDataFrame(df,schema)
  spark_data_frame = spark_session.createDataFrame(df)
  spark_data_frame=replace_nan(spark_data_frame,logger)
  spark_data_frame.show(200,truncate = False)
  spark_data_frame.printSchema()
  drop_table(spark_session,depot,db_name,table_name,logger)
    #---------------------------------------------------------------------------
    # creation de la base de donnees si elle n'existe pas
    #---------------------------------------------------------------------------
  spark_session.sql("CREATE DATABASE IF NOT EXISTS "+db_name+" LOCATION '"+depot+"/"+db_name+"'")
    #---------------------------------------------------------------------------
    # ecriture en mode overwrite plutot qu'append pour ne pas être en mode incremental
    #---------------------------------------------------------------------------
  spark_data_frame.write.mode('overwrite').parquet(depot+"/"+db_name+"/"+table_name)
  spark_session.sql("CREATE TABLE IF NOT EXISTS "+db_name+"."+table_name+" USING PARQUET LOCATION '"+depot+"/"+db_name+"/"+table_name+"'")
  
  
def load_aggregate_table_to_hdfs(spark_session,depot,db_name,table_name,df,logger):
  df.columns = [traiter_string(s).upper() for s in df.columns]
  logger.info("load_data ::: df.columns == "+str(df.columns))
  #---------------------------------------------
  # on convertit toutes les colonnes en string
  #---------------------------------------------
  df = df[df.columns].astype(str).replace(to_replace='\n', value=' ', regex=True)
  spark_data_frame = spark_session.createDataFrame(df)
  spark_data_frame=replace_nan(spark_data_frame,logger)
  #spark_data_frame.show(200,truncate = False)
  spark_data_frame.show()
  spark_data_frame.printSchema()
  #---------------------------------------------------------------------------
  # creation de la base de donnees si elle n'existe pas
  #---------------------------------------------------------------------------
  spark_session.sql("CREATE DATABASE IF NOT EXISTS "+db_name+" LOCATION '"+depot+"/"+db_name+"'")
  #---------------------------------------------------------------------------
  # ecriture en mode overwrite plutot qu'append pour ne pas être en mode incremental
  #---------------------------------------------------------------------------
  spark_data_frame.write.mode('append').parquet(depot+"/"+db_name+"/"+table_name)
  spark_session.sql("CREATE TABLE IF NOT EXISTS "+db_name+"."+table_name+" USING PARQUET LOCATION '"+depot+"/"+db_name+"/"+table_name+"'")

    
"""-------------------------------------------------------------------------
Traitement des données
La fonction permet de dire si la chaine de caractères représente une date ou pas
----------------------------------------------------------------------------"""      

def isdate(v):
    try :
        v = datetime.date(v)
        return True
    except :
        return False
            
"""------------------------------------------------------------------------"""

def add_zero(x):
    if x < 10:
        return '0' + str(x)
    return str(x)

            
"""-------------------------------------------------------------------------
Traitement des données

La fonction permet la suppression des lignes inutiles lors de la construction du DataFrame
----------------------------------------------------------------------------"""      
def delete_noise(df,column):
    index_to_keep = [i for i in df.index if isdate(df.iloc[i][column])]
    return df[df.index.isin(index_to_keep)]
    
"""-------------------------------------------------------------------------
Traitement des données 
La fonction permet de changer les noms des colonnes pour les remplacer par les dates
----------------------------------------------------------------------------"""      
def make_date_header(df): 
    if len([e for e in df.columns if type(e) == datetime]) > 0:
        return df
    else:
        row = 0
        found = len([e for e in df.iloc[row] if type(e) == datetime]) >0
        while not found:
            row += 1 
            found = len([e for e in df.iloc[row] if type(e) == datetime]) >0
        
        new_columns = df.iloc[0]
        df = df[1:]
        df.columns = new_columns
        
        return df 
    
"""-------------------------------------------------------------------------
Traitement des données

Cette fonction permet de retourner la colonne d'arrêt pour construction de dataframe final, ceci en supposant que 
la colonne du dataframe contient les dates d'observation
----------------------------------------------------------------------------"""      
def table_delimiter(df):
    """ 
    Cette fonction permet de retourner la colonne d'arrêt pour construction de dataframe final, ceci en supposant que 
    la colonne du dataframe contient les dates d'observation
    """
    try : 
        counter =  0
        start_count = (type(df.columns.tolist()[counter] )== datetime )
        found = False
        while ((found == False ) | (start_count == False ))  : 


            counter += 1 
            start_count = (type(df.columns.tolist()[counter] )== datetime )
            found = (type(df.columns.tolist()[counter+1] ) != datetime )

        return df[df.columns[:counter+1]]
    
    except : 
        return df
