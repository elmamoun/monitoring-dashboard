import sys
sys.path.append('/home/cdsw/code')
from utils import * 
import math 
import dateutil
from dateutil.parser import parse
import openpyxl
import numpy as np
import os
from monLogger import *


def prevision_data_ingestion_single_sheet(spark, depot, db_name, table_name, filename, extension, normal_values_nature_montant, log):
    
    """
    The function is useful to create a pandas dataframe from a singlesheet's excel files
    params : 
    - filename : the name of the file to be treated
    - extension : the extension of the file to be treated   
    """
    
    startTime = datetime.now()
    if extension == "*.xlsx" :
        df = pd.read_excel(filename, engine = 'openpyxl')
    elif extension == '*.csv':
        df = pd.read_csv(filename, engine = 'openpyxl')

    log.info(f"Début d'écriture du fichier {filename.split('plateforme/')[1]}")

    #--------------------------------------------------------------------------------------------------------------#
    #-----supprimer les tableaux secondaires que nous ne souhaitons pas récupérer à partir du fichier excel/csv----#
    #--------------------------------------------------------------------------------------------------------------#

    stop_l, stop_c, column_bloc = check_index(df)
    if stop_c[0] != column_bloc: 
        df.loc[df[df.columns[stop_c[0]]].isna(), df.columns[stop_c[0]]] = \
        df[df[df.columns[stop_c[0]]].isna()][df.columns[column_bloc]]
    df =df[df.columns[stop_c[0]:stop_c[1]]]
    df =df[:stop_l]
    df = df.dropna(axis = 0, how = 'all')   ## supprimer les lignes de séparation 

    #--------------------------------------------------------------------------------------------------------------#
    #-------------------transposer les données et renommer la colonne annee----------------------------------------#
    #--------------------------------------------------------------------------------------------------------------#

    df = df.T
    df = df.reset_index(level=0)
    df = df.rename(columns = {"index" : "annee"})

    #--------------------------------------------------------------------------------------------------------------#
    #------------------------------------remplir la colonne année--------------------------------------------------# 
    #--------------------------------------------------------------------------------------------------------------#

    df = df.rename(columns = {'index' : 'annee'})
    year_values= df[df[0] == 'janvier']['annee'].tolist()
    j= 0
    for i in range(2,len(df)):
        if df.iloc[i][0] != 'janvier':
            df.iloc[i]['annee'] = year_values[j]
        else :
            j+= 1
            df.iloc[i]['annee'] = year_values[j]
            
    #--------------------------------------------------------------------------------------------------------------#
    #---------------récupérer la première partie de la table déjà traitée------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------#

    df1= df[df.columns[:3]]

    #changer l'entete de la table
    df1= df1[1:].reset_index().drop(columns = 'index')
    df1 = df1.rename(columns = {0:'mois_nom', 1:'type_montant'})

    # ajouter une colonne 'key' pour une jointure plus tard
    df1['key'] = [i for i in range(len(df1))]

    #--------------------------------------------------------------------------------------------------------------#
    #--------------------------transformer la deuxième partie de la table------------------------------------------# 
    #--------------------------------------------------------------------------------------------------------------#
    df2= df[df.columns[3:]]
    new_header = df2.iloc[0] 
    df2 = df2[1:] 
    df2.columns = new_header 


    df3= df2.T.unstack().reset_index(level=1, name='montant_millions')
    df3= df3.rename(columns ={0:'nature_montant'})
    df3.loc[df3['nature_montant'].isna(), 'nature_montant'] = 'Eléments exceptionnels'

    l = [len(df2.columns)*[i] for i in range(len(df1))]
    flat_l = [item for sublist in l for item in sublist]
    #--------------------------------------------------------------------------------------------------------------#
    #-----ajouter la colonne 'key' pour réaliser la jointure avec les données de la première table-----------------#
    #--------------------------------------------------------------------------------------------------------------#

    df3['key'] = flat_l
    
    dff = df3.merge(df1, on= 'key')
    dff.drop(columns ='key', inplace = True)
    dff['montant_millions'] = dff['montant_millions'].astype(float, errors = 'ignore')

    mapping_month = {'janvier': '01' , 'février':'02' , 'mars': '03', 'avril': '04', 'mai':'05', 'juin': '06' , 'juillet':'07' , 'août' : '08' ,
                 'septembre' : '09' , 'octobre' : '10' , 'novembre' : '11' , 'décembre' : '12' }

    dff['mois'] = dff['mois_nom'].map(mapping_month)
    
    #-----------------------------------------------------------------------------------------------------------#
    #---------ajouter la colonne version_prev qui indique la date de génération du fichier de prévision---------#
    #-----------------------------------------------------------------------------------------------------------#

    dff['version_prev'] = get_date_extraction_m_yy(filename)
    dff['version_prev'] = pd.to_datetime(dff['version_prev'])
    dff['version_prev']= dff['version_prev'].dt.strftime('%Y-%m')


    # trim de la colonne nature_trim 
    dff['nature_montant'] = dff['nature_montant'].str.strip()    
    
    dff = dff.replace('null', np.nan)
    dff.dropna(subset = ['montant_millions' ] , inplace = True)
    
    end_time = str(datetime.now()-startTime)
    print ("durée de traitement = "+ end_time)
    log.info("durée de traitement = "+ end_time )  
    
    dff = dff[['version_prev','annee' , 'mois' , 'mois_nom', 'nature_montant',  'type_montant', 'montant_millions' ]]

    #-----------------------------------------------------------------------------------------------------------#
    #---------------------ajouter le dataframe s'il vérifie les critères de Data Quality------------------------#
    #-----------------------------------------------------------------------------------------------------------#

    if data_quality(dff, normal_values_nature_montant)[1] :   
        # télécharger la table sur hdfs 
        load_aggregate_table_to_hdfs(spark,depot,db_name,table_name,dff,log)
        log.info(f" le fichier {filename.split('plateforme/')[1]} respecte les critères de Data Quality et sera écrit")

    
    else:
        log.warning('Le dataframe créé ne respecte pas les critères de Data Quality')
        return 'Le dataframe créé ne respecte pas les critères de Data Quality'
    
    
    return dff






    