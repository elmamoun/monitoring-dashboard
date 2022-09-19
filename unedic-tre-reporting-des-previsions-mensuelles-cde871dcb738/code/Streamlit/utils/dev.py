import streamlit as st 
from pyspark.sql import SparkSession
import pandas as pd

application_name = "treso_dashboard"
spark_session = SparkSession\
        .builder\
        .appName(application_name)\
        .getOrCreate()


@st.cache 
def get_raw_sage_data(maille = "Code flux"):
    if maille == 'Code flux'    :
        sage = spark_session.sql("SELECT  * FROM raw_reporting_treso.export_sage_code_flux_historique").toPandas()
    elif maille == 'Code budget' : 
         sage = spark_session.sql("SELECT  * FROM raw_reporting_treso.export_sage_code_budgetaire_v2").toPandas()
    sage.columns = [e.lower() for e in sage.columns]
    sage["date_montant"] = pd.to_datetime(sage['date_montant'], format='%Y/%m/%d')
    sage['date_production_fichier'] = pd.to_datetime(sage['date_production_fichier'], format='%Y/%m/%d %H:%M:%S')
    sage['montant'] = sage['montant'].astype(float)
    return sage
    




def get_keys_from_value(d, val):
    for k in list(d.keys()):
        if val in d[k]:
            return k 
            
@st.cache
def get_prev_data(frequence = "mensuelle"):
    prev = spark_session.sql("SELECT  * FROM raw_reporting_treso.treso_dea_prev ").toPandas()
    prev.columns = [e.lower() for e in prev.columns]
    # prev["version_prev"] = pd.to_datetime(prev['version_prev'], format='%Y/%m')
    prev["montant_millions"] = pd.to_numeric(prev['montant_millions'], errors='coerce')

    ## standariser les libelles 

    prev.loc[prev['nature_montant'] == 'ARE / AREF' , "nature_montant"] = "ARE / AREF, y compris UE"
    prev.loc[prev['nature_montant'] == 'Allocation partielle de longue durée (APLD)', 'nature_montant'] = "Activité partielle, allocation partielle de longue durée (APLD)"
    prev.loc[prev['nature_montant'] == 'Autres (AS-FNE, Plan rebond, ...)', 'nature_montant'] = 'Autres'
    prev.loc[prev['nature_montant'] == 'CRP / CSP, y compris équivalents ARE', 'nature_montant'] = 'CRP / CTP / CSP, y compris équivalents ARE'
    prev.loc[prev['nature_montant'] == "Contributions et autres produits",'nature_montant'] = "Contributions d'Assurance chômage"
    prev.loc[prev['nature_montant'] == "Contributions et autres recettes d'Assurance chômage", 'nature_montant'] = "Contributions d'Assurance chômage"
    prev.loc[prev['nature_montant'] == "Gestion administrative", 'nature_montant']= "Autres produits"
    prev.loc[prev['nature_montant'] == "Autres aides  (prime CSP, aide fin de droit, ...)", 'nature_montant'] = 'Autres aides (prime CSP, aide fin de droit, adr, ...)'
    prev.loc[prev['nature_montant'] == "Autres aides (prime CSP, aide fin de droit, ...)", 'nature_montant'] = 'Autres aides (prime CSP, aide fin de droit, adr, ...)'
    prev.loc[prev['nature_montant'] == 'Autres aides (adr, aide fin de droit, ...)', 'nature_montant'] = "Autres aides (prime CSP, aide fin de droit, adr, ...)"
    prev.loc[prev['nature_montant'] == "Autres produits (gestion administrative et financière)",'nature_montant'] = "Autres produits"
    prev.loc[prev['nature_montant'] == "Remboursements d'indus", 'nature_montant'] = 'Indus, avances et acomptes'
    prev.loc[prev['nature_montant'] == "Frais de gestion et décaissements sur immobilisations" ,'nature_montant'] = "Frais de fonctionnement et de gestion"
    prev.loc[prev['nature_montant'].isin(['10% Pôle Emploi','11% Pôle Emploi']),'nature_montant'] = 'Contributions pôle emploi'
    prev.loc[prev['nature_montant'] == 'Contributions', 'nature_montant'] = "Contributions principales"
    prev.loc[prev['nature_montant'] == "Etat - Prélèvement à la source" , 'nature_montant'] = "Prélèvement à la source"
    prev.loc[prev['nature_montant'] == "Caisses de retraite complémentaire", 'nature_montant'] = "Caisses de retraites complémentaire"
    prev.loc[prev['nature_montant'] == 'Caisses de retraite complémentaire (dont précomptes)' , 'nature_montant'] = "Caisses de retraites complémentaire (dont précomptes)"
    prev.loc[prev['nature_montant'].isin(["SOLDE = RECETTES - DÉPENSES","Solde",'Solde financier']),'nature_montant'] = 'Solde' 
    prev.loc[(prev['nature_montant'] =="Sécurité sociale (CSG, CRDS, maladie)") 
            & (prev['montant_millions'] < 0), "nature_montant"] = 'Dépenses - Sécurité sociale (CSG, CRDS, maladie)'
    prev.loc[(prev['nature_montant'] =="Sécurité sociale (CSG, CRDS, maladie)") 
            & (prev['montant_millions'] >= 0), "nature_montant"] = 'Recettes - Sécurité sociale (CSG, CRDS, maladie)'
    prev.loc[(prev['nature_montant'] =='Etat - Prélèvement à la source') &
            (prev['montant_millions'] <0 ),'nature_montant' ] =  'Dépenses - Etat - Prélèvement à la source'
    prev.loc[(prev['nature_montant'] =='Etat - Prélèvement à la source') &
            (prev['montant_millions'] >= 0 ) ,'nature_montant']=  'Recettes - Etat - Prélèvement à la source'
    prev.loc[(prev['nature_montant'] =='Prélèvement à la source') &
            (prev['montant_millions'] <0 ) ,'nature_montant'] =  'Dépenses - Etat - Prélèvement à la source'
    prev.loc[(prev['nature_montant'] =='Prélèvement à la source') &
            (prev['montant_millions'] >= 0 ),'nature_montant'] =  'Recettes - Etat - Prélèvement à la source'
    prev.loc[(prev['nature_montant'] == "Prsélèvements sociaux"), 'nature_montant'] = "Prélèvements sociaux et fiscaux"
    prev.loc[prev['nature_montant'] =="Intérêts nets sur emprunts" , "nature_montant"] = "Intérêts sur emprunts"

    dff= prev[prev['type_montant'].isin(["Observé", "Réalisé", "Réalisé définitif" ,"Réalisé (Quasi Déf)" 
                ,"Réalisé (Provisoire)" ," Réalisé Semi Déf"])]\
                    .groupby(['annee' ,'mois','mois_nom','type_montant','nature_montant'])\
                    .agg({"version_prev" : "max"} )\
                    .reset_index()

    prevp = prev[prev['type_montant'] == "Prévu"]
    prevr = prev.merge(dff, on = ["annee","mois","mois_nom", 'nature_montant',"version_prev",'type_montant' ])
    prev = pd.concat([prevp,prevr])

    prev = prev[~prev['mois_nom'].isin(['TOTAL','TOTAL.1'])] 
    prev['annee-mois'] = prev.apply(lambda x : str(x['annee'] +'-'+str(x['mois'])), axis = 1 )
    prev["annee-mois"] = pd.to_datetime(prev['annee-mois'], format='%Y/%m')
    prev['trimestre'] = prev['annee-mois'].dt.quarter
    prev['trimestre'] = prev.apply(lambda x : str(x['annee']) + '-T'+str(x['trimestre']), axis = 1)
    prev["annee"] = prev['annee'].astype(int)
    
    prev = prev[~prev['mois_nom'].isin(['TOTAL','TOTAL.1'])] 
    prev['annee-mois'] = prev['annee-mois'].dt.strftime('%Y-%m')
    if frequence == "mensuelle":
        return prev  
    else :
        return prev.groupby(['trimestre','nature_montant','type_montant','annee',"version_prev"]).agg({"montant_millions":'sum'}).reset_index()


@st.cache
def get_balance(sage_data,maille,frequence = 'J') : 
    
    df = sage_data
    if maille == "Code flux":
        df= df[(df['code_flux_libelle'] ==  "Solde Final") ]
    elif maille == "Code budget" :
        df= df[(df['code_budgetaire'] ==  "Solde Final") ]


    ## data to plot according to frequency
    if frequence.upper() == "J" :
        final_df = df
    elif frequence.upper() == "S" : 
        final_df = df.groupby(["annee-semaine","banque_compte"])\
            .agg({"montant" : "sum"}).reset_index()\
            .merge(df.groupby("annee-semaine")\
            .agg({"date_montant" : "min"}).reset_index())
    elif frequence.upper() == "M":
        final_df = df.groupby(['annee-mois',"banque_compte"])\
            .agg({"montant" : "sum"}).reset_index()
    elif frequence.upper() == "T" : 
        final_df = df.groupby(['trimestre',"banque_compte"])\
            .agg({"montant" : "sum"}).reset_index()
    elif frequence.upper() == "A":
        final_df = df.groupby(['annee',"banque_compte"])\
            .agg({"montant" : "sum"}).reset_index()
    return final_df


@st.cache()
def get_sage_data(maille = "Code flux"):
    if maille == "Code flux" : 
        sage = spark_session.sql("SELECT  * FROM raw_reporting_treso.export_sage_code_flux_historique").toPandas()
    elif maille == "Code budget" :
        sage = spark_session.sql("SELECT  * FROM raw_reporting_treso.export_sage_code_budgetaire_v2").toPandas()
    sage.columns = [e.lower() for e in sage.columns]
    sage["date_montant"] = pd.to_datetime(sage['date_montant'], format='%Y/%m/%d')
    sage['date_production_fichier'] = pd.to_datetime(sage['date_production_fichier'], format='%Y/%m/%d %H:%M:%S')
    sage['montant'] = sage['montant'].astype(float)
    dff= sage.groupby(['date_montant' ,'banque_compte']).agg({"date_production_fichier" : "max"} ).reset_index()
    sage = sage.merge(dff, on = ["date_montant","banque_compte", "date_production_fichier" ])

    sage['annee'] = pd.DatetimeIndex(sage['date_montant']).year
    sage['annee-mois'] = sage['date_montant'].dt.strftime('%Y-%m')
    sage['annee-semaine'] = sage['date_montant'].dt.strftime('%Y-%U')
    sage['trimestre'] = sage['date_montant'].dt.quarter
    sage['trimestre'] = sage.apply(lambda x : str(x['annee']) + '-T'+str(x['trimestre']), axis = 1)
    
    return sage

@st.cache()
def get_libelle(type ="recettes",maille = 'Code flux'):
    sage = get_sage_data()
    sageb = spark_session.sql("SELECT * FROM raw_reporting_treso.codeflux_codebudget_libelles").toPandas()
    all_values = sage[sage['type_montant'] == "R"]["code_flux_libelle"].unique().tolist()
    a = sageb["CODE_BUDGETAIRE"].unique()
    
    if maille == "Code flux" : 
        if type =="recettes" :
            return [e for e in all_values if "ecette" in e]
        else :
            return [e for e in all_values if "pense" in e]
        
    elif maille == "Code budget":
        if type =="recettes" :
            return [e for e in a if e[0].upper() == "R" ]
        else :
            return [e for e in a if e[0].upper() == "D" ]


    
@st.cache 
def get_recettes(sage_data,m,frequence = "J"):

    
    libelle_recettes = get_libelle(type ="recettes",maille =  m )
    df = sage_data
    if m =="Code flux":
        df= df[(df["code_flux_libelle"].isin(libelle_recettes) ) & (df['banque_compte'] =="Total") ]
    elif m == "Code budget": 
        df= df[(df["code_budgetaire"].isin(libelle_recettes)) & ( df['banque_compte'] =="Total")]

    maille_selected = "code_flux_libelle" if m =="Code flux" else "code_budgetaire"

    ## data to plot according to frequency
    if frequence.upper() == "J" : 
        final_df = df 
        edf = final_df.groupby('date_montant').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "S":
        final_df = df.groupby(["annee-semaine",maille_selected])\
            .agg({"montant" : "sum"}).reset_index()\
            .merge(df.groupby("annee-semaine")\
            .agg({"date_montant" : "min"}).reset_index())
        edf = final_df.groupby('annee-semaine').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "M" : 
        final_df = df.groupby(['annee-mois',maille_selected])\
            .agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('annee-mois').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "T" : 
        final_df = df.groupby(['trimestre',maille_selected]).\
            agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('trimestre').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "A":
        final_df = df.groupby(['annee',maille_selected])\
            .agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('annee').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    return final_df, max_value,min_value

@st.cache
def get_depenses(sage_data, m = "Code flux",frequence = 'J'): 


    libelle_depenses = get_libelle(type= "depenses", maille= m )
    df = sage_data
    if m =="Code flux":
        df= df[df["code_flux_libelle"].isin(libelle_depenses) & (df['banque_compte'] =="Total")]
    elif m =="Code budget" :
        df= df[df["code_budgetaire"].isin(libelle_depenses) & (df['banque_compte'] =="Total")]
        
    maille_selected = "code_flux_libelle" if m =="Code flux" else "code_budgetaire"


    ## data to plot according to frequency

    if frequence.upper() == 'J' :
        final_df = df
        edf = final_df.groupby('date_montant').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "S":
        final_df = df.groupby(["annee-semaine",maille_selected])\
            .agg({"montant" : "sum"}).reset_index()\
            .merge(df.groupby("annee-semaine")\
            .agg({"date_montant" : "min"}).reset_index())
        edf = final_df.groupby('annee-semaine').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "M" : 
        final_df = df.groupby(['annee-mois',maille_selected])\
            .agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('annee-mois').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "T" : 
        final_df = df.groupby(['trimestre',maille_selected]).\
            agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('trimestre').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    elif frequence.upper() == "A":
        final_df = df.groupby(['annee',maille_selected])\
            .agg({"montant" : "sum"}).reset_index()
        edf = final_df.groupby('annee').agg({"montant" : "sum"})['montant']
        max_value, min_value = edf.max(), edf.min()
    return final_df, max_value, min_value


