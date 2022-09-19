from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
from datetime import date, datetime, timedelta
import streamlit as st
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import sys
sys.path.append("/home/cdsw/code/Streamlit/")
from utils.dev import * 
from utils.page import Page
# from utils.sidebar import filter_table_option


class Prev(Page):
    def __init__(self, **kwargs):
        name = "Pilotage des données de prévision"
        super().__init__(name, **kwargs)
        

    def content(self) :


        side_barre = st.sidebar
        frequence = side_barre.selectbox("Fréquence", options =( "Mensuelle", "Trimestrielle"))
        cle = {'Mensuelle' : "annee-mois" , "Trimestrielle" : 'trimestre'}
        prev = get_prev_data(frequence.lower())
        prevision = prev[prev['type_montant']=='Prévu' ]
        realise = prev[prev['type_montant'].isin(["Observé", "Réalisé", "Réalisé définitif" ,"Réalisé (Quasi Déf)" 
                                                ,"Réalisé (Provisoire)" ," Réalisé Semi Déf"])]

        st.header("Flux recettes, dépenses et autres - Réalisé et prévision")
        col2, col1 = st.columns([4,1]) 
        



  
        selected_year = col1.selectbox(label = "Année d'observation", key = '01' , 
        options = (2023,2022,2021,2020,2019,2018,2017,2016,2015
        ,2014,2013,2012))

        options_versions = sorted(prev[(prev['annee'] == selected_year) & (prev['type_montant'] == "Prévu")]['version_prev'].unique(), reverse = True)
        options_versions = [str(e) for e in options_versions]

        selected_version = col1.selectbox(label = "Version de prévision" , key = '02',
        options = options_versions)
        
        with col2 :

            ################## DONNEES RECETTES #################
            recettes_libelles = ["Prélèvements sociaux et fiscaux" , "Autres", "Autres (accompagnement CSP, recouvrements",
                                "Allocations brutes", "Aides", "Activité partielle, allocation partielle de longue durée (APLD)"] 

            df1 = realise[(realise['nature_montant'].isin(recettes_libelles)) & (realise['annee'] == selected_year)]\
                                .groupby(cle[frequence])\
                                .agg({"montant_millions" : "sum"})\
                                .reset_index()\
                                .sort_values(by = cle[frequence])

            df1['cumul annuel - réalisé'] = df1['montant_millions'].cumsum()

            df2 = prevision[(prevision['nature_montant'].isin(recettes_libelles)) & (prevision['annee'] == selected_year) & (prevision["version_prev"] == selected_version)]\
                            .groupby(cle[frequence])\
                            .agg({"montant_millions" : "sum"})\
                            .reset_index()\
                            .sort_values(by = cle[frequence])

            df2['cumul annuel - prévu'] = df2['montant_millions'].cumsum()

            df_r = df1.merge(df2, on = cle[frequence] , how ='outer')\
            .rename(columns = {'montant_millions_x' : 'montant_realise' , 'montant_millions_y' : 'montant_prevu'})
            df_r['type'] = 'recettes'

            ################## DONNEES DEPENSES #################
            df1 = realise[(realise['nature_montant'] == "Total Dépenses") & (realise['annee'] == selected_year)]\
                                    .groupby(cle[frequence])\
                                    .agg({"montant_millions" : "sum"})\
                                    .reset_index()\
                                    .sort_values(by = cle[frequence])


            df1['cumul annuel - réalisé'] = -df1['montant_millions'].cumsum()

            df2 = prevision[(prevision['nature_montant'] == "Total Dépenses") & (prevision['annee'] == selected_year) & (prevision["version_prev"] == selected_version)]\
                            .groupby(cle[frequence])\
                            .agg({"montant_millions" : "sum"})\
                            .reset_index()\
                            .sort_values(by = cle[frequence])

            df2['cumul annuel - prévu'] = -df2['montant_millions'].cumsum()

            df_d = df1.merge(df2, on = cle[frequence], how ='outer')\
            .rename(columns = {'montant_millions_x' : 'montant_realise' , 'montant_millions_y' : 'montant_prevu'})
            df_d['type'] = 'depenses'
            df_d['montant_realise'] = - df_d["montant_realise"]
            df_d['montant_prevu'] = -df_d['montant_prevu']

            ################## DONNEES AUTRES #################

            autres_libelles =  ["Caisses de retraites complémentaire (dont précomptes)" , "Contributions pôle emploi", 
                                "Recettes - Etat - Prélèvement à la source", "Recettes - Sécurité sociale (CSG, CRDS, maladie)", 
                                "Intérêts sur emprunts", "Frais de fonctionnement et de gestion"] 

            df1 = realise[(realise['nature_montant'].isin(autres_libelles))& (realise['annee'] == selected_year)]\
                            .groupby(cle[frequence])\
                            .agg({"montant_millions" : "sum"})\
                            .reset_index()\
                            .sort_values(by = cle[frequence])

            df1['cumul annuel - réalisé'] = -df1['montant_millions'].cumsum()

            df2 = prevision[(prevision['nature_montant'].isin(autres_libelles)) & (prevision['annee'] == selected_year)& (prevision["version_prev"] == selected_version)]\
                            .groupby(cle[frequence])\
                            .agg({"montant_millions" : "sum"})\
                            .reset_index()\
                            .sort_values(by = cle[frequence] )

            df2['cumul annuel - prévu'] = -df2['montant_millions'].cumsum()

            df_a = df1.merge(df2, on =cle[frequence], how ='outer')\
            .rename(columns = {'montant_millions_x' : 'montant_realise' , 'montant_millions_y' : 'montant_prevu'})

            df_a['type'] = 'autres'
            df_a['montant_realise'] = -df_a['montant_realise']
            df_a['montant_prevu'] = -df_a['montant_prevu']


   

            ################## DONNEES FINALES #################

            dff = pd.concat([df_r,df_d,df_a]).sort_values(by = cle[frequence])


            flux_chart = go.Figure(
                data=[
                    ## recettes 
                    go.Bar( x=dff[dff["type"] == "recettes"][cle[frequence]],
                            y=dff[dff["type"] == "recettes"].fillna(0)["montant_realise"],
                            name = "Flux recettes - Réalisé", 
                            marker_color = "green",
                            offsetgroup=0),
                    ## dépenses
                    go.Bar( x=dff[dff["type"] == "depenses"][cle[frequence]],
                            y=dff[dff["type"] == "depenses"].fillna(0)["montant_realise"],
                            name = "Flux dépenses - Réalisé",
                            marker_color = "red",
                        offsetgroup=0), 
                    ## autres
                    go.Bar( x=dff[dff["type"] == "autres"][cle[frequence]],
                            y=dff[dff["type"] == "autres"].fillna(0)["montant_realise"],
                            name = "Autre flux - Réalisé",
                            marker_color = "yellow",
                            offsetgroup=0)

                    ]
                                )


            flux_chart.add_traces(
                data=[
                    go.Bar( x=dff[dff["type"] == "recettes"][cle[frequence]],
                            y=dff[dff["type"] == "recettes"]["montant_prevu"],
                            name = "Flux recettes - Prévu" ,
                            marker_color = "lightgreen",
                            offsetgroup=1),

                    go.Bar( x=dff[dff["type"] == "depenses"][cle[frequence]],
                            y=dff[dff["type"] == "depenses"]["montant_prevu"],
                            name = "Flux dépenses - Prévu" ,
                            marker_color = "rgb(237, 102, 102)",
                            offsetgroup=1),

                    go.Bar( x=dff[dff["type"] == "autres"][cle[frequence]],
                    y=dff[dff["type"] == "autres"]["montant_prevu"],
                    name = "Autre flux - Prévu" ,
                    marker_color = "rgb(235, 180, 52)",
                    offsetgroup=1)

                    ]
                                )


            flux_chart.update_layout(title = "Flux de recettes/dépenses techniques"  )

            flux_chart.update_xaxes({"type":"category"} )
            st.plotly_chart(flux_chart, use_container_width = True)

        col3, col4 = st.columns([4,1])
        with col3: 
            chart_cumul = go.Figure(
                data = [
                    ## recettes réalisées
                    go.Line(x = dff[dff['type'] == 'recettes'][cle[frequence]],
                    y = dff[dff['type'] == "recettes"]['cumul annuel - réalisé'],
                    name = "Cumul recettes - Réalisé",
                    marker_color = "green"),
                    ## recettes prévues
                    go.Line(x = dff[dff['type'] == "recettes"][cle[frequence]],
                    y = dff[dff['type'] == "recettes"]['cumul annuel - prévu'],
                    name = "Cumul recettes - Prévu", 
                    marker_color = "lightgreen"),
                    ## dépenses réalisées
                    go.Line(x = dff[dff['type'] == 'depenses'][cle[frequence]],
                    y = dff[dff['type'] == "depenses"]['cumul annuel - réalisé'],
                    name = "Cumul dépenses - Réalisé",
                    marker_color = "red"),
                    ## dépenses prévues
                    go.Line(x = dff[dff['type'] == 'depenses'][cle[frequence]],
                    y = dff[dff['type']== "depenses"]['cumul annuel - prévu'],
                    name = "Cumul dépenses - Prévu",
                    marker_color = "rgb(237, 102, 102)"),
                    ## autres réalisés
                    go.Line(x = dff[dff['type'] == 'autres'][cle[frequence]],
                    y = dff[dff['type']== "autres"]['cumul annuel - réalisé'],
                    name = "Cumul autres flux - Réalisé",
                    marker_color = "yellow"),
                    ## autres prévues
                    go.Line(x = dff[dff['type'] == 'autres'][cle[frequence]],
                    y = dff[dff['type']== "autres"]['cumul annuel - prévu'],
                    name = "Cumul autre flux - Prévu",
                    marker_color = "rgb(235, 180, 52)")
                ]
            )
            chart_cumul.update_layout(xaxis={'type': 'category'}, title = "Cumul de flux - Réalisé et prévision"  )

            st.plotly_chart(chart_cumul, use_container_width = True)
        
        



        #########################################   CHOIX DE NATURE MONTANT      #########################################

        st.header("Choix de nature montant")

        col_chart, filtre = st.columns([4,1])

        ## dictionnaire de mapping nature_montant et type de montant
        split_nature = dict()
        split_nature['recettes']= ["Contributions d'Assurance chômage","Contributions principales","Impositions de toutes natures",
                                "Conventions diverses, y compris UE","Participation entreprise CSP / CRP","Autres produits",
                                "Revenus financiers"]
        split_nature['autres'] =  ["Caisses de retraites complémentaire (dont précomptes)" , "Contributions pôle emploi", 
                                        "Recettes - Etat - Prélèvement à la source", "Recettes - Sécurité sociale (CSG, CRDS, maladie)", 
                                        "Intérêts sur emprunts", "Frais de fonctionnement et de gestion"] 
        split_nature['depenses']= ['Reversements et compléments','ATI','ARE / AREF, y compris UE',
        'Autres aides (prime CSP, aide fin de droit, adr, ...)','Autres','Dépenses allocataires',
        'Autres (accompagnement CSP, recouvrements)','Prélèvements sociaux et fiscaux','CRP / CTP / CSP, y compris équivalents ARE',
        'Dette','Aides', 'Allocations brutes','Activité partielle, allocation partielle de longue durée (APLD)',
        'Autres produits, conventions diverses','Dépenses - Sécurité sociale (CSG, CRDS, maladie)','Dépenses - Etat - Prélèvement à la source',
        'Financement et frais de gestion opérateurs','AREP', 'Caisses de retraites complémentaire','Dépenses autres publics',
        'Indus, avances et acomptes',"Aides à la reprise et création d'entreprise",'Autres allocations (décès, ...)']
        av = [x for xs in list(split_nature.values()) for x in xs]


        selected_year = filtre.selectbox(label = "Année d'observation", key = '03', 
        options = (2023,2022,2021,2020,2019,2018,2017,2016,2015
        ,2014,2013,2012))
        selected_value = filtre.selectbox(label = "Nature montant choisi", key = '04',
        options = [e for e in av])

        options_versions = sorted(prev[(prev['annee'] == selected_year) & (prev['type_montant'] == "Prévu") 
                            & (prev['nature_montant']==selected_value)]['version_prev'].unique(), reverse = True)
        options_versions = [str(e) for e in options_versions]

        selected_version = filtre.selectbox(label = "Version de prévision" ,key = "05", 
        options = options_versions)
        kk = get_keys_from_value(split_nature,selected_value)

        if kk == "recettes" : 
            c1,c2,c3,c4 = 'green', 'lightgreen','rgb(4, 56, 13)' ,'rgb(79, 219, 158)'
        elif kk == "depenses" : 
            c1,c2,c3,c4 = "red", "rgb(237, 102, 102)",'rgb(163, 11, 16)','rgb(222, 116, 71)'
        elif kk == "autres":
            c1,c2 = "yellow" , "rgb(235, 180, 52)"

        kk_key = {"recettes" : "Flux de recettes techniques choisi" , "depenses" : "Flux de dépenses techniques choisi" , "autres" : "Flux de type 'autres' choisi"}


        with col_chart :

            df1 = realise[ (realise['nature_montant'] == selected_value) & (realise['annee'] == selected_year)]\
                .groupby(cle[frequence])\
                .agg({"montant_millions" : "sum"})\
                .reset_index()\
                .sort_values(by = cle[frequence])

            df1['cumul annuel - réalisé'] = df1['montant_millions'].cumsum()
                       
            df2 = prevision[ (prevision['nature_montant'] == selected_value) & (prevision['annee'] == selected_year) & (prevision['version_prev'] ==selected_version)]\
                .groupby(cle[frequence])\
                .agg({"montant_millions" : "sum"})\
                .reset_index()\
                .sort_values(by = cle[frequence])
            df2['cumul annuel - prévu'] = df2['montant_millions'].cumsum()

            df = df1.merge(df2, on =cle[frequence], how ='outer')\
            .rename(columns = {'montant_millions_x' : 'montant_realise' , 'montant_millions_y' : 'montant_prevu'})
            
 


            chart_nature_montant = make_subplots(specs=[[{"secondary_y": True}]])

            chart_nature_montant.add_trace(go.Bar(x=df[cle[frequence]],
                                y=df["montant_realise"],
                                name="Flux mensuels - Réalisé",
                                marker_color = c1),secondary_y=False )
            chart_nature_montant.add_trace(go.Bar(x=df[cle[frequence]],
                                y=df["montant_prevu"],
                                name="Flux mensuels - Prévu",
                                marker_color = c2),secondary_y=False )

            chart_nature_montant.add_trace(go.Line(x = df[cle[frequence]],
                                y = df['cumul annuel - réalisé'],
                                name  = 'cumul annuel - réalisé',
                                line=dict(color=c1),
                                marker=dict(size=8, color='rgb(66, 87, 245)')
                                        ),secondary_y=True )

            chart_nature_montant.add_trace(go.Line(x = df[cle[frequence]],
                                y = df['cumul annuel - prévu'],
                                name  = 'cumul annuel - prévu', 
                                line = dict(color = c2),
                                marker = dict(size=8, color='rgb(66, 87, 245)')
                                        ),secondary_y=True )


            chart_nature_montant.update_traces(marker=dict(size=12,
                                                line=dict(width=2,
                                                         color='DarkSlateGrey')),
                                                selector=dict(mode='markers'))
            chart_nature_montant.update_layout(xaxis={'type': 'category'}, title = kk_key[kk]  )
            
            st.plotly_chart(chart_nature_montant, use_container_width = True)