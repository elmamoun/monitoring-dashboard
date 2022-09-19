from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
from datetime import date, datetime, timedelta
import streamlit as st
import os
import plotly.graph_objects as go
import sys
sys.path.append("/home/cdsw/code/Streamlit")

from utils.dev import * 
from utils.page import Page
# from utils.sidebar import filter_table_option



class Sage(Page):
    def __init__(self, **kwargs):
        name = "Pilotage des données Sage"
        super().__init__(name, **kwargs)


    def content(self):


    #--------------------------------------------------------------------------------------------------------------
    #------------------------------------------------Data and filters----------------------------------------------
    #--------------------------------------------------------------------------------------------------------------

        

        side_barre = st.sidebar
        side_barre.title('Filtres')
        maille_selected = side_barre.selectbox(label = "Maille", options = ("Code flux","Code budget"))
        sage = get_sage_data(maille_selected)
        start_date = sage['date_montant'].min()
        end_date = sage['date_montant'].max()

        day_count = (end_date - start_date).days + 1

        date_from = side_barre.date_input('Depuis la date', value=start_date, max_value=end_date, min_value=start_date)
        date_to = side_barre.date_input('Jusqu\'à la date', value=end_date, max_value=end_date, min_value=start_date)


        dates_selected = [d for d in (date_from + timedelta(n) for n in range(day_count)) if d <= date_to]
        period_selected = side_barre.selectbox(label = "Fréquence",
        options = ("Journalière","Hebdomadaire","Mensuelle","Trimestrielle",
        "Annuelle"))
        key = {"Code flux" :"code_flux_libelle", "Code budget": "code_budgetaire"}


        #--------------------------------------------------------------------------------------------------------------
        #----------------------------------------------Page streamlit--------------------------------------------------
        #--------------------------------------------------------------------------------------------------------------
        st.header("Contrôle de données")
        col0 = st.container()
        col00 = st.container()
        


        st.header("KPI et visualisations")

        evol_key = {"Journalière" : 1 , "Hebdomadaire" : 7 , "Mensuelle" : 30, "Trimestrielle" : 90 , "Annuelle" : 365} 

        beforedate = end_date - timedelta(evol_key[period_selected])
    
        selection, met1, met2, met3 = st.columns(4)
        account_selected = selection.multiselect(label ='Compte en banque',
        options = ("Total", " 107CDNB", " 107CACIBA", " 107CICA"," 108CDNB", " 509CDNA", " 107CACIBB", " 107CDNCSL"
        , " 509CDNCSL", " 107CDNA", " 107BNPA", " 108CDNA", " 107BREDA") , default= "Total")
        


        si = sage[(sage['date_montant'] == end_date) 
        & (sage[key[maille_selected]] == "Solde Initial") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]
        sav = sage[(sage['date_montant'] == end_date)
        & (sage[key[maille_selected]] == "Solde avant Décision") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]
        sf = sage[(sage['date_montant'] == end_date)
        & (sage[key[maille_selected]] == "Solde Final") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]

        sia = sage[(sage['date_montant'] == beforedate)
        & (sage[key[maille_selected]] == "Solde Initial") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]
        sava  = sage[(sage['date_montant'] == beforedate)
        & (sage[key[maille_selected]] == "Solde avant Décision") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]
        sfa = sage[(sage['date_montant'] == beforedate)
        & (sage[key[maille_selected]] == "Solde Final") 
        & (sage['banque_compte'].isin(account_selected))]['montant'].values[0]

        delta1 = 100* (si - sia) / sia
        delta2 = 100 *(sav - sava) / sava
        delta3 = 100* (sf - sfa) / sfa

        delta1c =delta2c =delta3c = """
                                    <style>
                                    div[data-testid="metric-container"] {
                                    background-color: rgba(153, 61, 40, 0.1);
                                    border: 2px solid rgba(153, 61, 40, 0.1);
                                    padding: 5% 5% 5% 10%;r
                                    border-radius: 5px;
                                    color: rgb(30, 103, 119);
                                    overflow-wrap: break-word;
                                    }

                                    /* breakline for metric text         */
                                    div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
                                    overflow-wrap: break-word;
                                    white-space: break-spaces;
                                    color: black;
                                    font-size : 24px;
                                    }
                                    </style>
                                """
        
        delta1 = '{:,}'.format(round(delta1,2)).replace(',', ' ')
        delta2 = '{:,}'.format(round(delta2,2)).replace(',', ' ') 
        delta3 = '{:,}'.format(round(delta3,2)).replace(',', ' ')

        met1.markdown( delta1c
        , unsafe_allow_html=True)
        
        
        met2.markdown(delta2c
        , unsafe_allow_html=True)
            
        met3.markdown(delta3c
        , unsafe_allow_html=True)
        
        
        met1.metric("Solde initial", '{:,}'.format(round(si)).replace(',', ' ') +" €", delta1+" %")
        met2.metric("Solde avant décision", '{:,}'.format(round(sav)).replace(',', ' ')+" €",delta2 +" %")
        met3.metric("Solde final", '{:,}'.format(round(sf)).replace(',', ' ')+ " €" , delta3 +" %")

        col1 = st.container()
        col2, col3 = st.columns(2)



        ## Contrôle des fichiers chargées sur la plateforme



        with col0 : 
 
            bars = list()

            df = get_raw_sage_data(maille_selected)\
                .groupby('date_montant')\
                .agg({"fichier" : 'nunique'})\
                .reset_index()\
                .rename(columns = {"fichier" : "Nombre de fichiers"})\
                .sort_values(by = "date_montant")


            controle_chart = px.bar(df , x = "date_montant" , y = "Nombre de fichiers")
            controle_chart.update_layout(title = "Nombre de fichiers chargés par 'date_montant'")

            st.plotly_chart(controle_chart,use_container_width = True)

        with col00:
            data = sage.groupby(key[maille_selected])\
            .agg({"date_montant":'nunique'})\
            .reset_index()\
            .sort_values(by ="date_montant", ascending = False)

            libelle_chart = px.bar(data, x='date_montant', y= key[maille_selected], orientation = 'h')
            libelle_chart.update_layout(title = "Distribution des libelles")
            st.plotly_chart(libelle_chart, use_container_width = True)

        # balance chart
        with col1 :
            if period_selected == "Journalière" : 
                balance_chart = px.area(get_balance(sage[(sage['date_montant'].isin(dates_selected)) 
                                                    & (sage['banque_compte'].isin(account_selected))], maille = maille_selected),
                x="date_montant", 
                y="montant", 
                color="banque_compte" , 
                title = "Solde final par compte bancaire - J")


            elif period_selected == "Hebdomadaire" : 
                balance_chart = px.area(get_balance(sage[(sage['date_montant'].isin(dates_selected)) 
                                                        & (sage['banque_compte'].isin(account_selected))], maille = maille_selected,frequence = 's'),
                x="date_montant", 
                y="montant", 
                color="banque_compte" , 
                title = "Solde final par compte bancaire - S")
            elif period_selected == "Mensuelle":
                balance_chart = px.area(get_balance(sage[(sage['date_montant'].isin(dates_selected)) 
                                                           & (sage['banque_compte'].isin(account_selected)) ], maille = maille_selected, frequence = 'm'),
                x="annee-mois", 
                y="montant", 
                color="banque_compte" , 
                title = "Solde final par compte bancaire - M")
                balance_chart.update_layout(xaxis={'type': 'category'})

            elif period_selected == "Trimestrielle" : 
                balance_chart = px.area(get_balance(sage[(sage['date_montant'].isin(dates_selected)) 
                                                    & (sage['banque_compte'].isin(account_selected))], maille = maille_selected ,frequence = "t"),
                x="trimestre", 
                y="montant", 
                color="banque_compte" , 
                title = "Solde final par compte bancaire - T")
                balance_chart.update_layout(xaxis={'type': 'category'})

            elif period_selected == "Annuelle" : 
                balance_chart = px.area(get_balance(sage[(sage['date_montant'].isin(dates_selected))
                                                        & (sage['banque_compte'].isin(account_selected)) ],maille = maille_selected ,frequence = 'a'),
                x="annee", 
                y="montant", 
                color="banque_compte" , 
                title = "Solde final par compte bancaire - A")




            st.plotly_chart(balance_chart,use_container_width = True)


        ## récupérer le max de des dépenses et des recettes pour forcer les echelles des barplots

        ## recettes chart
        with col2: 
            if period_selected == "Journalière" : 
                recettes_chart =  px.bar(get_recettes(sage[sage['date_montant'].isin(dates_selected) 
               ] ,m = maille_selected,frequence = 'j')[0], 
                x="date_montant",
                y="montant", 
                color=key[maille_selected],
                title=f"[TOTAL] Flux de recettes par {maille_selected} - J")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'j')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'j')[1])
                max_value = 1.10*max_value
                              ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[2])
                
                recettes_chart.update_layout(yaxis_range=[min_value,max_value])
                


            elif period_selected == "Hebdomadaire" : 
                recettes_chart = px.bar(get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = "s")[0], 
                x="date_montant",
                y="montant", 
                color=key[maille_selected],
                title=f"[TOTAL] Flux de recettes par {maille_selected} - S")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
               ] , m=maille_selected,frequence = 's')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ] , m=maille_selected,frequence = 's')[1])
                max_value = 1.10*max_value
                              ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 's')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 's')[2])
                recettes_chart.update_layout(yaxis_range=[min_value,max_value])

            elif period_selected == "Mensuelle":
                recettes_chart = px.bar(get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'm')[0],
                x="annee-mois", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de recettes par {maille_selected} - M")
                ## max_axis of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'm')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'm')[1])
                max_value = 1.10*max_value
                ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'm')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'm')[2])
                recettes_chart.update_layout(yaxis_range=[min_value,max_value],xaxis={'type': 'category'})

            elif period_selected == "Trimestrielle" : 
                recettes_chart = px.bar(get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = "t")[0],
                x="trimestre", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de recettes par {maille_selected} - T")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 't')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 't')[1])
                max_value = 1.10*max_value
                ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 't')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 't')[2])
                recettes_chart.update_layout(xaxis={'type': 'category'} , yaxis_range=[min_value,max_value])

            elif period_selected == "Annuelle" : 
                recettes_chart = px.bar(get_recettes(sage[sage['date_montant'].isin(dates_selected)
               ],m=maille_selected ,frequence = 'a')[0],
                x="annee", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de recettes par {maille_selected} - A")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[1])
                max_value = 1.10*max_value
                ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[2])
                recettes_chart.update_layout(yaxis_range=[min_value,max_value])

            st.plotly_chart(recettes_chart, use_container_width=True)


        ## dépenses chart
        with col3: 
            if period_selected == "Journalière" : 
                depenses_chart =  px.bar(get_depenses(sage[sage['date_montant'].isin(dates_selected)
               ], m=maille_selected,frequence = 'j')[0], 
                x="date_montant",
                y="montant", 
                color=key[maille_selected],
                title=f"[TOTAL] Flux de dépenses par {maille_selected} - J")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[1])
                max_value = 1.10*max_value
                ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'j')[2])
                

                depenses_chart.update_layout(yaxis_range=[min_value,max_value])

            elif period_selected == "Hebdomadaire" : 
                depenses_chart = px.bar(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = "s")[0], 
                x="date_montant",
                    y="montant", 
                    color=key[maille_selected],
                    title=f"[TOTAL] Flux de dépenses par {maille_selected} - S")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 's')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected ,frequence = 's')[1])
                max_value = 1.10*max_value
                ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 's')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 's')[2])
                
                depenses_chart.update_layout(yaxis_range=[min_value,max_value])

            elif period_selected == "Mensuelle":
                depenses_chart = px.bar(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = 'm')[0],
                x="annee-mois", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de dépenses par {maille_selected} - M")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'm')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ] ,m=maille_selected,frequence = 'm')[1])
                max_value = 1.10*max_value
                 ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'm')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'm')[2])
                
                depenses_chart.update_layout(yaxis_range=[min_value,max_value],xaxis={'type': 'category'})

            elif period_selected == "Trimestrielle" : 
                depenses_chart = px.bar(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = "t")[0],
                x="trimestre", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de dépenses par {maille_selected} - T")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = 't')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = 't')[1])
                max_value = 1.10*max_value
                 ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 't')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 't')[2])
                depenses_chart.update_layout(xaxis={'type': 'category'} , yaxis_range=[min_value,max_value])

            elif period_selected == "Annuelle" : 
                depenses_chart = px.bar(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[0],
                x="annee", 
                y="montant", 
                color=key[maille_selected] , 
                title = f"[TOTAL] Flux de dépenses par {maille_selected} - A")
                ## max_value of y_axis
                max_value  = max(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = 'a')[1]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ],m=maille_selected ,frequence = 'a')[1])
                max_value = 1.10*max_value
                 ## min_value of y_axis 
                min_value  = min(get_depenses(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[2]
                                ,get_recettes(sage[sage['date_montant'].isin(dates_selected)
                ], m=maille_selected,frequence = 'a')[2])
                depenses_chart.update_layout(yaxis_range=[min_value,max_value])
                

            st.plotly_chart(depenses_chart, use_container_width=True)




    

    

