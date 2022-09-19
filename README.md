# 1- Automatisation des reportings de la tresorerie

L'objectif du projet est de normaliser les données de prévisions mensuelles de la trésorerie initialement réprésentées dans des fichiers excel afin de les transformer en tables Hive.

>*Les tables finales sont retrouvées dans :*

**Nom de la database**: raw_reporting_treso

**Nom de la table** : treso_dea_prev

>*Les fichiers sources sont présents dans le dossier suivant:*

**Emplacement réseau** : T:\RAC\Plateforme\1 - Collecte des données sources auprès des OP\19 - Tresorerie\ajout_plateforme



###  **Organisation du dépôt**

Cette section fait une brève présentation du contenu de ce dépôt.


```
├── code/
│   ├── prev/
│       ├── chargement_prévision.py
│       ├── multiple_sheet_process.py
│       ├── single_sheet_process.py
│       ├── resize_hdfs_files.py
│       ├── ingestion_retraitements.py
│   ├── sage/
│       ├── chargement_sage_norm.py
|   |── Streamlit/
|       |── pages_streamlit/
|           |──page_prev.py 
|           |──page_sage.py 
|   |── utils/
|       |──dev.py
|       |──page.py    
|   |── dashboard
│   ├── monLogger.py
│   ├── utils.py
│   ├── maconfig.ini
│
├── logs/
│   └── Prev/

├── notebooks/
│   └── Analysis.ipynb
│   └── Data_quality.ipynb
│   └── Standarisation_reporting_multi_sheet.ipynb
│   └── Standarisation_reporting_single_sheet.ipynb
│   └── Monitoring automatique.ipynb
└── README.md

```

> **Les scripts**

Le dossier `code` contient l'ensemble des scripts de chargement et de création de Dashboard: 

* `chargement_prévision.py` : ce fichier contient le script de chargement des données depuis tous les fichiers excel présents dans l'emplacement choisi.

* `multiple_sheet_process.py` : ce fichier contient la fonction prevision_data_ingestion_multiple_sheet, utile pour le traitement et chargement des fichiers excel à feuilles multiples.

* `single_sheet_process.py` : ce fichier contient la fonction prevision_data_ingestion_single_sheet, utile pour le traitement et chargement des fichiers excel à feuille unique.

* `resize_hdfs_files.py` : script pour resizer les tailles des fichiers HDFS crées.

* `ingestion_retraitements.py` : script pour l'ingestion des retraitements réalisés par la sDFT.

* `chargement_sage_norm.py` : ce fichier contient le script de chargement des données de l'export SAGE dont la date de montant est celle du 10 janvier 2022.

* `monLogger.py` : ce fichier contient l'ensemble des fonctions de création de log.

* `utils.py` : ce fichier contient l'ensemble des fonctions utilitaires du projet.

* `page_prev.py` : ce fichier contient les graphes pour les données de prévision.

* `page_sage.py` : ce fichier contient les graphes pour les données de SAGE.

* `page.py` : Il s'agit du fichier qui définit la classe "page".

* `dev.py` : Fichier contenant toutes les fonctions utiles utilisées dans la création du dashboard.





> **Les notebooks**

Le dossier `notebooks` contient les notebooks suivants :


* `Analysis.ipynb` : ce notebook est un notebook brouillon duquel je me sers pour développer les fonctions et débugger les bouts de code.
* `Data_quality.ipynb` : ce notebook a servi pour développer un script de Data quality qui jugera la qualité du Dataframe produit par les deux fonctions: prevision_data_ingestion_multiple_sheet et prevision_data_ingestion_single_sheet.
* `Dataviz.ipynb` : ce notebook a servi pour tracer les toutes premières courbes et graphes d'exploration de données d'équilibre technique. 
* `Standarisation_reporting_multi_sheet.ipynb` : ce notebook a servi pour développer la fonction prevision_data_ingestion_multiple_sheet. Cette fonction permet de créer un dataframe pandas et depuis un fichier excel à feuilles multiples, puis enregistrer le dataframe sous une table Hive. 
* `Standarisation_reporting_single_sheet.ipynb` : ce notebook a servi pour développer la fonction prevision_data_ingestion_single_sheet. Cette fonction permet de créer un dataframe pandas et depuis un fichier excel à feuille unique, puis enregistrer le dataframe sous une table Hive. 
* `Monitoring automatique.ipynb` : ce notebook contient le travail sur la proposition d'automatisation des contôles de cohérence réalisés par la sDFT.
* `PREV_VIZ.ipynb` : ce notebook contient les premiers élèments qui ont servi pour tracer les graphes du dashboard.
* `ingestion_code_flux_code_budget.ipynb` : ce notebook a permis l'ingestion de la table de regroupement des codes budgétaires. 



> **Fichier de config**

* `maconfig.ini` : ce fichier de config regroupe l'ensemble des valeurs possibles et classées "normales" pour la colonne nature_montant. Toute valeur de 'nature_montant' en dehors de cette liste sera anormale, le dataframe créé ne respectera pas ainsi les critères de Data quality. 

### **Contraintes sur le fichier de source excel à respecter**
Le script permet la lecture des fichiers excel d'une manière plus ou moins intelligente pour s'adapter aux différents templates définis, mais possède tout de même quelques limitations. Il s'appuye sur quelques contraites qui doivent être définies: 

* `Date (version_prev)` : La date doit être présente dans le nom du fichier, idéalement elle doit être clairement définie (ex: 2020-10-19).
* `Année` : Le traitement varie selon le nombre de feuilles du fichier:
    * **single_sheet** : l'année doit être présente dans la première ligne, au dessus de la ligne regroupant les mois d'observation ou de prévision.
    * **multiple_sheet** : l'année doit être présente comme nom de la feuille du fichier excel.
* `Forme du tableau de restitution excel` : Le script lance des recherches pour retrouver le colonnes et la ligne qui délimitent la partie du excel souhaité. 
    * Pour la recherche verticale, la ligne limite est la ligne qui suit celle où le nom `emprunts` est défini, l'idée alors est que la ligne `Intérêts sur emprunts` soit toujours l'avant dernière ligne du tableau des prévisions.
    * Pour la recherche horizontale, le script s'arrête sur la colonne vide ou la colonne contenant du texte, l'idée alors est d'avoir une colonne vide juste après la colonne de la dernière prévision. Le script vérifie également si la colonne contient un nom de mois, une colonne sans nom de mois peut constituer une colonne d'arrêt.


# 2- Réalisation d'un dashboard de reporting  

Réalisé grâce à Streamlit, le dashboard contient deux pages, une première pour les données Sage, puis une seconde page pour les données de prévision. Chacune des deux permet de réaliser le suivi et le contrôle des données de trésorerie d'une manière fluide et dynamique.

Sur la `barre latérale`, nous retrouvons quelques filtres afin de raffiner notre rechercher et affichage de visuels:

- Maille (Code flux, Code budgétaire) : permet de choisir le regroupement sohaité.
- Intervalle de dates : permet de filtrer sur un intervalle choisi pour l'ensemble des graphes de la partit KPI et visualisations.
- Fréquence : permet le choix de fréquences des axes d'abscisse.

## 1- Page des données Sage

Deux choix se présentent : 
-   Données à la maille code flux
-   Données à la maille code budgétaire.

>**Partie contrôle de données**

Sur cette page nous retrouvons un premier graphe permettant le contrôle d'ingesion des données dans la plateforme. Mettant en lien le nombre de fichiers chargés et le différents valeurs "date_montant", il est possible de conclure le nombre de fois où un opération flux datée est représentée.

Le second graphe est une simple distrubution des différents libellés, il est utile pour comprendre si un code flux est usuel ou pas. 

>**Partie KPI et visualisations**

On y retrouve en premier 3 indicateurs; Solde initial, Solde avant décisions et solde Final. Cela pour compte en banque choisi. 

Un peu plus bas, on retrouve l'évolution du solde du compte en banque en question. 

Finalement, les deux graphes côte à côte affichent les recettes et les dépenses sur l'ensembles des comptes en banque.

## 2- Page des données Sage

Conçernant la deuxième page, nous trouverons 3 graphes :
- L'évolution sur une année des flux de recettes, dépenses et des autres flux. Les filtres à droite du graphe permettent de chosir l'année de prévision, et la date de la version de prévision. 
L'objectif est de pouvoir lire et comparer sur un seul graphe les chiffres des montants d'équilibre technique réalisés et prévus. 
- Juste en bas, on retrouve uun tracé du cumul des différentes grandeurs présentes dans le premier barplot.
- Dernièrement, on peut rerouver un barplot à 3 filtres; année d'observation, version de la prévision et nature montant. Il permet d'effectuer des zooms sur des nature montants en particuliers et donner plus de liberté de choix à l'utilisateur de l'interface.

