# Projet de Traitement de Données massives en temps reel 

# Partie 1: Traitement en local

Ce dépôt regroupe l'ensemble des éléments d'un projet complet de traitement de données en continu qui combine plusieurs technologies (Spark, et Python) pour :

- **Scrapper des données sur le site **
- **Envoyer ces données dans Kafka**
- **Traiter ces données en streaming avec Spark** (transformations et écriture en fichiers CSV)
- **Fusionner les fichiers CSV générés en un fichier unique**
- **Présenter le projet** via une présentation PowerPoint

---

## Contenu du Dépôt

- **Présentation**
  - `Presentation_Dassi_LO_DEME.pdf`  
    La présentation PowerPoint décrivant l'architecture, les objectifs et les résultats du projet.

- **Traitement Spark**
  - `pratique/pyspark.py`  
    Le code Spark qui lit les données en streaming (par exemple depuis Kafka), effectue les transformations nécessaires et écrit les résultats sous forme de fichiers CSV dans un dossier.

- **Générateur de Données**
  - `data_creator/device_events.py`  
    Un script qui simule un flux de données en générant des données aléatoires.

- **Kafka Producer**
  - `data_creator/post_to_kafka.py`  
    Un script Python qui envoie les données générées dans Kafka afin qu'elles soient consommées par Spark.

- **Fusion CSV**
  - `merge.py`  
    Un script Python qui surveille en continu le dossier contenant les fichiers CSV générés par Spark et les fusionne en un fichier CSV unique. Le script gère les fichiers vides ou invalides en enregistrant les fichiers déjà traités dans `processed_files.txt`.

---

## Prérequis

- **Python 3.6+**
- **Apache Spark** (installé et configuré)
- **Kafka** (serveur opérationnel)
- **Bibliothèques Python** :
  - [`pandas`](https://pandas.pydata.org/) (installable via `pip install pandas`)
  - Modules standards : `os`, `glob`, `time`
- **Utiliser un docker avec ces trois outils intallés à défaut**
---

## Instructions d'Installation et d'Exécution


# 1. Cloner le dépôt
git clone https://github.com/faloworld1/Projet_big_data
cd votre-depot

# 2. Installer les dépendances Python
# (Si un fichier requirements.txt est fourni)
pip install -r requirements.txt

# Sinon, installez manuellement les bibliothèques nécessaires, par exemple :
pip install pandas

# 3. Configurer Kafka et Spark
# Assurez-vous que Kafka est installé, configuré et opérationnel.
# Configurez Apache Spark (local ou cluster).

# 4. Exécuter les composants du projet

# - Générateur de Données
python data_creator/device_events.py

# - Kafka Producer
python data_creator/post_to_kafka.py

# - Traitement Spark
spark-submit spark/traitement_spark.py

# - Fusion des Fichiers CSV
python merge.py

## Personnalisation


# - Fréquence de Vérification :
   Le script de fusion (merge_csv.py) vérifie le dossier toutes les 10 secondes par défaut.
   Pour ajuster cette fréquence, modifiez la valeur dans :
   time.sleep(10)

# - Chemins d'Accès :
   Adaptez les chemins vers les dossiers et fichiers dans chaque script selon l'organisation de votre projet et votre environnement.

# - Gestion des Fichiers Traités :
   Le script de fusion utilise un fichier processed_files.txt pour enregistrer les fichiers déjà traités et éviter leur re-traitement.
  Pour reprocesser des données, supprimez ou modifiez ce fichier.

---

# Partie 2: UTILISATION DU CLOUD: CAS DE AWS
# README - Projet de Traitement des Données Cryptographiques

## Description
Ce projet vise à récupérer, traiter et stocker des données de marché sur les cryptomonnaies en utilisant l'API CoinGecko. Il repose sur AWS Lambda, AWS Glue et Apache Spark pour automatiser l'extraction, la transformation et le stockage des données sur AWS S3.

## Technologies utilisées
- **AWS Lambda** : Récupération des données via l'API CoinGecko et stockage sur S3.
- **Amazon EventBridge**: execution automatique du scraping
- **AWS Glue & Apache Spark** : Transformation et nettoyage des données en streaming.
- **AWS S3** : Stockage des données brutes et transformées.
- **Python** : Développement des scripts de traitement des données.


## Structure du projet
- `lambda_function.py` : Script Lambda récupérant les données et les sauvegardant sur S3.
- `Processing_crypto_datas_v2.ipynb` : Notebook Apache Spark pour le traitement des données en streaming.
- `s3://raw-datas-projet/finance/` : Emplacement des données brutes sur S3.
- `s3://raw-datas-projet/output/` : Emplacement des données transformées sur S3.

## Installation et Exécution
### Prérequis
- Un compte AWS avec accès à S3, Lambda et Glue.
- Python 3.x installé avec les bibliothèques requises (`boto3`, `requests`, `pyspark`).

### Étapes
1. **Déployer la fonction AWS Lambda**
   - Modifier `lambda_function.py` avec votre région et bucket S3.
   - Déployer la fonction sur AWS Lambda avec les permissions S3.

2. **Exécuter le traitement Spark**
   - Lancer le notebook `Processing_crypto_datas_v2.ipynb`.
   - Vérifier les données traitées dans `s3://raw-datas-projet/output/`.
---

## Auteurs
- Ange DASSI NGUEGANG: ange.dassi.06@gmail.com
- IBRAHIMA FA LO: Loibrahimafa@gmail.com
- SAFIETOU DEME: Sofiedme@yahoo.com



