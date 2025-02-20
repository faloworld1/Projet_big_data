# Projet de Traitement de Données en Continu

Ce dépôt regroupe l'ensemble des éléments d'un projet complet de traitement de données en continu qui combine plusieurs technologies (Spark, Kafka et Python) pour :

- **Générer des données aléatoires**
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
git clone https://github.com/faloworld1/Traitement-de-donn-es-en-temps-r-el-avec-spark
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
