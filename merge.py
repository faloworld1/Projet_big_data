import os
import glob
import pandas as pd
import time

def merge_new_csv_files(input_directory, output_file, processed_files_log="processed_files.txt"):
    print("Dossier d'entrée:", input_directory)
    
    # Charger la liste des fichiers déjà traités (s'il existe)
    processed_files = set()
    if os.path.exists(processed_files_log):
        with open(processed_files_log, "r") as log:
            for line in log:
                processed_files.add(line.strip())
    
    # Lister tous les fichiers CSV dans le dossier d'entrée
    csv_files = glob.glob(os.path.join(input_directory, "*.csv"))
    print("Fichiers CSV trouvés:", csv_files)
    
    # Sélectionner uniquement les fichiers non traités
    new_files = [f for f in csv_files if f not in processed_files]
    
    if not new_files:
        print("Aucun nouveau fichier à traiter.")
        return
    
    # Vérifier que output_file n'est pas déjà un dossier
    if os.path.isdir(output_file):
        print(f"L'output_file '{output_file}' est un dossier. Veuillez le supprimer ou choisir un autre nom de fichier.")
        return
    
    for file in new_files:
        try:
            # Vérifier si le fichier est vide (taille = 0)
            if os.stat(file).st_size == 0:
                print(f"Le fichier {file} est vide, passage...")
                with open(processed_files_log, "a") as log:
                    log.write(file + "\n")
                continue

            print(f"Traitement du fichier: {file}")
            df = pd.read_csv(file)
            
            # Si le DataFrame est vide, on passe aussi
            if df.empty:
                print(f"Le fichier {file} ne contient pas de données, passage...")
                with open(processed_files_log, "a") as log:
                    log.write(file + "\n")
                continue

            # Append dans le fichier de sortie
            if os.path.exists(output_file):
                df.to_csv(output_file, mode="a", index=False, header=False)
            else:
                df.to_csv(output_file, mode="w", index=False, header=True)
            
            # Marquer le fichier comme traité
            with open(processed_files_log, "a") as log:
                log.write(file + "\n")
            
            print(f"Fichier traité : {file}")
        except Exception as e:
            print(f"Erreur lors du traitement du fichier {file}: {e}")

if __name__ == "__main__":
    # Répertoire contenant les fichiers CSV générés par Spark
    input_directory = "data/output/device_data_1.csv"
    # Chemin complet pour le fichier CSV unique de sortie
    output_file = "data/output/device_data_single.csv"
    
    # Exécution en continu avec une pause de 10 secondes entre chaque vérification
    while True:
        merge_new_csv_files(input_directory, output_file)
        print("Attente de 10 secondes avant la prochaine vérification...\n")
        time.sleep(10)
