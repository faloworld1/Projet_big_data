import json
import requests
import boto3
from datetime import datetime

# Configuration AWS S3
s3 = boto3.client("s3", region_name="us-east-1")  # Remplace par ta région

# URL de l'API CoinGecko
url = 'https://api.coingecko.com/api/v3/coins/markets'
parameters = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 100,
    'page': 1,
    'price_change_percentage': '1h,24h,7d'
}

def lambda_handler(event, context):
    try:
        # Récupération des données
        response = requests.get(url, params=parameters)
        
        # Vérifier le statut HTTP
        if response.status_code != 200:
            raise Exception(f"Erreur API {response.status_code}: {response.text}")

        data = response.json()

        # Vérifier si la réponse est une liste
        if isinstance(data, list):
            data = {"results": data}  # Convertir la liste en dictionnaire

        json_data = json.dumps(data).encode("utf-8")  # Encodage en bytes

        # Nom du fichier basé sur l'heure UTC
        bucket_name = "raw-datas-projet"
        file_name = f"finance/crypto_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"

        # Envoi vers S3 avec Content-Type
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType='application/json'
        )


        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Données envoyées sur S3 : {file_name}"})
        }
    
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
