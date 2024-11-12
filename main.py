import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta


load_dotenv()

# Hepiaders pour l'authentification
headers = {
    "Authorization": os.getenv('api_intercom'),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

mongo_pat = os.getenv('mongo_pat')
api_pipedrive = os.getenv('api_pipedrive')

client = MongoClient( f'mongodb+srv://{mongo_pat}',tls=True,tlsAllowInvalidCertificates=True)
db = client['legacy-api-management']
col_soc = db["societies"]
col_it = db["items"]
col_users = db["users"]
col_bills = db["bills"]

def get_rating():
    # URL de recherche pour obtenir les conversations
    search_url = "https://api.intercom.io/conversations/search"
    conversations_data = []

    # Timestamp pour la veille
    timestamp_yesterday = int((datetime.now() - timedelta(days=3)).timestamp())
    # timestamp_yesterday = int(datetime(2023, 1, 1).timestamp())

    # Requête de recherche pour obtenir les conversations mises à jour depuis hier avec une évaluation
    search_payload = {
        "query": {
            "operator": "AND",
            "value": [
                {
                    "field": "created_at",
                    "operator": ">",
                    "value": timestamp_yesterday
                },
                {
                    "field": "conversation_rating.score",
                    "operator": "IN",
                    "value": [0, 1, 2, 3, 4, 5]
                }
            ]
        },
        "per_page": 50  # Nombre maximal de résultats par page
    }

    # Gestion de la pagination pour récupérer toutes les pages
    page_number = 0
    while True:
        response = requests.post(search_url, headers=headers, json=search_payload)
        data = response.json()
        print(data)
        # Vérifier les erreurs de l'API
        if response.status_code != 200:
            print(f"Erreur API : {data}")
            break

        # Incrémenter et afficher le numéro de page actuel
        page_number += 1
        print(f"Traitement de la page {page_number}")

        # Extraire les informations de chaque conversation
        for conversation in data.get("conversations", []):
            author_info = conversation.get("source", {}).get("author", {})
            rating_info = conversation.get("conversation_rating", {})

            if author_info and rating_info:
                conversations_data.append({
                    "author_id": author_info.get("id"),
                    "author_name": author_info.get("name"),
                    "author_email": author_info.get("email"),
                    "rating_score": rating_info.get("rating"),
                    "rating_remark": rating_info.get("remark"),
                    "assigned_to": rating_info.get("teammate").get("id"),
                    "created_at": conversation.get("created_at", {}),
                })

        # Vérifier s'il y a une page suivante
        next_page = data.get("pages", {}).get("next")
        if next_page:
            search_payload['pagination'] = {"starting_after": next_page.get("starting_after")}
            print(f"Passage à la page suivante à partir de : {next_page.get('starting_after')}")
        else:
            print(f"Nombre total de pages traitées : {page_number}")
            break

        time.sleep(0.5)  # Pause pour éviter de dépasser les limites de l'API

    # Sauvegarder les données extraites dans un fichier CSV
    df = pd.DataFrame(conversations_data)
    df = df.sort_values(by = 'created_at')



    df.to_csv("conversations_ratings.csv", index=False)

    print("Les informations sur les auteurs et évaluations de la veille ont été sauvegardées dans 'conversations_ratings_yesterday.csv'.")
def get_company():
    import pandas as pd
    from pymongo import MongoClient

    # Load the CSV file into a DataFrame
    df = pd.read_csv(r"C:\Users\super\PycharmProjects\intercom\conversations_ratings.csv")
    df['created_at'] = pd.to_datetime(df['created_at'], unit='s')

    # Initialize an empty list to store company names
    company_names = []

    # Iterate over each email in the DataFrame to retrieve company names
    for email in df["author_email"]:
        user_doc = col_users.find_one({"email": email})
        if user_doc:
            user_id = user_doc["_id"]
            company = col_soc.find_one({"members.user": ObjectId(user_id)})
            company_name = (company['name'])
        else:
            user_id = ""
            company_name = ""
            print(f"No document found for email: {email}")
        print(company_name,user_id,email)
        company_names.append(company_name)
    # Add the company names to the DataFrame
    df["company_name"] = company_names
    id_to_name = {
        7456691: "Sarah",
        5160845: "Karine",
        7729712: "Eva",
        5432315: "Luis",
        7471429: "Gamze",
        5302396: "Karima",
        5615296: "Nourya",
        6814357: "Sholane",
        3746898: "Ruben",
        3746897: "Nicole"

    }
    df["assigned_to"] = df["assigned_to"].replace(id_to_name)
    # Save the updated DataFrame to a new CSV file
    df.to_csv(r"C:\Users\super\PycharmProjects\intercom\conversations_ratings_with_company.csv", index=False)

    print("The DataFrame has been updated with company names and saved.")

def merge():
    df_old = pd.read_csv(r"C:\Users\super\PycharmProjects\intercom\base_22.csv")
    df_new = pd.read_csv(r"C:\Users\super\PycharmProjects\intercom\conversations_ratings_with_company.csv")

    df_merged = df_new.set_index('created_at').combine_first(df_old.set_index('created_at')).reset_index()
    df_merged = df_merged.sort_values(by = 'created_at')

    rating_to_name = {
        1: "😠",
        2: "🙁",
        3: "😐",
        4: "😃",
        5: "🤩",

    }
    df_merged["emoticone"] = df_merged["rating_score"].replace(rating_to_name)



    df_merged.to_csv(r"C:\Users\super\PycharmProjects\intercom\final_rating.csv", index=False)

def update_drive():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json
        import pandas as pd

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        spreadsheet_id = '1ojx7fVnEDPUUCPtNYL_k0BixMfkP-Ex5XGMz7P9T1IE'
        credentials_file_path = r'C:\Users\super\PycharmProjects\intercom\creds\n8n-api-311609-115ae3a49fd9.json'

        # Charger les informations d'identification
        with open(credentials_file_path, 'r') as file:
            credentials_data = json.load(file)

        creds = Credentials.from_service_account_info(credentials_data, scopes=SCOPES)
        client = gspread.authorize(creds)

        # Ouvrir le fichier Google Sheets
        spreadsheet = client.open_by_key(spreadsheet_id)

        # Sélectionner la feuille 'ratings'
        sheet = spreadsheet.worksheet("ratings")

        # Lire le fichier CSV et charger les données dans la feuille
        csv_file = r'C:\Users\super\PycharmProjects\intercom\final_rating.csv'
        data = pd.read_csv(csv_file)
        data = data.fillna('')  # Remplir les valeurs manquantes avec des chaînes vides

        # Convertir les données en une liste de cellules pour update_cells
        cells = []
        for row_idx, row in enumerate([data.columns.values.tolist()] + data.values.tolist(), start=1):
            for col_idx, cell_value in enumerate(row, start=1):
                cell = gspread.Cell(row_idx, col_idx, cell_value)
                cells.append(cell)

        # Mise à jour de toutes les cellules d'un coup
        sheet.update_cells(cells)

        print("Data successfully uploaded to Google Sheet in 'ratings' sheet!")
    except Exception as e:
        raise RuntimeError(f"Erreur dans update_drive: {e}")



get_rating()
get_company()
merge()
update_drive()