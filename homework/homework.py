"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel



import os
import pandas as pd
from zipfile import ZipFile

def extract_data_from_zips(input_folder):
    """Extrae y combina los datos de archivos CSV dentro de archivos ZIP en una carpeta."""
    dataframes = []
    for zip_name in os.listdir(input_folder):
        with ZipFile(os.path.join(input_folder, zip_name)) as zip_ref:
            with zip_ref.open(zip_ref.namelist()[0]) as file:
                df = pd.read_csv(file).drop(columns=["Unnamed: 0"], errors='ignore')
                dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

def process_client_data(df):
    """Procesa y limpia los datos de clientes."""
    client = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client["job"] = client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA).str.replace("[.-]", "_", regex=True)
    client["credit_default"] = (client["credit_default"] == "yes").astype(int)
    client["mortgage"] = (client["mortgage"] == "yes").astype(int)
    return client

def process_campaign_data(df):
    """Procesa y limpia los datos de la campaña."""
    month_to_number = {"jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
                        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"}
    campaign = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
                   "previous_outcome", "campaign_outcome", "month", "day"]].copy()
    campaign["previous_outcome"] = (campaign["previous_outcome"] == "success").astype(int)
    campaign["campaign_outcome"] = (campaign["campaign_outcome"] == "yes").astype(int)
    campaign["month"] = campaign["month"].str.lower().map(month_to_number).fillna("00")
    campaign["last_contact_date"] = "2022-" + campaign["month"] + "-" + campaign["day"].astype(str).str.zfill(2)
    return campaign.drop(columns=["month", "day"])

def process_economics_data(df):
    """Procesa los datos económicos."""
    return df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

def save_dataframes(output_folder, client, campaign, economics):
    """Guarda los DataFrames en archivos CSV."""
    os.makedirs(output_folder, exist_ok=True)
    client.to_csv(os.path.join(output_folder, "client.csv"), index=False)
    campaign.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    economics.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

def clean_campaign_data():
    """Función principal que ejecuta todo el proceso de limpieza y guardado de datos."""
    input_folder, output_folder = os.path.join("files", "input"), os.path.join("files", "output")
    df = extract_data_from_zips(input_folder)
    save_dataframes(output_folder, process_client_data(df), process_campaign_data(df), process_economics_data(df))

if __name__ == "__main__":
    clean_campaign_data()