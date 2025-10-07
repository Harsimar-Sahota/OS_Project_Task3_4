# lambda_function.py
# Simulated serverless function: reads All_Diets.csv from Azurite, computes averages/top-protein/ratios,
# and writes results to simulated_nosql/ as JSON and CSV files.

import os
import io
import json
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient

# Connection string for local Azurite emulator (default)
AZURITE_CONN = os.getenv("AZURITE_CONN") or (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

def download_blob_to_bytes(container_name="datasets", blob_name="All_Diets.csv", conn_str=AZURITE_CONN):
    bsc = BlobServiceClient.from_connection_string(conn_str)
    container_client = bsc.get_container_client(container_name)
    # create container if missing (safe)
    try:
        container_client.create_container()
    except Exception:
        pass
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data

def process_dataframe(df: pd.DataFrame):
    # Ensure columns exist
    required = ["Protein(g)", "Carbs(g)", "Fat(g)"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # Convert to numeric (coerce errors -> NaN)
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Fill missing values by diet-type mean, otherwise global mean
    df[required] = df.groupby("Diet_type")[required].transform(lambda x: x.fillna(x.mean()))
    df[required] = df[required].fillna(df[required].mean())

    # Average macros per diet type
    avg_macros = df.groupby("Diet_type")[required].mean().round(2).reset_index()

    # Top 5 protein-rich recipes for each diet
    top_protein = df.sort_values("Protein(g)", ascending=False).groupby("Diet_type").head(5).reset_index(drop=True)

    # New ratio columns (safe division)
    df["Protein_to_Carbs_ratio"] = df["Protein(g)"] / df["Carbs(g)"].replace({0: np.nan})
    df["Carbs_to_Fat_ratio"] = df["Carbs(g)"] / df["Fat(g)"].replace({0: np.nan})

    # Diet with highest avg protein
    diet_highest_avg_protein = avg_macros.loc[avg_macros["Protein(g)"].idxmax(), "Diet_type"]

    return {
        "avg_macros": avg_macros,
        "top_protein": top_protein,
        "recipes_df": df,
        "diet_with_highest_avg_protein": diet_highest_avg_protein
    }

def save_results(result, out_dir="simulated_nosql"):
    os.makedirs(out_dir, exist_ok=True)

    # Save avg_macros and top_protein as JSON
    avg_json = result["avg_macros"].to_dict(orient="records")
    top_json = result["top_protein"].to_dict(orient="records")

    with open(os.path.join(out_dir, "avg_macros.json"), "w") as f:
        json.dump(avg_json, f, indent=2)

    with open(os.path.join(out_dir, "top_protein.json"), "w") as f:
        json.dump(top_json, f, indent=2)

    # Save full recipes with ratios as CSV
    result["recipes_df"].to_csv(os.path.join(out_dir, "recipes_with_ratios.csv"), index=False)

    with open(os.path.join(out_dir, "metadata.json"), "w") as f:
        json.dump({"diet_with_highest_avg_protein": result["diet_with_highest_avg_protein"]}, f, indent=2)

    print(f"Saved results under {out_dir}/")

def process_nutritional_data_from_azurite(container_name="datasets", blob_name="All_Diets.csv"):
    print("Downloading CSV from Azurite...")
    data_bytes = download_blob_to_bytes(container_name=container_name, blob_name=blob_name)
    print(f"Downloaded {len(data_bytes)} bytes.")
    df = pd.read_csv(io.BytesIO(data_bytes))
    print(f"Loaded DataFrame: {df.shape[0]} rows x {df.shape[1]} cols")
    result = process_dataframe(df)
    save_results(result)
    print("Processing complete.")
    return result

if __name__ == "__main__":
    # Try default blob name first. If it fails because blob name differs (space/underscore), give clear message.
    try:
        process_nutritional_data_from_azurite()
    except Exception as e:
        msg = str(e)
        print("Processing failed:", msg)
        print("If the blob name uses spaces (e.g. 'All Diets.csv') or a different name, re-run with:")
        print("  ./venv/bin/python lambda_function.py  # or edit the default blob_name in the function")
        raise
