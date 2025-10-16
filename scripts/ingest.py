#!/usr/bin/env python3
# scripts/ingest.py

import os
import json
from pathlib import Path
import typer
import pandas as pd
from pymongo import MongoClient, InsertOne
from bson.decimal128 import Decimal128
from dotenv import load_dotenv

# Charger les variables d'environnement 
load_dotenv()

app = typer.Typer(no_args_is_help=True)

# Par défaut, consommer le CSV "après préparation" depuis /app/data
DEFAULT_INPUT = os.getenv("INGEST_INPUT", "/app/data/healthcare_cleaned.csv")
DEFAULT_DB = os.getenv("MONGO_DB", "healthcare")
DEFAULT_COLL = os.getenv("MONGO_COLL", "encounters")
DEFAULT_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/healthcare")

def row_to_doc(r: pd.Series, src_file: str) -> dict:
    """
    Convertit une ligne du CSV nettoyé en document MongoDB structuré.
    Les colonnes et types ont été validés en amont.
    """
    # Champs patient
    name = str(r["Name"]).strip().title()
    age = int(str(r["Age"]).strip())
    gender = str(r["Gender"]).strip().capitalize()
    blood = str(r["Blood Type"]).strip().upper()

    # Champs visite
    adm = pd.to_datetime(r["Date of Admission"], errors="coerce")
    dis_raw = str(r.get("Discharge Date", "")).strip()
    dis = pd.to_datetime(dis_raw, errors="coerce") if dis_raw else None
    adm_type = str(r["Admission Type"]).strip()
    room = int(str(r["Room Number"]).strip())

    # Champs médicaux
    cond = str(r["Medical Condition"]).strip()
    medication = str(r["Medication"]).strip()
    test = str(r["Test Results"]).strip()

    # Champs admin
    doctor = str(r["Doctor"]).strip()
    hospital = str(r["Hospital"]).strip()
    insurer = str(r["Insurance Provider"]).strip()

    # Facturation
    amount = Decimal128(str(float(str(r["Billing Amount"]).strip())))

    # Trace source
    src = {"file": src_file, "ingested_at": pd.Timestamp.utcnow()}

    return {
        "patient": {"name": name, "age": age, "gender": gender, "blood_type": blood},
        "visit": {
            "admission_date": adm,
            "discharge_date": dis,
            "admission_type": adm_type,
            "room_number": room,
        },
        "medical": {"condition": cond, "medication": medication, "test_results": test},
        "admin": {"doctor": doctor, "hospital": hospital, "insurance_provider": insurer},
        "billing": {"amount": amount},
        "src": src,
    }

@app.command()
def load(
    csv_path: str = DEFAULT_INPUT,
    mongo_uri: str = DEFAULT_URI,
    db_name: str = DEFAULT_DB,
    coll_name: str = DEFAULT_COLL,
    chunk_size: int = 5000,
):
    """
    Charge le CSV nettoyé en base MongoDB via insertions bulk.
    - Ne crée aucun index ici.
    - Supposé: dataset sans doublons et types déjà propres.
    """
    p = Path(csv_path)
    if not p.exists():
        raise SystemExit(f"CSV not found: {p}")

    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    total = 0
    inserted = 0

    # Lecture par chunks pour limiter la mémoire et améliorer les perfs
    for df in pd.read_csv(p, dtype=str, keep_default_na=False, chunksize=chunk_size):
        ops = []
        for _, row in df.iterrows():
            total += 1
            ops.append(InsertOne(row_to_doc(row, str(p))))
            if len(ops) >= chunk_size:
                coll.bulk_write(ops, ordered=False)
                inserted += len(ops)
                ops = []
        if ops:
            coll.bulk_write(ops, ordered=False)
            inserted += len(ops)

    print(json.dumps({"read": total, "inserted": inserted}, indent=2, default=str))

if __name__ == "__main__":
    app()
