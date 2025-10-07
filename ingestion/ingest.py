# ingestion/ingest.py
import csv, sys, json, hashlib, datetime as dt, os
from pathlib import Path
import typer
import pandas as pd
from bson.decimal128 import Decimal128
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv

load_dotenv()
app = typer.Typer(no_args_is_help=True)

REQUIRED_COLS = [
    "Name","Age","Gender","Blood Type","Medical Condition","Date of Admission",
    "Doctor","Hospital","Insurance Provider","Billing Amount","Room Number",
    "Admission Type","Discharge Date","Medication","Test Results"
]

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

@app.command()
def validate(csv_path: str, report: str = "reports/pre_ingest.json"):
    p = Path(csv_path)
    assert p.exists(), f"CSV not found: {p}"
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    missing_cols = [c for c in REQUIRED_COLS if c not in cols]
    extra_cols = [c for c in cols if c not in REQUIRED_COLS]

    def to_int(s):
        try: return int(str(s).strip())
        except: return None
    def to_float(s):
        try: return float(str(s).strip())
        except: return None
    def to_date(s):
        try: return pd.to_datetime(s, errors="raise").date()
        except: return None

    checks = {}
    checks["row_count"] = len(df)
    checks["columns_present"] = cols
    checks["missing_required_columns"] = missing_cols
    checks["extra_columns"] = extra_cols

    df["_Age_ok"] = df["Age"].map(to_int)
    df["_Room_ok"] = df["Room Number"].map(to_int)
    df["_Amount_ok"] = df["Billing Amount"].map(to_float)

    df["_Adm_ok"] = df["Date of Admission"].map(to_date)
    df["_Dis_ok"] = df["Discharge Date"].map(lambda s: to_date(s) if str(s).strip() != "" else None)

    genders = {"Male","Female"}
    bloods = {"A+","A-","B+","B-","AB+","AB-","O+","O-"}
    df["_Gender_ok"] = df["Gender"].map(lambda x: str(x).strip().capitalize() in genders)
    df["_Blood_ok"] = df["Blood Type"].map(lambda x: str(x).strip().upper() in bloods)

    checks["integrity"] = {
        "Age_numeric_invalid": int(df["_Age_ok"].isna().sum()),
        "Room_numeric_invalid": int(df["_Room_ok"].isna().sum()),
        "Amount_numeric_invalid": int(df["_Amount_ok"].isna().sum()),
        "Admission_date_invalid": int(df["_Adm_ok"].isna().sum()),
        "Discharge_date_invalid": int(df["_Dis_ok"].isna().sum()),
        "Gender_enum_invalid": int((~df["_Gender_ok"]).sum()),
        "Blood_enum_invalid": int((~df["_Blood_ok"]).sum()),
        "missing_values_by_column": {
            c: int((df[c].astype(str).str.strip()=="").sum()) for c in REQUIRED_COLS if c in df.columns
        }
    }

    key_cols = ["Name","Date of Admission","Hospital"]
    if all(c in df.columns for c in key_cols):
        df["_key"] = (df["Name"].astype(str).str.strip().str.upper()
                      + "|" + df["Date of Admission"].astype(str).str.strip()
                      + "|" + df["Hospital"].astype(str).str.strip().str.upper())
        dups = int(df.duplicated("_key").sum())
    else:
        dups = None
    checks["potential_duplicates"] = dups

    checks["source_sha256"] = sha256_file(p)
    Path(report).parent.mkdir(parents=True, exist_ok=True)
    with open(report, "w") as f:
        json.dump(checks, f, indent=2, default=str)
    print(f"Wrote {report}")

@app.command()
def load(csv_path: str,
         mongo_uri: str = os.getenv("MONGO_URI", "mongodb://app_user:app_pass@localhost:27017/healthcare?authSource=healthcare"),
         db_name: str = "healthcare",
         coll_name: str = "encounters",
         rejects_path: str = "data/rejects.jsonl",
         chunk_size: int = 5000):
    p = Path(csv_path); assert p.exists(), f"CSV not found: {p}"
    client = MongoClient(mongo_uri)
    db = client[db_name]; coll = db[coll_name]
    bloods = {"A+","A-","B+","B-","AB+","AB-","O+","O-"}
    genders = {"Male","Female"}

    def parse_row(r):
        try:
            name = str(r["Name"]).strip().title()
            age = int(str(r["Age"]).strip())
            gender = str(r["Gender"]).strip().capitalize()
            blood = str(r["Blood Type"]).strip().upper()
            cond = str(r["Medical Condition"]).strip()
            adm = pd.to_datetime(r["Date of Admission"], errors="raise")
            dis_raw = str(r.get("Discharge Date","")).strip()
            dis = pd.to_datetime(dis_raw, errors="raise") if dis_raw else None
            doctor = str(r["Doctor"]).strip()
            hospital = str(r["Hospital"]).strip()
            insurer = str(r["Insurance Provider"]).strip()
            amount = Decimal128(str(float(str(r["Billing Amount"]).strip())))
            room = int(str(r["Room Number"]).strip())
            adm_type = str(r["Admission Type"]).strip()
            medication = str(r["Medication"]).strip()
            test = str(r["Test Results"]).strip()

            if gender not in genders or blood not in bloods:
                raise ValueError("enum")
            doc = {
                "patient": {"name": name, "age": age, "gender": gender, "blood_type": blood},
                "visit": {"admission_date": adm, "discharge_date": dis, "admission_type": adm_type, "room_number": room},
                "medical": {"condition": cond, "medication": medication, "test_results": test},
                "admin": {"doctor": doctor, "hospital": hospital, "insurance_provider": insurer},
                "billing": {"amount": amount},
                "src": {"file": str(p), "ingested_at": pd.Timestamp.utcnow()}
            }
            return doc, None
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"

    Path(rejects_path).parent.mkdir(parents=True, exist_ok=True)
    with open(rejects_path, "w", encoding="utf-8") as rej_fp:
        total, ok, bad = 0, 0, 0
        for df in pd.read_csv(p, dtype=str, keep_default_na=False, chunksize=chunk_size):
            ops = []
            for _, row in df.iterrows():
                total += 1
                doc, err = parse_row(row)
                if err:
                    bad += 1
                    rej_fp.write(json.dumps({"row": dict(row), "error": err}, ensure_ascii=False) + "\n")
                else:
                    ops.append(InsertOne(doc))
            if ops:
                coll.bulk_write(ops, ordered=False)
                ok += len(ops)
    print(json.dumps({"read": total, "inserted": ok, "rejected": bad}, indent=2))

@app.command()
def postcheck(mongo_uri: str = os.getenv("MONGO_URI", "mongodb://app_user:app_pass@localhost:27017/healthcare?authSource=healthcare"),
              db_name: str = "healthcare",
              coll_name: str = "encounters",
              report: str = "reports/post_ingest.json"):
    client = MongoClient(mongo_uri)
    db = client[db_name]; coll = db[coll_name]
    total = coll.estimated_document_count()
    agg = list(coll.aggregate([
        {"$group": {"_id": "$medical.condition", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": -1}},
        {"$limit": 10}
    ]))
    stats = {"total_docs": total, "by_condition_top10": agg}
    Path(report).parent.mkdir(parents=True, exist_ok=True)
    with open(report, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(f"Wrote {report}")

@app.command()
def test(csv_path: str = "data/healthcare_dataset.csv",
         mongo_uri: str = os.getenv("MONGO_URI", "mongodb://app_user:app_pass@localhost:27017/healthcare?authSource=healthcare"),
         db_name: str = "healthcare",
         coll_name: str = "encounters"):
    p = Path(csv_path); assert p.exists(), f"CSV not found: {p}"
    # 1) Re-lire et compter
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    csv_rows = len(df)
    # 2) BDD compte
    client = MongoClient(mongo_uri)
    total = client[db_name][coll_name].estimated_document_count()
    # 3) Assertions simples
    assert total == csv_rows, f"Count mismatch: csv={csv_rows}, mongo={total}"
    print(json.dumps({"csv_rows": csv_rows, "mongo_docs": total, "status": "ok"}, indent=2))


if __name__ == "__main__":
    app()

