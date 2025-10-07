# ingestion/ingest.py
import csv, sys, json, hashlib, datetime as dt
from pathlib import Path
import typer
import pandas as pd

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
    # lecture robuste
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    missing_cols = [c for c in REQUIRED_COLS if c not in cols]
    extra_cols = [c for c in cols if c not in REQUIRED_COLS]
    # contrôles typage/cohérence
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

    # champs numériques
    df["_Age_ok"] = df["Age"].map(to_int)
    df["_Room_ok"] = df["Room Number"].map(to_int)
    df["_Amount_ok"] = df["Billing Amount"].map(to_float)

    # dates
    df["_Adm_ok"] = df["Date of Admission"].map(to_date)
    df["_Dis_ok"] = df["Discharge Date"].map(lambda s: to_date(s) if str(s).strip() != "" else None)

    # enums
    genders = {"Male","Female"}
    bloods = {"A+","A-","B+","B-","AB+","AB-","O+","O-"}
    df["_Gender_ok"] = df["Gender"].map(lambda x: str(x).strip().capitalize() in {"Male","Female"})
    df["_Blood_ok"] = df["Blood Type"].map(lambda x: str(x).strip().upper() in bloods)

    # stats d’intégrité
    def pct_bad(col):
        bad = df[col].isna() | (df[col]==None)
        return float(bad.sum())/len(df) if len(df) else 0.0

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

    # clé naturelle pour doublons potentiels
    key_cols = ["Name","Date of Admission","Hospital"]
    if all(c in df.columns for c in key_cols):
        df["_key"] = (df["Name"].astype(str).str.strip().str.upper()
                      + "|" + df["Date of Admission"].astype(str).str.strip()
                      + "|" + df["Hospital"].astype(str).str.strip().str.upper())
        dups = int(df.duplicated("_key").sum())
    else:
        dups = None
    checks["potential_duplicates"] = dups

    # hash source
    checks["source_sha256"] = sha256_file(p)
    Path(report).parent.mkdir(parents=True, exist_ok=True)
    with open(report, "w") as f:
        json.dump(checks, f, indent=2, default=str)
    print(f"Wrote {report}")

if __name__ == "__main__":
    app()
