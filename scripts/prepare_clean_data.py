import pandas as pd
from pathlib import Path

# Paramètres
SOURCE = "data/healthcare_dataset.csv"
OUT_CLEAN = "data/healthcare_cleaned.csv"
OUT_REJECTS = "data/healthcare_rejects.csv"

# Schéma attendu
REQUIRED_COLS = [
    "Name","Age","Gender","Blood Type","Medical Condition","Date of Admission",
    "Doctor","Hospital","Insurance Provider","Billing Amount","Room Number",
    "Admission Type","Discharge Date","Medication","Test Results"
]

GENDER_SET = {"Male","Female"}
BLOOD_SET = {"A+","A-","B+","B-","AB+","AB-","O+","O-"}

def main():
    Path(OUT_CLEAN).parent.mkdir(parents=True, exist_ok=True)

    # Lecture
    df = pd.read_csv(SOURCE, dtype=str, keep_default_na=False)

    # 1) Vérification des colonnes
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Colonnes manquantes: {missing}")

    # 2) Contrôles de types/catégoriels
    def to_int_ok(s): 
        try: int(str(s).strip()); return True
        except: return False
    def to_float_ok(s): 
        try: float(str(s).strip()); return True
        except: return False
    def to_date_ok(s):
        try: 
            pd.to_datetime(s, errors="raise")
            return True
        except: 
            return False

    df["_Age_ok"] = df["Age"].apply(to_int_ok)
    df["_Room_ok"] = df["Room Number"].apply(to_int_ok)
    df["_Amount_ok"] = df["Billing Amount"].apply(to_float_ok)
    df["_Adm_ok"] = df["Date of Admission"].apply(to_date_ok)
    # Discharge facultatif vide autorisé
    df["_Dis_ok"] = df["Discharge Date"].apply(lambda x: True if str(x).strip()=="" else to_date_ok(x))

    df["_Gender_ok"] = df["Gender"].apply(lambda x: str(x).strip().capitalize() in GENDER_SET)
    df["_Blood_ok"] = df["Blood Type"].apply(lambda x: str(x).strip().upper() in BLOOD_SET)

    # 3) Valeurs manquantes sur colonnes critiques (Name, Date of Admission, Hospital)
    critical = ["Name","Date of Admission","Hospital"]
    df["_Critical_ok"] = df[critical].apply(lambda r: all(str(v).strip()!="" for v in r), axis=1)

    # 4) Détection des doublons (clé métier simple)
    key = (
        df["Name"].astype(str).str.strip().str.upper()
        + "|" + df["Date of Admission"].astype(str).str.strip()
        + "|" + df["Hospital"].astype(str).str.strip().str.upper()
    )
    df["_dup"] = key.duplicated(keep="first")

    # Lignes valides si tous les contrôles passent et pas doublon
    checks = ["_Age_ok","_Room_ok","_Amount_ok","_Adm_ok","_Dis_ok","_Gender_ok","_Blood_ok","_Critical_ok"]
    df["_valid"] = df[checks].all(axis=1) & (~df["_dup"])

    # Séparation nettoyé / rejets
    kept = df[df["_valid"]].drop(columns=[c for c in df.columns if c.startswith("_")])
    rej  = df[~df["_valid"]].copy()

    # Raison du rejet
    def reason(row):
        reasons = []
        for c in checks:
            if not row[c]: reasons.append(c.replace("_",""))
        if row["_dup"]: reasons.append("DUPLICATE")
        return ",".join(reasons) if reasons else "UNKNOWN"
    rej["_reject_reason"] = rej.apply(reason, axis=1)

    # Nettoyage des colonnes techniques dans rejets
    # On conserve _reject_reason pour l’audit
    for c in list(rej.columns):
        if c.startswith("_") and c != "_reject_reason":
            rej.drop(columns=[c], inplace=True)

    # Écriture
    kept.to_csv(OUT_CLEAN, index=False)
    rej.to_csv(OUT_REJECTS, index=False)

    # Petit récapitulatif
    print({
        "source_rows": len(df),
        "kept_rows": len(kept),
        "rejected_rows": len(rej),
        "clean_csv": OUT_CLEAN,
        "rejects_csv": OUT_REJECTS
    })

if __name__ == "__main__":
    main()
