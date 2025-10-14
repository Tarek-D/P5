import pandas as pd
from scripts.prepare_clean_data import norm, to_int_ok, to_float_ok, to_date_ok, build_key, prepare_frames

def test_norm():
    assert norm("  alice  ") == "ALICE"
    assert norm("") == ""

def test_type_checks():
    assert to_int_ok("42") and not to_int_ok("x")
    assert to_float_ok("3.14") and not to_float_ok("x")
    assert to_date_ok("2024-01-01") and not to_date_ok("2024-13-01")

def test_build_key_and_dupes():
    df = pd.DataFrame([
        {"Name":"John","Date of Admission":"2020-01-01","Hospital":"A","Age":"30","Gender":"Male","Blood Type":"A+",
         "Medical Condition":"X","Doctor":"D","Insurance Provider":"I","Billing Amount":"10","Room Number":"1",
         "Admission Type":"Urgent","Discharge Date":"","Medication":"M","Test Results":"R"},
        {"Name":"john ","Date of Admission":"2020-01-01","Hospital":"a","Age":"31","Gender":"Male","Blood Type":"A+",
         "Medical Condition":"X","Doctor":"D","Insurance Provider":"I","Billing Amount":"20","Room Number":"2",
         "Admission Type":"Urgent","Discharge Date":"","Medication":"M","Test Results":"R"},
    ])
    key = build_key(df)
    assert key.iloc[0] == key.iloc[1]

def test_prepare_frames_keeps_first_duplicate():
    df = pd.DataFrame([
        {"Name":"John","Date of Admission":"2020-01-01","Hospital":"A","Age":"30","Gender":"Male","Blood Type":"A+",
         "Medical Condition":"X","Doctor":"D","Insurance Provider":"I","Billing Amount":"10","Room Number":"1",
         "Admission Type":"Urgent","Discharge Date":"","Medication":"M","Test Results":"R"},
        {"Name":"john ","Date of Admission":"2020-01-01","Hospital":"a","Age":"31","Gender":"Male","Blood Type":"A+",
         "Medical Condition":"X","Doctor":"D","Insurance Provider":"I","Billing Amount":"20","Room Number":"2",
         "Admission Type":"Urgent","Discharge Date":"","Medication":"M","Test Results":"R"},
        {"Name":"Eve","Date of Admission":"bad-date","Hospital":"B","Age":"x","Gender":"Unknown","Blood Type":"Z",
         "Medical Condition":"Y","Doctor":"D","Insurance Provider":"I","Billing Amount":"n/a","Room Number":"n/a",
         "Admission Type":"Elective","Discharge Date":"", "Medication":"M","Test Results":"R"},
    ])
    clean, rej = prepare_frames(df)
    # 1ère occurrence de John conservée, la 2e rejetée en doublon
    assert len(clean) == 1
    assert clean.iloc[0]["Name"].strip().lower() == "john"
    assert len(rej) == 2
    reasons = ",".join(rej["_reject_reason"].tolist())
    assert "DUPLICATE" in reasons
    assert "Age" in reasons and "Amount" in reasons or "AMOUNT" in reasons