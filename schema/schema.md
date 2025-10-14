# Schéma de la collection healthcare

{
  "_id": ObjectId,
  "patient": {
    "name": String,
    "age": Int32,
    "gender": String,
    "blood_type": String
  },
  "visit": {
    "admission_date": Date,
    "discharge_date": Date | null,
    "admission_type": String,
    "room_number": Int32
  },
  "medical": {
    "condition": String,
    "medication": String,
    "test_results": String
  },
  "admin": {
    "doctor": String,
    "hospital": String,
    "insurance_provider": String
  },
  "billing": {
    "amount": Decimal128
  },
  "src": {
    "file": String,
    "ingested_at": Date
  }
}

