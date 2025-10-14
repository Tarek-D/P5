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

Model de validator : 

validator: {
  $jsonSchema: {
    bsonType: "object",
    required: ["patient","visit","medical","admin","billing","src"],
    properties: {
      patient: {
        bsonType: "object",
        required: ["name","age","gender","blood_type"],
        properties: {
          name: {bsonType: "string"},
          age: {bsonType: "int"},
          gender: {bsonType: "string"},
          blood_type: {bsonType: "string"}
        }
      },
      visit: {
        bsonType: "object",
        required: ["admission_type","room_number"],
        properties: {
          admission_date: {bsonType: ["date","null"]},
          discharge_date: {bsonType: ["date","null"]},
          admission_type: {bsonType: "string"},
          room_number: {bsonType: "int"}
        }
      },
      medical: {
        bsonType: "object",
        required: ["condition","medication","test_results"],
        properties: {
          condition: {bsonType: "string"},
          medication: {bsonType: "string"},
          test_results: {bsonType: "string"}
        }
      },
      admin: {
        bsonType: "object",
        required: ["doctor","hospital","insurance_provider"],
        properties: {
          doctor: {bsonType: "string"},
          hospital: {bsonType: "string"},
          insurance_provider: {bsonType: "string"}
        }
      },
      billing: {
        bsonType: "object",
        required: ["amount"],
        properties: {
          amount: {bsonType: "decimal"}
        }
      },
      src: {
        bsonType: "object",
        required: ["file","ingested_at"],
        properties: {
          file: {bsonType: "string"},
          ingested_at: {bsonType: "date"}
        }
      }
    }
  }
}
