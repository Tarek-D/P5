# P5
Migrez des données médicales à l'aide du NoSQL

Voici un README prêt à placer à la racine du projet, couvrant le contexte, l’installation, l’exécution des commandes validate/load/postcheck, le schéma cible, les index et le dépannage, en s’appuyant sur le dataset et les rapports générés. [1][2][3]

# README — Migration CSV vers MongoDB

### Contexte
Ce projet migre un dataset médical au format CSV vers MongoDB avec un script Python qui valide, typage/normalise puis insère les données dans une collection encounters, en produisant des rapports avant et après ingestion pour la traçabilité, la qualité et le contrôle de volumétrie. [1][2][3]

### Données sources
Le fichier source est healthcare_dataset.csv et contient 15 colonnes: Name, Age, Gender, Blood Type, Medical Condition, Date of Admission, Doctor, Hospital, Insurance Provider, Billing Amount, Room Number, Admission Type, Discharge Date, Medication, Test Results. [1]

### Résultats attendus
- Un script unique ingestion/ingest.py proposant trois commandes: validate (pré‑ingestion), load (ingestion), postcheck (contrôles post‑ingestion) pour une exécution reproductible, observable et documentée. [1]
- Des rapports JSON: reports/pre_ingest.json et reports/post_ingest.json, confirmant structure, intégrité, volume et répartitions clés. [2][3]

### Structure du projet
- data/: healthcare_dataset.csv (source), rejects.jsonl (rejets éventuels créés lors du load). [1]
- ingestion/: ingest.py (CLI Typer avec validate, load, postcheck). [1]
- reports/: pre_ingest.json, post_ingest.json (générés par le script). [2][3]
- logs/: réservé pour des logs additionnels si nécessaire. [1]

### Prérequis
- macOS ou Linux avec Python 3.11+ et MongoDB en local, ou connexion vers une instance accessible avec un utilisateur readWrite sur la base healthcare. [1]
- Paquets Python: pandas, pymongo, python-dotenv, typer, pydantic (voir requirements.txt). [1]

### Installation
- Installer les dépendances: pip install -r requirements.txt. [1]
- Vérifier MongoDB en local (exemples mongosh): ping et création d’un utilisateur applicatif app_user sur la base healthcare si nécessaire. [1]
- Optionnel: définir MONGO_URI dans un fichier .env à la racine, par exemple mongodb://app_user:app_pass@localhost:27017/healthcare?authSource=healthcare. [1]

### Commandes
- Validation pré‑ingestion: python ingestion/ingest.py data/healthcare_dataset.csv --report reports/pre_ingest.json écrit un rapport avec compte de lignes, présence des colonnes, contrôles de types/dates/enums et estimation des doublons potentiels. [1][2]
- Chargement: python ingestion/ingest.py load data/healthcare_dataset.csv lit en chunks, normalise, caste (Date, Int, Decimal128) et insère en bulk dans healthcare.encounters, en journalisant les rejets éventuels dans data/rejects.jsonl; un résumé read/inserted/rejected s’affiche. [1]
- Post‑contrôle: python ingestion/ingest.py postcheck génère reports/post_ingest.json avec total_docs et un top des conditions médicales. [1][3]

### Exemple de résultats
- pre_ingest.json indique 55 500 lignes, aucune colonne manquante, 0 invalidité de type/date/enum et pas de valeurs manquantes détectées dans les colonnes suivies. [2]
- post_ingest.json indique total_docs = 55 500 et un top des conditions: Arthritis, Diabetes, Hypertension, Obesity, Cancer, Asthma avec des volumes élevés et cohérents. [3]

### Schéma cible (document MongoDB)
- patient: name (String), age (Int32), gender (String), blood_type (String). [1]
- visit: admission_date (Date), discharge_date (Date|null), admission_type (String), room_number (Int32). [1]
- medical: condition (String), medication (String), test_results (String). [1]
- admin: doctor (String), hospital (String), insurance_provider (String). [1]
- billing: amount (Decimal128). [1]
- src: file (String), ingested_at (Date). [1]

### Index recommandés
- Composé: { admin.hospital: 1, visit.admission_date: 1 } pour recherches temporelles par établissement. [1]
- Simples: medical.condition, admin.insurance_provider, billing.amount pour filtrages fréquents. [1]
- Texte optionnel: patient.name et admin.doctor pour recherche libre. [1]

### Exemples CRUD
- Create: insertion d’un document encounter conforme via PyMongo ou mongosh. [1]
- Read: find par medical.condition avec projection patient.name et tri par visit.admission_date. [1]
- Update: set medical.test_results pour un _id donné. [1]
- Delete: delete_one sur une clé naturelle dérivée (Name|Date of Admission|Hospital) si utilisée en amont. [1]

### Qualité et intégrité
- La commande validate exécute les contrôles de structure, types numériques, dates parseables, enums Gender/Blood Type et signale les lignes vides par colonne, ainsi que des doublons potentiels sur la clé Name|Date of Admission|Hospital. [2]
- La commande load écrit un fichier rejects.jsonl avec la ligne source et la raison du rejet si un enregistrement échoue la normalisation/typage/enum. [1]

### Dépannage
- Tirets et guillemets: veiller à saisir des “--” ASCII pour les options et des guillemets droits " dans mongosh; les caractères typographiques provoquent des erreurs de parsing. [1]
- Module bson: utiliser pymongo (qui fournit bson) et éviter d’installer un paquet tiers bson séparé; en cas de conflit, désinstaller le paquet bson non officiel. [1]
- Connexion: si MONGO_URI n’est pas défini, le script utilise par défaut mongodb://app_user:app_pass@localhost:27017/healthcare?authSource=healthcare. [1]

### Traçabilité
- pre_ingest.json inclut le hash SHA‑256 du CSV source pour vérifier l’immuabilité des données de référence et faciliter des relectures reproductibles. [2]
- post_ingest.json fournit des métriques rapides confirmant le volume inséré et la distribution des conditions. [3]

Souhaité pour l’étape suivante: générer rapidement les commandes Mongo d’indexation et, si besoin, ajouter un validateur JSON Schema à la collection encounters pour renforcer le typage à l’écriture. [1]

Sources
[1] healthcare_dataset.csv https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/53032499/26dacc55-a1e7-4b8c-8d7e-d590f3894bf0/healthcare_dataset.csv
[2] pre_ingest.json https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/53032499/2edec6cd-65bd-4372-9028-e632835967fc/pre_ingest.json
[3] post_ingest.json https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/53032499/289b1e8c-95f0-4ba6-a0fa-815cbb51eca0/post_ingest.json
