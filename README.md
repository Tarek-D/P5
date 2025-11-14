# P5 Healthcare ETL + Mongo Ingestion

Pipeline de préparation et d’ingestion de données healthcare vers MongoDB, packagé avec Docker Compose. Le pipeline nettoie un CSV de 50k lignes, prépare un fichier nettoyé, puis ingère en masse dans MongoDB. Une étape finale contrôle le volume inséré.

## Prérequis

- Docker et Docker Compose installés
- Port 27017 disponible en local
- Espace disque suffisant pour le volume Mongo
- L'ensemble du dossier du projet
- Ajouter le fichier .env à la racine du dossier 

## Structure du projet

- docker-compose.yml
- run_pipeline.sh
- requirements.txt
- scripts/
  - prepare_clean_data.py
  - ingest.py
  - export_read_jsonl.py
  - delete_all.py
  - verify_migration.py
- data/healthcare_dataset.csv (source brute)
- docker/
  - init/01-create-user.js (init user Mongo)
  - Dockerfile.ingester
- .env (variables d’environnement)

## Variables d’environnement (.env)

Créer un fichier .env à la racine avec:

```bash
# Utilisateur root Mongo (créé au premier démarrage du volume)
MONGO_INITDB_ROOT_USERNAME=app_user
MONGO_INITDB_ROOT_PASSWORD=app_pass

# URI utilisée par l’ingester (utilisateur applicatif)
MONGO_URI=mongodb://app_user:app_pass@mongodb:27017/healthcare?authSource=admin
MONGO_DB=healthcare
MONGO_INITDB_DATABASE=healthcare
```

Notes:
- L’utilisateur applicatif app_user/app_pass sera créé automatiquement par le script d’init au premier démarrage d’un volume vierge.
- MONGO_URI doit contenir authSource=admin car l’utilisateur est créé dans la base admin avec rôle readWrite sur healthcare.

## Script d’initialisation Mongo / utilisateur

Placé dans docker/init/01-create-user.js, exécuté automatiquement uniquement lors de l’initialisation d’un volume vierge et va créer l'utilisateur "root" qui a les privilèges ReadWrite. 
Il existe d'autres types de roles intégrés dans MongoDB et aussi la possibilité de créer des roles configurables.

```javascript
db = db.getSiblingDB('admin');

const user = process.env.MONGO_INITDB_ROOT_USERNAME || 'app_user';
const pwd  = process.env.MONGO_INITDB_ROOT_PASSWORD || 'app_pass';
const appDb = 'healthcare';

const exists = db.getUser(user);
if (!exists) {
  db.createUser({
    user: user,
    pwd:  pwd,
    roles: [{ role: 'readWrite', db: appDb }]
  });
} else {
  // Optionnel: mettre à jour mot de passe/roles si nécessaire
  // db.updateUser(user, { pwd: pwd, roles: [{ role: 'readWrite', db: appDb }] });
}

if (!db.getUser('admin')) {
  db.createUser({
    user: 'admin',
    pwd: 'password123',
    roles: [{ role: 'userAdmin', db: appDb }]
  });
}

if (!db.getUser('readerwritter')) {
  db.createUser({
    user: 'readerwritter',
    pwd: 'readerpass',
    roles: [{ role: 'readWrite', db: appDb }]
  });
}
```
## Réseau, volumes et services
•	Docker Compose crée un réseau isolé pour les services; les conteneurs y résolvent les hôtes par nom de service, donc l’URI de la base utilise mongodb comme host au lieu de localhost ou 127.0.0.1.

•	Le service mongodb expose son stockage via un volume dédié (nommé ou bind mount) pour la persistance; il n’est pas affecté par les opérations réalisées dans le conteneur ingester.

•	Le service ingester monte le répertoire du projet sur /app via un bind mount; cela rend tous les fichiers locaux visibles dans le conteneur, et toute suppression/écriture dans /app affecte aussi le dossier hôte.

## Format de la collection et documents dans la base de données 

![alt text](image.png)

``` json
encounters:
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
```
## Exécution du pipeline complet

### Démarrage des services
•	Démarre le service MongoDB en arrière‑plan pour que les scripts applicatifs puissent s’y connecter via le réseau Compose et l’URI attendu.

### Préparation des données

•	Exécute le script de nettoyage/préparation sur le dataset source (CSV) afin d’obtenir un fichier propre et typé, prêt à être ingéré, avec chemins et options pilotés par variables d’environnement.

### Ingestion dans MongoDB
•	Lance le chargeur ingester qui lit les données préparées et insère en lots dans MongoDB via MONGO_URI/MONGO_DB, avec logs de volumes et gestion d’erreurs pour des reprises propres.

### Vérification d’intégrité
•	Exécute la vérification post‑ingestion pour comparer volumes attendus/ingérés et valider quelques champs, en émettant un statut clair pour le pipeline.

### Export JSONL (CRUD Read)
•	Réalise un export applicatif de la collection cible vers un fichier JSONL daté, en lecture streaming pour supporter de gros volumes.

### Nettoyage final (.env)
•	Supprime le fichier .env du workspace du conteneur ingester de manière non interactive via Docker Compose, ce qui efface aussi le .env côté hôte si le projet est bind‑mounté sur /app.

![alt text](image-2.png)

# ⚠ 
## Avant de lancer la commande, Docker doit etre en cours d'éxécution sur la machine.
# ⚠ 

Commande à executer à la racine du dossier :

```bash
bash run_pipeline.sh
```

Exemples de sorties attendues:

- Préparation:
  - Génère data/healthcare_cleaned.csv
- Ingestion:
  - Affiche un récapitulatif
  - inserted: 50000
- Contrôle:
  - estimatedDocumentCount() ≈ 50000


## Dépannage rapide

- S'assurer d'avoir tous les fichiers du projet 
- D'avoir créer le fichier .env à la racine du dossier

## Sécurité et bonnes pratiques

- Éviter d’utiliser le compte root pour l’application; préférer app_user avec readWrite sur healthcare.

- Ne pas commiter .env en clair; utiliser des secrets en production.

- Si des mots de passes sont stockés plus tard dans la base de données utilisez des solutions de hash comme bcrypt ne jamais stocker de mot de passe en clair.

