# P5 Healthcare ETL + Mongo Ingestion

Pipeline de préparation et d’ingestion de données healthcare vers MongoDB, packagé avec Docker Compose. Le pipeline nettoie un CSV de 50k lignes, prépare un fichier nettoyé, puis ingère en masse dans MongoDB. Une étape finale contrôle le volume inséré.

## Prérequis

- Docker et Docker Compose installés
- Port 27017 disponible en local
- Espace disque suffisant pour le volume Mongo

## Structure du projet

- docker-compose.yml
- run_pipeline.sh
- requirements.txt
- scripts/prepare_clean_data.py
- scripts/ingest.py
- data/healthcare_dataset.csv (source brute)
- data/healthcare_cleaned.csv (généré)
- docker/init/01-create-user.js (init user Mongo)
- .env (variables d’environnement)

## Variables d’environnement (.env)

Créer un fichier .env à la racine avec:

```bash
# Utilisateur root Mongo (créé au premier démarrage du volume)
MONGO_INITDB_ROOT_USERNAME=app_user
MONGO_INITDB_ROOT_PASSWORD=app_pass

# URI utilisée par l’ingester (utilisateur applicatif)
MONGO_URI=mongodb://app_user:app_pass@mongodb:27017/healthcare?authSource=admin
```

Notes:
- L’utilisateur applicatif app_user/app_pass sera créé automatiquement par le script d’init au premier démarrage d’un volume vierge.
- MONGO_URI doit contenir authSource=admin car l’utilisateur est créé dans la base admin avec rôle readWrite sur healthcare.

## Script d’initialisation Mongo (idempotent)

Placé dans docker/init/01-create-user.js, exécuté automatiquement uniquement lors de l’initialisation d’un volume vierge:

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

## Exécution du pipeline complet

Le pipeline fait:
1/7 Téléchargement / préparation des dossiers (optionnel selon script)
2/7 Préparation des données (clean CSV)
3/7 Validation du CSV
4/7 Construction image ingester
5/7 Vérifications pré-ingest (connexion Mongo)
6/7 Ingestion en bulk
7/7 Contrôle en base (count)
Fin du pipeline
- Export de la base de données 
- Effacement du fichier .env

Avant de lancer la commande Docker doit etre en cours d'éxécution sur la machine.

Commande:

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

