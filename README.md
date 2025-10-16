Voici un README complet, prêt à l’emploi, qui reflète l’architecture actuelle, les commandes, et les correctifs d’auth Mongo intégrés.

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
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=change_me

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
```

Important:
- Les scripts d’init ne se rejouent que si le volume de données Mongo est vierge (après un down -v par exemple).

## Démarrage des services

Démarrer Mongo seul (recommandé pour initialiser l’utilisateur applicatif):

```bash
docker compose up -d mongodb
```

Attendre quelques secondes que l’initialisation s’exécute. Pour repartir de zéro (réinitialisation complète):

```bash
docker compose down -v
docker compose up -d mongodb
```

## Exécution du pipeline complet

Le pipeline fait:
1/7 Téléchargement / préparation des dossiers (optionnel selon script)
2/7 Préparation des données (clean CSV)
3/7 Validation du CSV
4/7 Construction image ingester
5/7 Vérifications pré-ingest (connexion Mongo)
6/7 Ingestion en bulk
7/7 Contrôle en base (count)

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

## Étapes manuelles (si besoin)

Nettoyage:

```bash
docker compose run --rm ingester python scripts/prepare_clean_data.py
```

Ingestion:

```bash
docker compose run --rm ingester python scripts/ingest.py load
```

Test connexion Mongo depuis ingester:

```bash
docker compose run --rm ingester python -c "from pymongo import MongoClient; import os; print('URI=', os.getenv('MONGO_URI')); c=MongoClient(os.getenv('MONGO_URI')); print(c.list_database_names())"
```

Test contrôle en base (mongosh dans le conteneur Mongo):

```bash
docker exec -i mongodb mongosh "mongodb://app_user:app_pass@mongodb:27017/healthcare?authSource=admin" --eval 'db.encounters.estimatedDocumentCount()'
```

Astuce: pour ne pas exposer l’URI en clair dans la commande, sourcer .env avant:

```bash
set -a
[ -f .env ] && . ./.env
set +a
docker exec -i mongodb sh -lc 'mongosh "$MONGO_URI" --eval "db.getSiblingDB(\"healthcare\").encounters.estimatedDocumentCount()"'
```

## Points d’attention

- Auth Mongo:
  - L’ingestion réussit uniquement si l’utilisateur applicatif existe et si MONGO_URI contient authSource=admin.
  - En cas d’erreur “Authentication failed, code 18”, vérifier:
    - Existence de app_user dans admin (mongosh: use admin; db.getUsers())
    - MONGO_URI dans env du conteneur ingester (docker compose run --rm ingester env | grep MONGO_URI)
- REPL Python qui s’ouvre à la place du script:
  - Éviter un entrypoint ["bash","-lc"] sur le service ingester (retirer cette ligne si présente).
  - Lancer avec: docker compose run --rm ingester python scripts/ingest.py load
  - Si besoin de passer “--” pour transmettre les args: docker compose run --rm ingester -- python scripts/ingest.py load
- Commande count dépréciée:
  - Utiliser countDocuments({}) ou estimatedDocumentCount() plutôt que count().
- Rejeu automatique de la création d’utilisateur:
  - Les scripts d’init ne rejouent que sur volume vierge. Si tu veux forcer la création à chaque run, ajoute une étape de vérification dans run_pipeline.sh qui appelle mongosh et crée l’utilisateur s’il n’existe pas.

## Exemples de snippets utiles

Contrôle final dans run_pipeline.sh (recommandé):

```bash
echo "[7/7] Contrôle en base"
docker exec -i mongodb mongosh "mongodb://app_user:app_pass@mongodb:27017/healthcare?authSource=admin" --eval 'db.encounters.estimatedDocumentCount()'
```

Ou, en sourçant .env pour ne pas exposer l’URI:

```bash
set -a
[ -f .env ] && . ./.env
set +a
echo "[7/7] Contrôle en base"
docker exec -i mongodb sh -lc 'mongosh "$MONGO_URI" --eval "db.getSiblingDB(\"healthcare\").encounters.estimatedDocumentCount()"'
```

## Dépannage rapide

- “Authentication failed.”:
  - Vérifier db.getUsers() dans admin, présence de app_user, et authSource=admin dans l’URI.
- “command count requires authentication”:
  - La vérification finale n’utilise pas l’URI authentifiée. Utiliser mongosh avec MONGO_URI ou avec app_user/app_pass.
- “mongosh: command not found”:
  - Lancer mongosh dans le conteneur mongodb (docker exec -i mongodb mongosh ...), ou installer mongosh dans l’image où tu l’appelles.
- REPL Python vs exécution script:
  - Retirer entrypoint bash du service ingester et passer l’argument load correctement.

## Sécurité et bonnes pratiques

- Éviter d’utiliser le compte root pour l’application; préférer app_user avec readWrite sur healthcare.
- Ne pas commiter .env en clair; utiliser des secrets ou variables CI/CD en production.
- Documenter la procédure de reset dev (down -v) pour rejouer l’initialisation de Mongo.

## Licence

Projet à usage pédagogique. Adapter les licences des dépendances selon vos contraintes.

