#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CRUD: DELETE — Effacer tous les documents de toutes les collections d'une base MongoDB.

Comportement:
- Lit la config depuis les variables d'environnement (ex: injectées par Docker Compose via .env)
    - MONGO_URI : URI de connexion MongoDB
    - MONGO_DB  : Nom de la base de données
- Parcourt db.list_collection_names() et exécute delete_many({}) sur chaque collection.
- Conserve la base et les index (ne drop pas les collections).

Usage (exemples):
  docker compose run --rm ingester bash -lc 'python scripts/delete_all.py'
  # en supposant que MONGO_URI et MONGO_DB sont fournis au service via .env ou environment:
"""

import os
import sys
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def getenv_required(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise SystemExit(f"[delete] Missing required env var: {key}")
    return val


def main() -> None:
    mongo_uri = getenv_required("MONGO_URI")
    db_name   = getenv_required("MONGO_DB")

    client = MongoClient(mongo_uri)
    db = client[db_name]

    try:
        collections = db.list_collection_names()
    except PyMongoError as e:
        print(f"[delete][error] Unable to list collections for '{db_name}': {e}", file=sys.stderr)
        raise SystemExit(1)

    if not collections:
        print(f"[delete] Base '{db_name}': aucune collection, rien à supprimer.")
        return

    total_deleted = 0
    for coll_name in collections:
        coll = db[coll_name]
        try:
            res = coll.delete_many({})
            deleted = res.deleted_count
            total_deleted += deleted
            print(f"[delete] {db_name}.{coll_name}: supprimés={deleted}")
        except PyMongoError as e:
            print(f"[delete][error] {db_name}.{coll_name}: {e}", file=sys.stderr)

    print(f"[delete] Terminé. Base '{db_name}', total supprimés: {total_deleted} documents.")


if __name__ == "__main__":
    main()
