#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export JSONL (1 doc/ligne) d'une collection MongoDB via un "Read" applicatif.
- Robuste aux gros volumes (stream + batch_size)
- Session explicite + no_cursor_timeout pour éviter l'idle timeout des curseurs
- Paramétrage par variables d'environnement

Variables d'environnement:
  MONGO_URI   (obligatoire) ex: mongodb://user:pass@localhost:27017/admin?authSource=admin
  MONGO_DB    (défaut: healthcare)
  MONGO_COLL  (défaut: encounters)
  OUT_FILE    (défaut: ./exports/<db>_<coll>.jsonl)
  BATCH_SIZE  (défaut: 5000)
"""

import os
import sys
import json
from pathlib import Path
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError


def getenv_required(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise SystemExit(f"[export] Missing required env var: {key}")
    return val


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def export_jsonl(
    mongo_uri: str,
    db_name: str,
    coll_name: str,
    out_file: Path,
    batch_size: int = 5000,
) -> int:
    """
    Lit la collection Mongo et écrit chaque document en JSONL.
    Retourne le nombre de documents exportés.
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db[coll_name]

    ensure_parent_dir(out_file)

    n = 0
    # Session explicite pour éviter le session idle timeout
    with client.start_session() as session:
        # Ouvrir le fichier et consommer le curseur dans le même bloc
        with out_file.open("w", encoding="utf-8") as f:
            cursor = coll.find({}, no_cursor_timeout=True, session=session).batch_size(batch_size)
            try:
                for doc in cursor:
                    # Sérialiser ObjectId/Date en string
                    f.write(json.dumps(doc, default=str, ensure_ascii=False) + "\n")
                    n += 1
            finally:
                # Toujours fermer le curseur proprement
                cursor.close()

    return n


def main() -> None:
    mongo_uri = getenv_required("MONGO_URI")
    db_name = os.environ.get("MONGO_DB", "healthcare")
    coll_name = os.environ.get("MONGO_COLL", "encounters")
    default_out = f"./exports/{db_name}_{coll_name}.jsonl"
    out_file = Path(os.environ.get("OUT_FILE", default_out))
    batch_size = int(os.environ.get("BATCH_SIZE", "5000"))

    try:
        count = export_jsonl(
            mongo_uri=mongo_uri,
            db_name=db_name,
            coll_name=coll_name,
            out_file=out_file,
            batch_size=batch_size,
        )
        print(f"[export] {db_name}.{coll_name} -> {out_file} ({count} docs)")
    except PyMongoError as e:
        print(f"[export][error] Mongo error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except OSError as e:
        print(f"[export][error] FS error: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()