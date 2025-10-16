import os
import typer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def main():
    """
    Vérifie la connexion, la base active, les collections et les volumes clés.
    Utilise MONGO_URI (ex: mongodb://app_user:app_pass@mongodb:27017/healthcare?authSource=admin).
    """
    uri = os.environ.get("MONGO_URI")
    if not uri:
        typer.echo("Erreur: MONGO_URI introuvable dans l'environnement.")
        raise typer.Exit(code=1)

    typer.echo("[7/7] Contrôle en base via script Python")
    client = None
    try:
        client = MongoClient(uri)
        client.admin.command("ping")
        db = client.get_database()
        typer.echo(f"Connexion: {uri}")
        typer.echo(f"Base: {db.name}")

        cols = db.list_collection_names()
        typer.echo(f"Collections: {cols}")

        # Vérifs usuelles
        if "encounters" in cols:
            est = db["encounters"].estimated_document_count()
            cnt = db["encounters"].count_documents({})
            typer.echo(f"encounters.estimated_document_count(): {est}")
            typer.echo(f"encounters.count_documents({{}}): {cnt}")
        else:
            typer.echo("⚠️ Collection 'encounters' absente")

        typer.echo("Contrôle terminé.")
    except PyMongoError as e:
        typer.echo(f"Erreur de contrôle Mongo: {e}")
        raise typer.Exit(code=2)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()