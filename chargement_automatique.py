import pandas as pd
import os
from sqlalchemy import create_engine
import glob
from datetime import datetime

# ====== CONFIGURATION ======
# Choisis ton type de base de données en décommentant la ligne appropriée

# Pour SQLite (base de données locale - RECOMMANDÉ pour débuter)
engine = create_engine('sqlite:///projet_audiovisuel.db')

# Pour PostgreSQL (décommenter et configurer si nécessaire)
# engine = create_engine('postgresql://username:password@localhost:5432/database_name')

# Pour MySQL (décommenter et configurer si nécessaire)
# engine = create_engine('mysql+pymysql://username:password@localhost:3306/database_name')

# Dossier contenant tes fichiers CSV et Excel
DOSSIER_DONNEES = './data/'

# ====== FONCTIONS PRINCIPALES ======

def creer_dossier_data():
    """Crée le dossier data s'il n'existe pas"""
    if not os.path.exists(DOSSIER_DONNEES):
        os.makedirs(DOSSIER_DONNEES)
        print(f"✓ Dossier '{DOSSIER_DONNEES}' créé")
    else:
        print(f"✓ Dossier '{DOSSIER_DONNEES}' existe déjà")

def nettoyer_nom_colonne(nom):
    """Nettoie le nom d'une colonne pour SQL"""
    import re
    nom = str(nom).strip().lower()
    nom = nom.replace(' ', '_').replace('-', '_')
    nom = re.sub(r'[^\w\s]', '', nom)
    nom = re.sub(r'_+', '_', nom)
    return nom

def charger_csv_vers_db(fichier_path, nom_table):
    """Charge un fichier CSV dans la base de données"""
    try:
        # Essayer différents encodages
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None

        for encoding in encodings:
            try:
                df = pd.read_csv(fichier_path, encoding=encoding, sep=None, engine='python')
                break
            except:
                continue

        if df is None:
            raise Exception("Impossible de lire le fichier avec les encodages testés")

        # Nettoyer les noms de colonnes
        df.columns = [nettoyer_nom_colonne(col) for col in df.columns]

        # Supprimer les lignes complètement vides
        df = df.dropna(how='all')

        # Insérer dans la base de données
        df.to_sql(nom_table, engine, if_exists='replace', index=False)

        print(f"✓ CSV: {os.path.basename(fichier_path)} → table '{nom_table}' ({len(df)} lignes, {len(df.columns)} colonnes)")
        return True, len(df)
    except Exception as e:
        print(f"✗ Erreur CSV {os.path.basename(fichier_path)}: {str(e)[:100]}")
        return False, 0

def charger_excel_vers_db(fichier_path, nom_table):
    """Charge un fichier Excel dans la base de données"""
    try:
        # Lire toutes les feuilles du fichier Excel
        xls = pd.ExcelFile(fichier_path)

        if len(xls.sheet_names) == 1:
            # Une seule feuille: charger directement
            df = pd.read_excel(fichier_path, sheet_name=0)
            df.columns = [nettoyer_nom_colonne(col) for col in df.columns]
            df = df.dropna(how='all')
            df.to_sql(nom_table, engine, if_exists='replace', index=False)
            print(f"✓ Excel: {os.path.basename(fichier_path)} → table '{nom_table}' ({len(df)} lignes, {len(df.columns)} colonnes)")
            return True, len(df)
        else:
            # Plusieurs feuilles: créer une table par feuille
            total_lignes = 0
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(fichier_path, sheet_name=sheet_name)
                df.columns = [nettoyer_nom_colonne(col) for col in df.columns]
                df = df.dropna(how='all')

                # Nom de table avec le nom de la feuille
                table_name = f"{nom_table}_{nettoyer_nom_colonne(sheet_name)}"
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"  → Feuille '{sheet_name}' → table '{table_name}' ({len(df)} lignes)")
                total_lignes += len(df)

            print(f"✓ Excel: {os.path.basename(fichier_path)} → {len(xls.sheet_names)} tables ({total_lignes} lignes totales)")
            return True, total_lignes

    except Exception as e:
        print(f"✗ Erreur Excel {os.path.basename(fichier_path)}: {str(e)[:100]}")
        return False, 0

def automatiser_chargement():
    """Automatise le chargement de tous les fichiers CSV et Excel"""
    print("=" * 60)
    print("   CHARGEMENT AUTOMATISÉ DES DONNÉES - PROJET BIG DATA & IA")
    print("=" * 60)
    print(f"Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dossier source: {DOSSIER_DONNEES}
")

    # Créer le dossier si nécessaire
    creer_dossier_data()

    # Compter les fichiers traités
    total_fichiers = 0
    fichiers_reussis = 0
    total_lignes = 0

    # Liste tous les fichiers
    fichiers_csv = glob.glob(os.path.join(DOSSIER_DONNEES, '*.csv'))
    fichiers_excel = glob.glob(os.path.join(DOSSIER_DONNEES, '*.xlsx')) + glob.glob(os.path.join(DOSSIER_DONNEES, '*.xls'))

    print(f"Fichiers trouvés: {len(fichiers_csv)} CSV, {len(fichiers_excel)} Excel
")

    if len(fichiers_csv) == 0 and len(fichiers_excel) == 0:
        print("⚠ Aucun fichier trouvé!")
        print(f"  Place tes fichiers CSV et Excel dans le dossier '{DOSSIER_DONNEES}'")
        return

    print("-" * 60)
    print("TRAITEMENT DES FICHIERS CSV")
    print("-" * 60)

    # Charger tous les CSV
    for fichier in fichiers_csv:
        nom_fichier = os.path.basename(fichier).replace('.csv', '')
        nom_table = nettoyer_nom_colonne(nom_fichier)

        success, nb_lignes = charger_csv_vers_db(fichier, nom_table)
        if success:
            fichiers_reussis += 1
            total_lignes += nb_lignes
        total_fichiers += 1

    print("
" + "-" * 60)
    print("TRAITEMENT DES FICHIERS EXCEL")
    print("-" * 60)

    # Charger tous les Excel
    for fichier in fichiers_excel:
        nom_fichier = os.path.basename(fichier).replace('.xlsx', '').replace('.xls', '')
        nom_table = nettoyer_nom_colonne(nom_fichier)

        success, nb_lignes = charger_excel_vers_db(fichier, nom_table)
        if success:
            fichiers_reussis += 1
            total_lignes += nb_lignes
        total_fichiers += 1

    print("
" + "=" * 60)
    print(f"RÉSUMÉ: {fichiers_reussis}/{total_fichiers} fichiers chargés avec succès")
    print(f"Total: {total_lignes} lignes importées")
    print(f"Heure de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

def lister_tables():
    """Affiche toutes les tables dans la base de données"""
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    print("
" + "=" * 60)
    print("   TABLES CRÉÉES DANS LA BASE DE DONNÉES")
    print("=" * 60)

    if not tables:
        print("Aucune table trouvée")
        return

    for i, table in enumerate(tables, 1):
        # Compter le nombre de lignes
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            nb_lignes = result.scalar()

        # Obtenir les colonnes
        columns = inspector.get_columns(table)
        nb_colonnes = len(columns)

        print(f"
{i}. Table: {table}")
        print(f"   - {nb_lignes} lignes, {nb_colonnes} colonnes")
        print(f"   - Colonnes: {', '.join([col['name'] for col in columns[:5]])}" + 
              ("..." if nb_colonnes > 5 else ""))

    print("
" + "=" * 60)

def exporter_exemple_requete():
    """Crée un fichier exemple avec des requêtes SQL"""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        return

    requetes = f"""# EXEMPLES DE REQUÊTES SQL POUR TON PROJET
# Fichier généré automatiquement le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

import pandas as pd
from sqlalchemy import create_engine

# Connexion à la base de données
engine = create_engine('sqlite:///projet_audiovisuel.db')

# Tables disponibles: {', '.join(tables)}

# Exemple 1: Afficher les premières lignes d'une table
df = pd.read_sql("SELECT * FROM {tables[0]} LIMIT 10", engine)
print(df)

# Exemple 2: Compter le nombre de lignes
df = pd.read_sql("SELECT COUNT(*) as total FROM {tables[0]}", engine)
print(df)

# Exemple 3: Exporter le résultat vers CSV
df = pd.read_sql("SELECT * FROM {tables[0]}", engine)
df.to_csv('resultat_export.csv', index=False)
print("✓ Export réussi vers resultat_export.csv")
"""

    with open('exemples_requetes.py', 'w', encoding='utf-8') as f:
        f.write(requetes)

    print(f"
✓ Fichier 'exemples_requetes.py' créé avec des exemples de requêtes")

# ====== EXÉCUTION PRINCIPALE ======
if __name__ == "__main__":
    try:
        automatiser_chargement()
        lister_tables()
        exporter_exemple_requete()

        print("
✓ Script terminé avec succès!")
        print("  Tu peux maintenant utiliser ta base de données pour ton projet")

    except Exception as e:
        print(f"
✗ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
