import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# À adapter avec tes paramètres
USER = "postgres"
PASSWORD = "1108"
HOST = "localhost"
PORT = "5432"
DB = "temps_parole_femme"

engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

url_hourstatall = "https://static.data.gouv.fr/resources/temps-de-parole-des-hommes-et-des-femmes-a-la-television-et-a-la-radio/20190312-200022/20190308-hourstatall.csv"
url_tv_years = "https://static.data.gouv.fr/resources/temps-de-parole-des-hommes-et-des-femmes-a-la-television-et-a-la-radio/20190312-195651/20190308-tv-years.csv"
url_femme_genre_programme = "https://static.data.gouv.fr/resources/temps-de-parole-des-femmes-et-des-hommes-dans-les-programmes-ayant-fait-lobjet-dune-declaration-au-csa-pour-son-rapport-la-representation-des-femmes-a-la-television-et-la-radio/20210303-225535/ina-csa-parole-femmes-genreprogramme.csv"
url_parole_femme = "https://static.data.gouv.fr/resources/temps-de-parole-des-femmes-et-des-hommes-dans-les-programmes-ayant-fait-lobjet-dune-declaration-au-csa-pour-son-rapport-la-representation-des-femmes-a-la-television-et-la-radio/20210303-214530/ina-csa-parole-femmes-chaines.csv"


df_hourstatall = pd.read_csv(url_hourstatall,sep=",")
df_tv_years = pd.read_csv(url_tv_years,sep=",")
df_femme_genre_programme = pd.read_csv(url_femme_genre_programme,sep=",")
df_parole_femme = pd.read_csv(url_parole_femme,sep=",")

mapping_media = {
    "Fip": "france inter",
    "FunRadio": "Fun Radio",
    "BFMTV": "BFM TV",
    "C8": "D8/C8",
    "CANAL PLUS": "Canal+",
    "CNEWS": "I-Télé/CNews",
    "L'Equipe": "L'Equipe 21",
    "TMC": "Monte Carlo TMC",
    "franceinfo": "France Info",
    "France Ô": "France O"
}

df_parole_femme["Editeur"] = df_parole_femme["Editeur"].replace(mapping_media)

# récupération des chaines de la table hourstatall 
df_media = pd.DataFrame(df_hourstatall[['media_type', 'channel_name']].drop_duplicates())

# récupération des autres chaines de la table parole_femme 
df_new_media = df_parole_femme[['media', 'Editeur']].drop_duplicates()

#suppression des indexs dans les deux tables
df_new_media = df_new_media.reset_index(drop=True)
df_media = df_media.reset_index(drop=True)

#liste des nouvelles chaines 
nouvelles_chaines = []

#suppresion des lignes en double (on verifie si la nouvelle chaine est présente dans la table des anciennes chaines) 
for i in range(len(df_new_media)):
    trouvee = False
    for j in range(len(df_media)):
        if df_media.loc[j, "channel_name"].lower() == df_new_media.loc[i, "Editeur"].lower():
            trouvee = True
            break
    if not trouvee:
        nouvelles_chaines.append({
            'media_type': df_new_media.loc[i, 'media'],  # 'radio' ou 'tv'
            'channel_name': df_new_media.loc[i, 'Editeur']
        })
        
if nouvelles_chaines:
    df_nouvelles = pd.DataFrame(nouvelles_chaines)
    df_media = pd.concat([df_media, df_nouvelles], ignore_index=True)
    print(f"Ajouté {len(nouvelles_chaines)} nouvelles chaînes")
    print(df_nouvelles)
else:
    print("Aucune nouvelle chaîne à ajouter")


#table genre
df_genre = pd.DataFrame(df_femme_genre_programme['genre'].drop_duplicates().reset_index(drop=True))

#table année
df_annee = pd.DataFrame(df_hourstatall['year'].drop_duplicates().reset_index(drop=True))

# Normalisation pour mapping insensible à la casse (AVANT création des dicts)
df_media['channel_name_norm'] = df_media['channel_name'].str.upper()
df_hourstatall['channel_name_norm'] = df_hourstatall['channel_name'].str.upper()
df_parole_femme['Editeur_norm'] = df_parole_femme['Editeur'].str.upper()
df_femme_genre_programme['genre_norm'] = df_femme_genre_programme['genre'].str.upper()  # Si besoin pour genre

# Ajout d'index pour df_media (commençant à 1)
df_media = df_media.reset_index(drop=True)
df_media['id_media'] = df_media.index + 1

# Ajout d'index pour df_genre (commençant à 1)
df_genre = df_genre.reset_index(drop=True)
df_genre['id_genre'] = df_genre.index + 1

# 1. Créer les dictionnaires de mapping SUR LES NORM (majuscules)
media_dict_norm = dict(zip(df_media['channel_name_norm'], df_media['id_media']))
genre_dict_norm = dict(zip(df_genre['genre'].str.upper(), df_genre['id_genre']))  # Si genre a des casse vars

# 2. Remplacer dans df_hourstatall (channel_name → id_media via norm)
df_hourstatall['id_media'] = df_hourstatall['channel_name_norm'].map(media_dict_norm)
df_hourstatall = df_hourstatall.drop(['channel_name', 'channel_name_norm', 'media_type'], axis=1)

# 3. Remplacer dans df_femme_genre_programme (genre → id_genre via norm)
df_femme_genre_programme['id_genre'] = df_femme_genre_programme['genre_norm'].map(genre_dict_norm)
df_femme_genre_programme = df_femme_genre_programme.drop(['genre', 'genre_norm'], axis=1)

# 4. Remplacer dans df_parole_femme (Editeur → id_media via norm)
df_parole_femme['id_media'] = df_parole_femme['Editeur_norm'].map(media_dict_norm)
df_parole_femme = df_parole_femme.drop(['Editeur', 'Editeur_norm', 'media'], axis=1)


df_media[['id_media', 'media_type', 'channel_name']].to_sql('dim_media', engine, if_exists='append', index=False)
df_genre[['id_genre', 'genre']].to_sql('dim_genre', engine, if_exists='append', index=False)
df_annee['year'].drop_duplicates().to_sql('dim_annee', engine, if_exists='append', index=False)

df_hourstatall[['id_media', 'year', 'hour', 'women_expression_rate', 'speech_rate', 'nb_hours_analyzed']].to_sql('fact_hourstat', engine, if_exists='append', index=False)
df_femme_genre_programme[['id_genre', 'nb_declarations_2020', 'total_declarations_duration_2020', 'women_speech_duration_2020', 'men_speech_duration_2020', 'other_duration_2020', 'women_expression_rate_2020', 'speech_rate_2020', 'nb_declarations_2019', 'total_declarations_duration_2019', 'women_speech_duration_2019', 'men_speech_duration_2019', 'other_duration_2019','women_expression_rate_2019', 'speech_rate_2019']].to_sql('fact_genre_programme', engine, if_exists='append', index=False)
df_parole_femme[['id_media','nb_declarations_2020', 'total_declarations_duration_2020', 'women_speech_duration_2020', 'men_speech_duration_2020', 'other_duration_2020', 'women_expression_rate_2020', 'speech_rate_2020', 'nb_declarations_2019', 'total_declarations_duration_2019', 'women_speech_duration_2019', 'men_speech_duration_2019', 'other_duration_2019', 'women_expression_rate_2019', 'speech_rate_2019']].to_sql('fact_parole_chaines', engine, if_exists='append', index=False)