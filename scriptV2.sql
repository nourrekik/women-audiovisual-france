-- Nettoyage préalable
DROP TABLE IF EXISTS fact_parole_chaines CASCADE;
DROP TABLE IF EXISTS fact_genre_programme CASCADE;
DROP TABLE IF EXISTS fact_hourstat CASCADE;
DROP TABLE IF EXISTS dim_annee CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_media CASCADE;

-- Dimensions (INT PK, insert explicite)
CREATE TABLE dim_media (
    id_media INT PRIMARY KEY,
    media_type VARCHAR(10) NOT NULL CHECK (media_type IN ('tv', 'radio')),
    channel_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_genre (
    id_genre INT PRIMARY KEY,
    genre VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_annee (
    year INT PRIMARY KEY  -- UNIQUEMENT year (PK)
);

-- Facts (INT PK auto, FK)
CREATE TABLE fact_hourstat (
    id_fact SERIAL PRIMARY KEY,
    id_media INT NOT NULL REFERENCES dim_media(id_media),
    year INT NOT NULL REFERENCES dim_annee(year),  -- year direct
    hour INT,
    women_expression_rate DECIMAL,
    speech_rate DECIMAL,
    nb_hours_analyzed DECIMAL
);

CREATE TABLE fact_genre_programme (  -- 14 colonnes 2019/2020
    id_fact SERIAL PRIMARY KEY,
    id_genre INT NOT NULL REFERENCES dim_genre(id_genre),
    nb_declarations_2020 INT,
    total_declarations_duration_2020 BIGINT,
    women_speech_duration_2020 BIGINT,
    men_speech_duration_2020 BIGINT,
    other_duration_2020 BIGINT,
    women_expression_rate_2020 DECIMAL,
    speech_rate_2020 DECIMAL,
    nb_declarations_2019 INT,
    total_declarations_duration_2019 BIGINT,
    women_speech_duration_2019 BIGINT,
    men_speech_duration_2019 BIGINT,
    other_duration_2019 BIGINT,
    women_expression_rate_2019 DECIMAL,
    speech_rate_2019 DECIMAL
);

CREATE TABLE fact_parole_chaines (
    id_fact SERIAL PRIMARY KEY,
    id_media INT NOT NULL REFERENCES dim_media(id_media),
    nb_declarations_2020 INT,
    total_declarations_duration_2020 BIGINT,
    women_speech_duration_2020 BIGINT,
    men_speech_duration_2020 BIGINT,
    other_duration_2020 BIGINT,
    women_expression_rate_2020 DECIMAL,
    speech_rate_2020 DECIMAL,
    nb_declarations_2019 INT,
    total_declarations_duration_2019 BIGINT,
    women_speech_duration_2019 BIGINT,
    men_speech_duration_2019 BIGINT,
    other_duration_2019 BIGINT,
    women_expression_rate_2019 DECIMAL,
    speech_rate_2019 DECIMAL
);

-- Vérification
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'dim_%' OR table_name LIKE 'fact_%'
ORDER BY table_name;