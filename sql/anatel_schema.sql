-- =============================================================================
-- SCHEMA: Projeto Anatel — Telefonia Móvel & Banda Larga Fixa (2020–2025)
-- Banco: Supabase (PostgreSQL)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS anatel;

-- -----------------------------------------------------------------------------
-- DIM 1: Regiões
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.dim_regioes (
    id_regiao   SMALLINT    PRIMARY KEY,
    nome_regiao VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO anatel.dim_regioes VALUES
    (1,'Norte'),(2,'Nordeste'),(3,'Centro-Oeste'),(4,'Sudeste'),(5,'Sul');

-- -----------------------------------------------------------------------------
-- DIM 2: Estados
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.dim_estados (
    id_estado   SMALLINT    PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sigla_uf    CHAR(2)     NOT NULL UNIQUE,
    nome_estado VARCHAR(50) NOT NULL,
    id_regiao   SMALLINT    NOT NULL REFERENCES anatel.dim_regioes(id_regiao)
);

INSERT INTO anatel.dim_estados (sigla_uf, nome_estado, id_regiao) VALUES
    ('AC','Acre',1),('AM','Amazonas',1),('AP','Amapá',1),('PA','Pará',1),
    ('RO','Rondônia',1),('RR','Roraima',1),('TO','Tocantins',1),
    ('AL','Alagoas',2),('BA','Bahia',2),('CE','Ceará',2),('MA','Maranhão',2),
    ('PB','Paraíba',2),('PE','Pernambuco',2),('PI','Piauí',2),
    ('RN','Rio Grande do Norte',2),('SE','Sergipe',2),
    ('DF','Distrito Federal',3),('GO','Goiás',3),
    ('MS','Mato Grosso do Sul',3),('MT','Mato Grosso',3),
    ('ES','Espírito Santo',4),('MG','Minas Gerais',4),
    ('RJ','Rio de Janeiro',4),('SP','São Paulo',4),
    ('PR','Paraná',5),('RS','Rio Grande do Sul',5),('SC','Santa Catarina',5);

-- -----------------------------------------------------------------------------
-- DIM 3: Municípios
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.dim_municipios (
    cod_ibge        INTEGER      PRIMARY KEY,
    nome_municipio  VARCHAR(100) NOT NULL,
    sigla_uf        CHAR(2)      NOT NULL REFERENCES anatel.dim_estados(sigla_uf),
    capital         BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_municipios_uf ON anatel.dim_municipios(sigla_uf);

-- -----------------------------------------------------------------------------
-- DIM 4: Operadoras
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.dim_operadoras (
    id_operadora    SMALLINT     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nome_operadora  VARCHAR(100) NOT NULL,
    grupo_economico VARCHAR(100),
    tipo_operadora  VARCHAR(10)  CHECK (tipo_operadora IN ('MNO','MVNO','ISP','Outra')),
    ativa           BOOLEAN      NOT NULL DEFAULT TRUE,
    UNIQUE(nome_operadora)
);

INSERT INTO anatel.dim_operadoras (nome_operadora, grupo_economico, tipo_operadora) VALUES
    ('Claro',  'América Móvil',  'MNO'),
    ('Vivo',   'Telefónica',     'MNO'),
    ('TIM',    'Telecom Italia', 'MNO'),
    ('Oi',     'Oi S.A.',        'MNO'),
    ('SKY',    'AT&T / DirecTV', 'MNO'),
    ('Outras', NULL,             'Outra');

-- -----------------------------------------------------------------------------
-- FATO 1: Telefonia Móvel (SMP)
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.fato_movel (
    id              BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ano             SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL CHECK (mes BETWEEN 1 AND 12),
    data_referencia DATE        NOT NULL,
    id_operadora    SMALLINT    NOT NULL REFERENCES anatel.dim_operadoras(id_operadora),
    cod_ibge        INTEGER     NOT NULL REFERENCES anatel.dim_municipios(cod_ibge),
    acessos_total   INTEGER,
    acessos_prepago INTEGER,
    acessos_pospago INTEGER,
    acessos_2g      INTEGER,
    acessos_3g      INTEGER,
    acessos_4g      INTEGER,
    acessos_5g      INTEGER,
    inserido_em     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fonte_arquivo   TEXT,
    UNIQUE(data_referencia, id_operadora, cod_ibge)
);

CREATE INDEX idx_movel_data      ON anatel.fato_movel(data_referencia);
CREATE INDEX idx_movel_operadora ON anatel.fato_movel(id_operadora);
CREATE INDEX idx_movel_ibge      ON anatel.fato_movel(cod_ibge);

-- -----------------------------------------------------------------------------
-- FATO 2: Banda Larga Fixa (SCM)
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.fato_banda_larga (
    id                   BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ano                  SMALLINT    NOT NULL,
    mes                  SMALLINT    NOT NULL CHECK (mes BETWEEN 1 AND 12),
    data_referencia      DATE        NOT NULL,
    id_operadora         SMALLINT    NOT NULL REFERENCES anatel.dim_operadoras(id_operadora),
    cod_ibge             INTEGER     NOT NULL REFERENCES anatel.dim_municipios(cod_ibge),
    acessos_total        INTEGER,
    velocidade_contratada VARCHAR(20),
    tecnologia           VARCHAR(30),
    acessos_fibra        INTEGER,
    acessos_cabo         INTEGER,
    acessos_xdsl         INTEGER,
    acessos_radio        INTEGER,
    acessos_satelite     INTEGER,
    inserido_em          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fonte_arquivo        TEXT,
    UNIQUE(data_referencia, id_operadora, cod_ibge, velocidade_contratada, tecnologia)
);

CREATE INDEX idx_bl_data      ON anatel.fato_banda_larga(data_referencia);
CREATE INDEX idx_bl_operadora ON anatel.fato_banda_larga(id_operadora);
CREATE INDEX idx_bl_ibge      ON anatel.fato_banda_larga(cod_ibge);

-- -----------------------------------------------------------------------------
-- VIEWS ANALÍTICAS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW anatel.vw_market_share_movel AS
SELECT
    f.data_referencia, f.ano, f.mes,
    e.sigla_uf, e.nome_estado, r.nome_regiao,
    o.nome_operadora, o.grupo_economico,
    SUM(f.acessos_total) AS acessos,
    SUM(SUM(f.acessos_total)) OVER (PARTITION BY f.data_referencia, e.sigla_uf) AS total_uf,
    ROUND(SUM(f.acessos_total) * 100.0 /
        NULLIF(SUM(SUM(f.acessos_total)) OVER (PARTITION BY f.data_referencia, e.sigla_uf), 0), 2
    ) AS market_share_pct
FROM anatel.fato_movel f
JOIN anatel.dim_municipios m  ON f.cod_ibge     = m.cod_ibge
JOIN anatel.dim_estados e     ON m.sigla_uf     = e.sigla_uf
JOIN anatel.dim_regioes r     ON e.id_regiao    = r.id_regiao
JOIN anatel.dim_operadoras o  ON f.id_operadora = o.id_operadora
GROUP BY f.data_referencia, f.ano, f.mes,
         e.sigla_uf, e.nome_estado, r.nome_regiao,
         o.nome_operadora, o.grupo_economico;

CREATE OR REPLACE VIEW anatel.vw_banda_larga_tecnologia AS
SELECT
    f.data_referencia, f.ano, f.mes,
    r.nome_regiao, e.sigla_uf, f.tecnologia,
    SUM(f.acessos_total)    AS acessos,
    SUM(f.acessos_fibra)    AS acessos_fibra,
    SUM(f.acessos_cabo)     AS acessos_cabo,
    SUM(f.acessos_xdsl)     AS acessos_xdsl,
    SUM(f.acessos_radio)    AS acessos_radio,
    SUM(f.acessos_satelite) AS acessos_satelite
FROM anatel.fato_banda_larga f
JOIN anatel.dim_municipios m ON f.cod_ibge     = m.cod_ibge
JOIN anatel.dim_estados e    ON m.sigla_uf     = e.sigla_uf
JOIN anatel.dim_regioes r    ON e.id_regiao    = r.id_regiao
GROUP BY f.data_referencia, f.ano, f.mes, r.nome_regiao, e.sigla_uf, f.tecnologia;

CREATE OR REPLACE VIEW anatel.vw_penetracao_4g5g_municipio AS
SELECT
    m.cod_ibge, m.nome_municipio, m.sigla_uf,
    e.nome_estado, r.nome_regiao, f.data_referencia,
    SUM(f.acessos_total) AS acessos_total,
    SUM(f.acessos_4g)    AS acessos_4g,
    SUM(f.acessos_5g)    AS acessos_5g,
    ROUND((SUM(f.acessos_4g) + SUM(f.acessos_5g)) * 100.0 /
        NULLIF(SUM(f.acessos_total), 0), 2) AS pct_4g5g
FROM anatel.fato_movel f
JOIN anatel.dim_municipios m ON f.cod_ibge  = m.cod_ibge
JOIN anatel.dim_estados e    ON m.sigla_uf  = e.sigla_uf
JOIN anatel.dim_regioes r    ON e.id_regiao = r.id_regiao
WHERE f.data_referencia = (SELECT MAX(data_referencia) FROM anatel.fato_movel)
GROUP BY m.cod_ibge, m.nome_municipio, m.sigla_uf,
         e.nome_estado, r.nome_regiao, f.data_referencia;

-- -----------------------------------------------------------------------------
-- ETL LOG
-- -----------------------------------------------------------------------------
CREATE TABLE anatel.etl_log (
    id               BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tabela_destino   VARCHAR(50) NOT NULL,
    arquivo_origem   TEXT        NOT NULL,
    data_referencia  DATE,
    status           VARCHAR(10) NOT NULL CHECK (status IN ('sucesso','erro','parcial')),
    linhas_inseridas INTEGER,
    linhas_erro      INTEGER,
    mensagem_erro    TEXT,
    executado_em     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
