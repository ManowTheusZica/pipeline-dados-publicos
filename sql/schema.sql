-- Schema do pipeline: raw -> staging -> marts
-- Regra: staging e' append-only com chave natural; marts sao recriados
-- a cada execucao (baratos e sempre consistentes com o staging).

CREATE TABLE IF NOT EXISTS execucao (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    iniciada_em     TEXT NOT NULL,
    finalizada_em   TEXT,
    status          TEXT NOT NULL DEFAULT 'rodando',   -- rodando | ok | erro
    series_ok       INTEGER NOT NULL DEFAULT 0,
    series_erro     INTEGER NOT NULL DEFAULT 0,
    detalhe         TEXT
);

CREATE TABLE IF NOT EXISTS carga (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execucao_id  INTEGER REFERENCES execucao(id),
    serie        TEXT NOT NULL,
    fonte        TEXT NOT NULL,
    arquivo      TEXT NOT NULL,
    hash         TEXT NOT NULL,
    linhas_lidas INTEGER NOT NULL,
    linhas_novas INTEGER NOT NULL,
    linhas_erro  INTEGER NOT NULL DEFAULT 0,
    carregada_em TEXT NOT NULL,
    UNIQUE (serie, hash)
);

CREATE TABLE IF NOT EXISTS fato_indicador (
    serie       TEXT NOT NULL,
    data        TEXT NOT NULL,          -- ISO YYYY-MM-DD (mensal: dia 01)
    valor       REAL NOT NULL,
    carga_id    INTEGER REFERENCES carga(id),
    PRIMARY KEY (serie, data)
);

CREATE INDEX IF NOT EXISTS ix_fato_data ON fato_indicador (data);

-- Mart: variacao mensal e anual por serie, pronta para dashboard/API.
CREATE VIEW IF NOT EXISTS mart_variacao AS
SELECT
    serie,
    data,
    valor,
    LAG(valor) OVER (PARTITION BY serie ORDER BY data)                             AS valor_anterior,
    ROUND(valor - LAG(valor) OVER (PARTITION BY serie ORDER BY data), 4)           AS variacao_abs,
    ROUND(
        100.0 * (valor - LAG(valor) OVER (PARTITION BY serie ORDER BY data))
        / NULLIF(LAG(valor) OVER (PARTITION BY serie ORDER BY data), 0), 4)        AS variacao_pct,
    ROUND(AVG(valor) OVER (PARTITION BY serie ORDER BY data ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 4) AS media_movel_12
FROM fato_indicador;

-- Mart: ultimo valor e media de cada serie (o que o painel mostra no topo).
CREATE VIEW IF NOT EXISTS mart_ultimo AS
SELECT
    serie,
    MAX(data)                                            AS data_referencia,
    ROUND(AVG(valor), 4)                                 AS media_historica,
    COUNT(*)                                             AS observacoes
FROM fato_indicador
GROUP BY serie;
