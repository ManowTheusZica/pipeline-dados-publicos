# Pipeline de indicadores públicos (BCB/SGS)

![CI](https://github.com/ManowTheusZica/pipeline-dados-publicos/actions/workflows/ci.yml/badge.svg)

Pipeline de dados em 3 camadas que baixa séries do Banco Central (Selic, IPCA,
dólar, crédito PJ), carrega de forma **idempotente** em SQLite e valida a
qualidade antes de publicar. Sem dependência externa: `urllib` + `sqlite3` da
biblioteca padrão — roda em CI sem subir cluster.

```
raw (JSON cru + hash)  →  staging (fato_indicador)  →  marts (views SQL)
                                    ↓
                          testes de qualidade (bloqueiam o job)
```

## Resultado medido (execução real, modo fixture)

```
pipeline: 4 serie(s) | 01/01/2015 a 31/12/2026 | fonte: fixture
  carga selic_diaria      142 lidas |  142 novas | 0 invalidas
  carga ipca_mensal        48 lidas |   48 novas | 0 invalidas
  carga dolar_compra      142 lidas |  142 novas | 0 invalidas
  carga credito_pj         48 lidas |   48 novas | 0 invalidas
  [OK  ] sem_nulo, sem_duplicidade, datas_validas, volume_minimo
  [OK  ] faixa_plausivel  todas dentro da faixa
  [OK  ] buraco_de_datas  maior intervalo 31 dias (credito_pj)
  [OK  ] variacao_absurda nenhuma variacao acima de 500%
execucao 1: OK
```

Segunda execução com os mesmos arquivos: **0 linhas novas** (idempotência por
hash do arquivo + `PRIMARY KEY (serie, data)`).

## Rodando

```bash
python -m venv .venv && source .venv/bin/activate   # sem dependencias
python -m pipeline run --fixture        # offline (dados de data/fixtures)
python -m pipeline run                  # de verdade, na API do BCB
python -m pipeline relatorio
python -m pipeline status
```

Saída do relatório:

```
serie            referencia        valor        media        obs     var%
credito_pj       2023-12-01   3337274.83 3324039.5225         48  -0.2398
dolar_compra     2026-07-17       5.4727       5.3256        142   0.3834
ipca_mensal      2023-12-01         0.15       0.1377         48    -40.0
selic_diaria     2026-07-17       0.0498       0.0488        142  -1.9685
```

## Testes de qualidade (bloqueiam o pipeline)

| teste | pergunta que responde | crítico |
|---|---|---|
| `sem_nulo` | veio linha sem data/valor/serie? | sim |
| `sem_duplicidade` | a mesma serie/data entrou duas vezes? | sim |
| `datas_validas` | alguma data fora de ISO? | sim |
| `volume_minimo` | a serie veio vazia/truncada? | sim |
| `faixa_plausivel` | Selic de 300%? crédito de R$ 1? | sim |
| `buraco_de_datas` | sumiu um mês no meio? | não (aviso) |
| `variacao_absurda` | salto de +500% entre dois pontos? | não (aviso) |

Qualidade crítica falhando → `pipeline run` sai com código 1 e a execução é
gravada como `erro` na tabela `execucao` (o Airflow marca a task como failed).

## Decisões que valem explicar em entrevista

- **Idempotência em duas camadas**: hash do arquivo na tabela `carga` evita
  reprocessar o mesmo download; `INSERT OR IGNORE` com PK `(serie, data)` evita
  duplicar o fato. Rodar duas vezes dá 0 linhas novas — testado.
- **Raw imutável**: o JSON baixado fica em `data/raw` com metadados (fonte,
  código SGS, janela, hash, data de extração). Reprocessar não exige rede.
- **Validação como parte do pipeline, não do dashboard**: dado ruim falha antes
  de chegar no gráfico. Faixa plausível é por série (Selic em % a.d., crédito em
  R$ milhões) — não um `BETWEEN` genérico.
- **Orquestração desacoplada**: `pipeline/orquestrador.py` é uma DAG de 4 tarefas
  com ordenação topológica própria (testada, inclusive detecção de ciclo); o
  arquivo do Airflow só chama o mesmo CLI. Nada de lógica duplicada.
- **SQLite por escolha**: 380 linhas de fato não justificam Postgres no CI. O
  schema é ANSI o suficiente para trocar o driver (ver `ops/postgres.md`).
- **Mensal vira dia 01**: normalizar `MM/YYYY` para `YYYY-MM-01` deixa `LAG()` e
  janelas de 12 meses funcionando sem tabela de calendário.

## Estrutura

```
pipeline/
  config.py        series (código SGS, faixa plausível) e caminhos
  extract.py       download com retry/backoff, cache raw por janela
  transform.py     normalização de data/valor e deduplicação
  load.py          execução, carga por hash e inserção idempotente
  quality.py       7 testes de qualidade
  orquestrador.py  DAG com ordenação topológica
  job.py           composição + relatório
  cli.py           run | relatorio | status
sql/schema.sql     tabelas, índices e views (mart_variacao, mart_ultimo)
dags/              DAG do Airflow
ops/               docker-compose do Airflow + notas de Postgres
data/fixtures/     4 séries reais em formato BCB (usadas no CI)
tests/             18 testes
```

## Próximos passos

- Materializar `mart_variacao` em tabela (hoje é view) quando passar de 1M linhas
- Adicionar `series_meta` com unidade/periodicidade para o painel não chutar rótulo
- Publicar o relatório em Parquet para consumo no DuckDB/BI
