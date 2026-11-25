"""Pipeline de indicadores publicos do Banco Central (SGS).

Camadas:
  raw     -> JSON exatamente como veio da API (com hash para idempotencia)
  staging -> tabela normalizada (data ISO, valor float, serie, carga_id)
  marts   -> agregados prontos para consumo (variacao mensal, YoY, media movel)

Sem dependencia externa: urllib + sqlite3 da biblioteca padrao. O motivo e'
que pipeline de dado precisa rodar em CI sem subir cluster para testar 3 series.
"""
__version__ = "1.0.0"
