"""Acesso ao SQLite e aplicacao do schema.

SQLite porque o volume e' de milhares de linhas e o pipeline precisa rodar
em CI sem servico externo. O SQL e' ANSI o suficiente para Postgres trocar
apenas o driver (ver ops/postgres.md).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import CAMINHOS

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def conectar(caminho: Path | str | None = None) -> sqlite3.Connection:
    destino = Path(caminho) if caminho else CAMINHOS.banco
    destino.parent.mkdir(parents=True, exist_ok=True)
    conexao = sqlite3.connect(destino)
    conexao.row_factory = sqlite3.Row
    conexao.execute("PRAGMA foreign_keys = ON")
    return conexao


def criar_schema(conexao: sqlite3.Connection) -> None:
    conexao.executescript(SCHEMA.read_text(encoding="utf-8"))
    conexao.commit()


def tabela_existe(conexao: sqlite3.Connection, nome: str) -> bool:
    linha = conexao.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?", (nome,)
    ).fetchone()
    return linha is not None
