"""Carga no SQLite: registra execucao, carga (hash) e insere o fato.

Idempotencia em duas camadas:
- `carga` tem UNIQUE(serie, hash): o mesmo arquivo nao carrega duas vezes;
- `fato_indicador` tem PK (serie, data): INSERT OR IGNORE protege o resto.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def agora() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def abrir_execucao(conexao: sqlite3.Connection) -> int:
    cursor = conexao.execute(
        "INSERT INTO execucao (iniciada_em, status) VALUES (?, 'rodando')", (agora(),)
    )
    conexao.commit()
    return int(cursor.lastrowid)


def fechar_execucao(
    conexao: sqlite3.Connection,
    execucao_id: int,
    status: str,
    series_ok: int,
    series_erro: int,
    detalhe: str = "",
) -> None:
    conexao.execute(
        "UPDATE execucao SET finalizada_em = ?, status = ?, series_ok = ?, series_erro = ?, detalhe = ? "
        "WHERE id = ?",
        (agora(), status, series_ok, series_erro, detalhe, execucao_id),
    )
    conexao.commit()


def carga_ja_existe(conexao: sqlite3.Connection, serie: str, hash_arquivo: str) -> bool:
    linha = conexao.execute(
        "SELECT 1 FROM carga WHERE serie = ? AND hash = ?", (serie, hash_arquivo)
    ).fetchone()
    return linha is not None


def registrar_carga(
    conexao: sqlite3.Connection,
    execucao_id: int,
    serie: str,
    fonte: str,
    arquivo: str,
    hash_arquivo: str,
    linhas_lidas: int,
    linhas_novas: int,
    linhas_erro: int,
) -> int:
    cursor = conexao.execute(
        """
        INSERT INTO carga
            (execucao_id, serie, fonte, arquivo, hash, linhas_lidas, linhas_novas, linhas_erro, carregada_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (execucao_id, serie, fonte, arquivo, hash_arquivo, linhas_lidas, linhas_novas, linhas_erro, agora()),
    )
    conexao.commit()
    return int(cursor.lastrowid)


def inserir_fatos(
    conexao: sqlite3.Connection, carga_id: int, registros: list[tuple[str, str, float]]
) -> int:
    """Insere e devolve quantas linhas eram realmente novas."""
    antes = conexao.total_changes
    conexao.executemany(
        "INSERT OR IGNORE INTO fato_indicador (serie, data, valor, carga_id) VALUES (?, ?, ?, ?)",
        [(serie, data, valor, carga_id) for serie, data, valor in registros],
    )
    conexao.commit()
    return conexao.total_changes - antes
