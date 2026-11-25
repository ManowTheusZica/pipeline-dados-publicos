"""Testes de qualidade de dados: rodam depois da carga e bloqueiam o job.

Cada teste devolve um Resultado. Se algum "critico" falhar, o pipeline sai
com codigo != 0 (o Airflow marca a task como failed e o alerta dispara).

Os testes sao de proposito simples e explicitos: nulo, duplicidade, faixa,
volume minimo, buraco de datas e variacao absurda. Dado ruim que passa
silencioso custa mais caro que pipeline que falha.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class Resultado:
    nome: str
    ok: bool
    detalhe: str
    critico: bool = True

    def __str__(self) -> str:
        marca = "OK  " if self.ok else ("FALHA" if self.critico else "AVISO")
        return "[%s] %-28s %s" % (marca, self.nome, self.detalhe)


def sem_nulo(conexao: sqlite3.Connection) -> Resultado:
    total = conexao.execute(
        "SELECT COUNT(*) FROM fato_indicador WHERE data IS NULL OR valor IS NULL OR serie IS NULL"
    ).fetchone()[0]
    return Resultado("sem_nulo", total == 0, "%d linhas nulas" % total)


def sem_duplicidade(conexao: sqlite3.Connection) -> Resultado:
    total = conexao.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT serie, data, COUNT(*) AS n
            FROM fato_indicador GROUP BY serie, data HAVING n > 1
        )
        """
    ).fetchone()[0]
    return Resultado("sem_duplicidade", total == 0, "%d chaves repetidas" % total)


def datas_validas(conexao: sqlite3.Connection) -> Resultado:
    total = conexao.execute(
        "SELECT COUNT(*) FROM fato_indicador WHERE date(data) IS NULL"
    ).fetchone()[0]
    return Resultado("datas_validas", total == 0, "%d datas invalidas" % total)


def volume_minimo(conexao: sqlite3.Connection, minimo: int = 10) -> Resultado:
    linhas = conexao.execute(
        "SELECT serie, COUNT(*) AS n FROM fato_indicador GROUP BY serie"
    ).fetchall()
    if not linhas:
        return Resultado("volume_minimo", False, "nenhuma serie carregada")
    problematicas = [(linha["serie"], linha["n"]) for linha in linhas if linha["n"] < minimo]
    return Resultado(
        "volume_minimo",
        not problematicas,
        "menos de %d pontos em: %s" % (minimo, problematicas or "nenhuma"),
    )


def faixa_plausivel(conexao: sqlite3.Connection) -> Resultado:
    """Cada serie tem sua faixa (Selic nao pode dar 300%, credito pode passar de 1e6)."""
    from .config import POR_NOME

    problemas = []
    for serie, limites in POR_NOME.items():
        total = conexao.execute(
            "SELECT COUNT(*) FROM fato_indicador WHERE serie = ? AND (valor < ? OR valor > ?)",
            (serie, limites.minimo, limites.maximo),
        ).fetchone()[0]
        if total:
            problemas.append("%s: %d fora de [%s, %s]" % (serie, total, limites.minimo, limites.maximo))
    return Resultado("faixa_plausivel", not problemas, "; ".join(problemas) or "todas dentro da faixa")


def buraco_de_datas(conexao: sqlite3.Connection, max_dias: int = 400) -> Resultado:
    """Maior intervalo entre dois pontos da mesma serie (mensal tolera ~40 dias)."""
    pior = 0
    pior_serie = None
    for serie in [linha[0] for linha in conexao.execute("SELECT DISTINCT serie FROM fato_indicador")]:
        datas = [
            linha[0]
            for linha in conexao.execute(
                "SELECT data FROM fato_indicador WHERE serie = ? ORDER BY data", (serie,)
            )
        ]
        for anterior, atual in zip(datas, datas[1:]):
            from datetime import date

            delta = (date.fromisoformat(atual) - date.fromisoformat(anterior)).days
            if delta > pior:
                pior, pior_serie = delta, serie
    return Resultado(
        "buraco_de_datas",
        pior <= max_dias,
        "maior intervalo %d dias (%s)" % (pior, pior_serie),
        critico=False,
    )


def variacao_absurda(conexao: sqlite3.Connection, limite_pct: float = 500.0) -> Resultado:
    linha = conexao.execute(
        "SELECT serie, data, variacao_pct FROM mart_variacao "
        "WHERE ABS(COALESCE(variacao_pct, 0)) > ? "
        "AND ABS(COALESCE(valor_anterior, 0)) >= 0.05 "
        "ORDER BY ABS(variacao_pct) DESC LIMIT 1",
        (limite_pct,),
    ).fetchone()
    if linha is None:
        return Resultado("variacao_absurda", True, "nenhuma variacao acima de %.0f%%" % limite_pct)
    return Resultado(
        "variacao_absurda",
        False,
        "%s em %s variou %.1f%%" % (linha["serie"], linha["data"], linha["variacao_pct"]),
        critico=False,
    )


def rodar_todos(conexao: sqlite3.Connection) -> list[Resultado]:
    return [
        sem_nulo(conexao),
        sem_duplicidade(conexao),
        datas_validas(conexao),
        volume_minimo(conexao),
        faixa_plausivel(conexao),
        buraco_de_datas(conexao),
        variacao_absurda(conexao),
    ]


def falhou(resultados: list[Resultado]) -> bool:
    return any(not resultado.ok and resultado.critico for resultado in resultados)
