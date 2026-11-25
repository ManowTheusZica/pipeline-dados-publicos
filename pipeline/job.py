"""Monta e executa o pipeline completo: extrair -> carregar -> validar -> publicar.

`executar()` devolve um resumo com tudo que aconteceu, e e' o que o CLI, o
Airflow e os testes chamam. Nada de estado global: series e caminhos entram
como argumento, o que deixa o teste rodar em banco temporario.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from . import load, quality
from .config import CAMINHOS, SERIES, Serie
from .db import conectar, criar_schema
from .extract import extrair
from .orquestrador import DAG
from .transform import ler_raw, transformar


@dataclass
class Resumo:
    execucao_id: int = 0
    series: dict = field(default_factory=dict)
    qualidade: list = field(default_factory=list)
    ok: bool = True
    erros: list = field(default_factory=list)


def selecionar_series(nomes: list[str] | None = None) -> list[Serie]:
    if not nomes or "todas" in nomes:
        return list(SERIES)
    escolhidas = []
    for nome in nomes:
        for serie in SERIES:
            if serie.nome == nome:
                escolhidas.append(serie)
                break
        else:
            raise ValueError("serie desconhecida: %s (use: %s)" % (nome, ", ".join(s.nome for s in SERIES)))
    return escolhidas


def executar(
    series: list[Serie] | None = None,
    inicio: str = "01/01/2015",
    fim: str = "31/12/2026",
    usar_fixture: bool = False,
    banco: Path | str | None = None,
    verboso: bool = True,
) -> Resumo:
    series = series or list(SERIES)
    resumo = Resumo()
    conexao = conectar(banco)
    criar_schema(conexao)
    resumo.execucao_id = load.abrir_execucao(conexao)

    dag = DAG("indicadores_bcb")

    def extrair_tudo() -> dict:
        return {serie.nome: extrair(serie, inicio, fim, usar_fixture=usar_fixture) for serie in series}

    def carregar_tudo() -> dict:
        feito = {}
        for serie in series:
            meta = dag.tarefas["extrair"].resultado.get(serie.nome)
            if not meta:
                continue
            if load.carga_ja_existe(conexao, serie.nome, meta["hash"]):
                feito[serie.nome] = {"novas": 0, "motivo": "arquivo ja carregado (hash igual)"}
                continue
            linhas = ler_raw(meta["arquivo"])
            registros, invalidas = transformar(serie, linhas)
            carga_id = load.registrar_carga(
                conexao,
                resumo.execucao_id,
                serie.nome,
                "BCB/SGS %s" % serie.codigo,
                meta["arquivo"],
                meta["hash"],
                len(linhas),
                0,
                invalidas,
            )
            novas = load.inserir_fatos(conexao, carga_id, registros)
            conexao.execute("UPDATE carga SET linhas_novas = ? WHERE id = ?", (novas, carga_id))
            conexao.commit()
            feito[serie.nome] = {"novas": novas, "invalidas": invalidas, "lidas": len(linhas)}
            if verboso:
                print("  carga %-16s %4d lidas | %4d novas | %d invalidas"
                      % (serie.nome, len(linhas), novas, invalidas))
        return feito

    def validar() -> dict:
        resultados = quality.rodar_todos(conexao)
        resumo.qualidade = resultados
        if verboso:
            for resultado in resultados:
                print("  " + str(resultado))
        return {"falhou": quality.falhou(resultados)}

    dag.adicionar("extrair", extrair_tudo)
    dag.adicionar("carregar", carregar_tudo, depende_de=("extrair",))
    dag.adicionar("validar", validar, depende_de=("carregar",))

    try:
        dag.rodar()
        resumo.series = dag.tarefas["carregar"].resultado
        falhou = dag.tarefas["validar"].resultado.get("falhou", False)
        resumo.ok = not falhou
        load.fechar_execucao(
            conexao,
            resumo.execucao_id,
            "erro" if falhou else "ok",
            len(resumo.series),
            0,
            "qualidade falhou" if falhou else "",
        )
    except Exception as exc:
        resumo.ok = False
        resumo.erros.append(str(exc))
        load.fechar_execucao(conexao, resumo.execucao_id, "erro", 0, len(series), str(exc))
        if verboso:
            print("  ERRO:", exc)
    finally:
        conexao.close()
    return resumo


def relatorio(banco: Path | str | None = None) -> list[dict]:
    """Ultimo valor + media + variacao de cada serie (o que o painel mostra)."""
    conexao = conectar(banco)
    criar_schema(conexao)
    linhas = conexao.execute(
        """
        SELECT u.serie, u.data_referencia, u.media_historica, u.observacoes, v.valor, v.variacao_pct
        FROM mart_ultimo u
        LEFT JOIN mart_variacao v ON v.serie = u.serie AND v.data = u.data_referencia
        ORDER BY u.serie
        """
    ).fetchall()
    conexao.close()
    return [dict(linha) for linha in linhas]
