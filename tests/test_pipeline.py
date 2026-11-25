"""Testes do pipeline: transformacao, carga idempotente, qualidade e DAG."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.db as db_mod
from pipeline import load, quality
from pipeline.config import POR_NOME, SERIES
from pipeline.db import criar_schema
from pipeline.extract import extrair
from pipeline.job import executar, relatorio
from pipeline.orquestrador import DAG
from pipeline.transform import normalizar_data, normalizar_valor, transformar, ler_raw

RAIZ = Path(__file__).resolve().parent.parent


@pytest.fixture
def conexao(tmp_path):
    con = sqlite3.connect(tmp_path / "teste.db")
    con.row_factory = sqlite3.Row
    criar_schema(con)
    yield con
    con.close()


@pytest.fixture
def banco_tmp(tmp_path, monkeypatch):
    caminho = tmp_path / "pipeline.db"
    monkeypatch.setenv("PIPELINE_DB", str(caminho))
    return caminho


# --------------------------------------------------------------- transform
def test_normalizar_data_formatos():
    assert normalizar_data("01/01/2020") == "2020-01-01"
    assert normalizar_data("2020-01-31") == "2020-01-31"
    assert normalizar_data("03/2021") == "2021-03-01"


def test_normalizar_data_invalida():
    with pytest.raises(ValueError):
        normalizar_data("31/31/2020")


def test_normalizar_valor_com_virgula():
    assert normalizar_valor("0,38") == 0.38
    assert normalizar_valor("1.234,56") == 1234.56
    assert normalizar_valor(2) == 2.0


def test_transformar_descarta_invalidos_e_duplicados():
    serie = POR_NOME["ipca_mensal"]
    linhas = [
        {"data": "01/01/2020", "valor": "0,20"},
        {"data": "01/01/2020", "valor": "0,99"},   # duplicada: descartada
        {"data": "01/02/2020", "valor": "0,30"},
        {"data": "xx/yy", "valor": "1"},           # invalida
        {"data": "01/03/2020", "valor": "abc"},    # invalida
    ]
    registros, invalidas = transformar(serie, linhas)
    assert invalidas == 3
    assert registros == [
        ("ipca_mensal", "2020-01-01", 0.2),
        ("ipca_mensal", "2020-02-01", 0.3),
    ]


def test_ler_raw_aceita_lista_e_dicionario(tmp_path):
    lista = tmp_path / "lista.json"
    lista.write_text(json.dumps([{"data": "01/01/2020", "valor": "1"}]))
    assert len(ler_raw(lista)) == 1

    envelope = tmp_path / "envelope.json"
    envelope.write_text(json.dumps({"dados": [{"data": "01/01/2020", "valor": "1"}]}))
    assert len(ler_raw(envelope)) == 1


# ---------------------------------------------------------------- extracao
def test_extrair_fixture_grava_raw_com_hash(monkeypatch, tmp_path):
    import pipeline.extract as ext

    monkeypatch.setattr(ext.CAMINHOS, "raw", tmp_path)
    monkeypatch.setattr(ext.CAMINHOS, "fixtures", RAIZ / "data" / "fixtures")
    serie = POR_NOME["ipca_mensal"]
    meta = extrair(serie, "01/01/2015", "31/12/2026", usar_fixture=True)
    assert meta["linhas"] > 10
    assert meta["reaproveitado"] is False

    de_novo = extrair(serie, "01/01/2015", "31/12/2026", usar_fixture=True)
    assert de_novo["reaproveitado"] is True
    assert de_novo["hash"] == meta["hash"]


# ------------------------------------------------------------------- carga
def test_carga_idempotente_por_hash(conexao):
    execucao = load.abrir_execucao(conexao)
    assert load.carga_ja_existe(conexao, "ipca_mensal", "abc") is False
    load.registrar_carga(conexao, execucao, "ipca_mensal", "BCB", "arq.json", "abc", 10, 0, 0)
    assert load.carga_ja_existe(conexao, "ipca_mensal", "abc") is True


def test_inserir_fatos_nao_duplica(conexao):
    execucao = load.abrir_execucao(conexao)
    carga = load.registrar_carga(conexao, execucao, "ipca_mensal", "BCB", "a.json", "h1", 2, 0, 0)
    registros = [("ipca_mensal", "2020-01-01", 0.2), ("ipca_mensal", "2020-02-01", 0.3)]
    assert load.inserir_fatos(conexao, carga, registros) == 2
    assert load.inserir_fatos(conexao, carga, registros) == 0


def test_inserir_fatos_conta_so_os_novos(conexao):
    execucao = load.abrir_execucao(conexao)
    carga = load.registrar_carga(conexao, execucao, "ipca_mensal", "BCB", "a.json", "h1", 2, 0, 0)
    load.inserir_fatos(conexao, carga, [("ipca_mensal", "2020-01-01", 0.2)])
    novos = load.inserir_fatos(
        conexao, carga, [("ipca_mensal", "2020-01-01", 0.2), ("ipca_mensal", "2020-02-01", 0.3)]
    )
    assert novos == 1


# --------------------------------------------------------------- qualidade
def _popular(conexao, registros):
    execucao = load.abrir_execucao(conexao)
    carga = load.registrar_carga(conexao, execucao, "ipca_mensal", "BCB", "a.json", "h1", len(registros), 0, 0)
    load.inserir_fatos(conexao, carga, registros)


def test_qualidade_banco_vazio_falha_volume(conexao):
    resultados = {r.nome: r for r in quality.rodar_todos(conexao)}
    assert resultados["volume_minimo"].ok is False
    assert resultados["sem_nulo"].ok is True


def test_qualidade_pega_valor_fora_da_faixa(conexao):
    registros = [("ipca_mensal", "2020-%02d-01" % mes, 0.2) for mes in range(1, 13)]
    registros.append(("ipca_mensal", "2021-01-01", 99.0))     # impossivel para IPCA
    _popular(conexao, registros)
    resultado = quality.faixa_plausivel(conexao)
    assert resultado.ok is False
    assert "ipca_mensal" in resultado.detalhe


def test_qualidade_banco_bom_passa(conexao):
    registros = [("ipca_mensal", "2020-%02d-01" % mes, 0.2) for mes in range(1, 13)]
    _popular(conexao, registros)
    resultados = quality.rodar_todos(conexao)
    assert quality.falhou(resultados) is False
    assert all(r.ok for r in resultados if r.critico)


def test_variacao_absurda_ignora_base_perto_de_zero(conexao):
    registros = [
        ("ipca_mensal", "2020-01-01", 0.001),
        ("ipca_mensal", "2020-02-01", 0.05),
    ]
    _popular(conexao, registros)
    assert quality.variacao_absurda(conexao).ok is True


# ------------------------------------------------------------- orquestrador
def test_dag_ordem_respeita_dependencia():
    dag = DAG("teste")
    dag.adicionar("c", lambda: {"x": 1}, depende_de=("b",))
    dag.adicionar("a", lambda: {"x": 1})
    dag.adicionar("b", lambda: {"x": 1}, depende_de=("a",))
    assert [t.nome for t in dag.ordem()] == ["a", "b", "c"]


def test_dag_detecta_ciclo():
    dag = DAG("ciclo")
    dag.adicionar("a", lambda: None, depende_de=("b",))
    dag.adicionar("b", lambda: None, depende_de=("a",))
    with pytest.raises(ValueError):
        dag.ordem()


# ------------------------------------------------------------------- ponta
def test_pipeline_completo_com_fixture(banco_tmp, monkeypatch, capsys):
    monkeypatch.setattr("pipeline.extract.CAMINHOS.fixtures", RAIZ / "data" / "fixtures")
    resumo = executar(usar_fixture=True, banco=banco_tmp)
    assert resumo.ok is True
    assert len(resumo.series) == len(SERIES)
    assert all(info["novas"] > 0 for info in resumo.series.values())

    dados = relatorio(banco_tmp)
    assert len(dados) == len(SERIES)
    assert all(linha["observacoes"] > 0 for linha in dados)


def test_pipeline_duas_vezes_nao_duplica(banco_tmp, monkeypatch):
    monkeypatch.setattr("pipeline.extract.CAMINHOS.fixtures", RAIZ / "data" / "fixtures")
    executar(usar_fixture=True, banco=banco_tmp, verboso=False)
    segunda = executar(usar_fixture=True, banco=banco_tmp, verboso=False)
    assert all(info["novas"] == 0 for info in segunda.series.values())


def test_cli_fixture_retorna_zero(banco_tmp, monkeypatch):
    monkeypatch.setattr("pipeline.extract.CAMINHOS.fixtures", RAIZ / "data" / "fixtures")
    from pipeline.cli import main

    codigo = main(["run", "--fixture", "--banco", str(banco_tmp)])
    assert codigo == 0
    assert main(["status", "--banco", str(banco_tmp)]) == 0
