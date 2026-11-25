"""Transformacao: raw JSON -> staging (fato_indicador), idempotente.

Duas decisoes que evitam dor de cabeca:
1. data sempre em ISO (YYYY-MM-DD), mesmo nas series mensais (dia 01);
2. INSERT OR IGNORE pela chave (serie, data): rodar duas vezes nao duplica
   nem sobrescreve valor antigo silenciosamente - mudanca de valor aparece
   no relatorio de qualidade, nao no banco.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .config import Serie

FORMATOS = ("%d/%m/%Y", "%Y-%m-%d", "%m/%Y", "%d/%m/%y")


def normalizar_data(bruto: str) -> str:
    bruto = (bruto or "").strip()
    for formato in FORMATOS:
        try:
            return datetime.strptime(bruto, formato).date().isoformat()
        except ValueError:
            continue
    raise ValueError("data em formato desconhecido: %r" % bruto)


def normalizar_valor(bruto) -> float:
    if isinstance(bruto, (int, float)):
        return float(bruto)
    texto = str(bruto).strip().replace(".", "").replace(",", ".") if "," in str(bruto) else str(bruto)
    return float(texto)


def ler_raw(caminho: Path) -> list[dict]:
    conteudo = json.loads(Path(caminho).read_text(encoding="utf-8"))
    if isinstance(conteudo, dict) and "dados" in conteudo:
        return conteudo["dados"]
    if isinstance(conteudo, list):
        return conteudo
    raise ValueError("raw inesperado em %s" % caminho)


def transformar(serie: Serie, linhas: list[dict]) -> tuple[list[tuple[str, str, float]], int]:
    """Devolve (registros, quantidade_de_linhas_invalidas)."""
    validos: list[tuple[str, str, float]] = []
    invalidas = 0
    vistos = set()
    for linha in linhas:
        try:
            data = normalizar_data(linha.get("data", ""))
            valor = normalizar_valor(linha.get("valor"))
            # serie mensal: o BCB manda MM/YYYY; padronizo no dia 01
            chave = (serie.nome, data)
            if chave in vistos:
                invalidas += 1
                continue
            vistos.add(chave)
            validos.append((serie.nome, data, valor))
        except (ValueError, TypeError):
            invalidas += 1
    validos.sort(key=lambda item: item[1])
    return validos, invalidas
