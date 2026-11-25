"""Extracao: API do BCB com retry, cache em disco e modo fixture (CI offline).

Regras:
- o arquivo raw guarda o JSON cru + metadados (fonte, codigo, janela, hash)
- se o arquivo raw da mesma janela ja' existe e o hash confere, nao baixa de novo
- 5xx/timeout: 3 tentativas com backoff exponencial; se falhar, levanta ErroDeRede
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

from .config import CAMINHOS, URL_SGS, Serie


class ErroDeRede(RuntimeError):
    pass


def _hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def caminho_raw(serie: Serie, inicio: str, fim: str) -> Path:
    return CAMINHOS.raw / ("%s_%s_%s.json" % (serie.nome, inicio.replace("/", ""), fim.replace("/", "")))


def baixar(serie: Serie, inicio: str, fim: str, tentativas: int = 3, pausa: float = 1.0) -> bytes:
    url = URL_SGS.format(codigo=serie.codigo, inicio=inicio, fim=fim)
    ultimo_erro: Exception | None = None
    for tentativa in range(1, tentativas + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as resposta:
                return resposta.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            ultimo_erro = exc
            if tentativa < tentativas:
                time.sleep(pausa * (2 ** (tentativa - 1)))
    raise ErroDeRede("falha ao baixar %s apos %d tentativas: %s" % (serie.nome, tentativas, ultimo_erro))


def extrair(serie: Serie, inicio: str, fim: str, usar_fixture: bool = False) -> dict:
    """Baixa (ou le fixture) e grava o raw. Retorna metadados do arquivo."""
    CAMINHOS.garantir_diretorios()
    destino = caminho_raw(serie, inicio, fim)

    if destino.exists():
        registro = json.loads(destino.read_bytes())
        return {
            "arquivo": str(destino),
            "hash": registro.get("hash") or _hash(destino.read_bytes()),
            "linhas": len(registro.get("dados") or []),
            "reaproveitado": True,
            "serie": serie.nome,
        }

    if usar_fixture:
        fixture = CAMINHOS.fixtures / ("%s.json" % serie.nome)
        if not fixture.exists():
            raise ErroDeRede("fixture %s nao existe" % fixture)
        bruto = fixture.read_bytes()
    else:
        bruto = baixar(serie, inicio, fim)

    dados = json.loads(bruto)
    if not isinstance(dados, list):
        raise ValueError("resposta inesperada do BCB para %s" % serie.nome)

    registro = {
        "fonte": "BCB/SGS",
        "codigo": serie.codigo,
        "serie": serie.nome,
        "inicio": inicio,
        "fim": fim,
        "extraido_em": date.today().isoformat(),
        "hash": _hash(bruto),
        "dados": dados,
    }
    destino.write_text(json.dumps(registro, ensure_ascii=False, indent=1), encoding="utf-8")
    return {
        "arquivo": str(destino),
        "hash": registro["hash"],
        "linhas": len(dados),
        "reaproveitado": False,
        "serie": serie.nome,
    }
