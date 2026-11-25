"""Configuracao: quais series entram no pipeline e onde ficam os arquivos."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Serie:
    codigo: str          # codigo SGS
    nome: str            # chave usada nas tabelas
    descricao: str
    unidade: str
    periodicidade: str   # diaria | mensal
    minimo: float = -1e9  # faixa plausivel (teste de qualidade)
    maximo: float = 1e9


SERIES: tuple[Serie, ...] = (
    Serie("11", "selic_diaria", "Taxa Selic efetiva diaria", "% a.d.", "diaria", 0.0, 1.0),
    Serie("433", "ipca_mensal", "IPCA - variacao mensal", "%", "mensal", -5.0, 5.0),
    Serie("1", "dolar_compra", "Dolar americano (compra)", "BRL", "diaria", 0.5, 20.0),
    Serie("4390", "credito_pj", "Saldo de credito PJ", "R$ milhoes", "mensal", 0.0, 2e7),
)

POR_NOME = {serie.nome: serie for serie in SERIES}


@dataclass
class Caminhos:
    raiz: Path = field(default_factory=lambda: Path(os.getenv("PIPELINE_HOME", Path(__file__).resolve().parent.parent)))
    banco: Path = field(init=False)
    raw: Path = field(init=False)
    fixtures: Path = field(init=False)

    def __post_init__(self):
        self.banco = Path(os.getenv("PIPELINE_DB", self.raiz / "data" / "indicadores.db"))
        self.raw = Path(os.getenv("PIPELINE_RAW", self.raiz / "data" / "raw"))
        self.fixtures = Path(os.getenv("PIPELINE_FIXTURES", self.raiz / "data" / "fixtures"))

    def garantir_diretorios(self) -> None:
        self.raw.mkdir(parents=True, exist_ok=True)
        self.fixtures.mkdir(parents=True, exist_ok=True)
        self.banco.parent.mkdir(parents=True, exist_ok=True)


CAMINHOS = Caminhos()
URL_SGS = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={inicio}&dataFinal={fim}"
