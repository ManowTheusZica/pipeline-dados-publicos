"""Orquestracao: DAG de 4 tasks, executada pelo runner local OU pelo Airflow.

runner local (`python -m pipeline run`) e' o caminho usado em CI e em
desenvolvimento; `dags/pipeline_indicadores.py` monta a mesma ordem em
Airflow quando o dado sai do notebook.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Tarefa:
    nome: str
    funcao: Callable
    depende_de: tuple[str, ...] = ()
    resultado: dict = field(default_factory=dict)

    def rodar(self) -> dict:
        self.resultado = self.funcao() or {}
        return self.resultado


class DAG:
    def __init__(self, nome: str):
        self.nome = nome
        self.tarefas: dict[str, Tarefa] = {}

    def adicionar(self, nome: str, funcao: Callable, depende_de: tuple[str, ...] = ()) -> None:
        if nome in self.tarefas:
            raise ValueError("tarefa duplicada: %s" % nome)
        self.tarefas[nome] = Tarefa(nome, funcao, depende_de)

    def ordem(self) -> list[Tarefa]:
        """Ordenacao topologica; levanta erro se houver ciclo ou dependencia solta."""
        prontas: list[Tarefa] = []
        pendentes = dict(self.tarefas)
        while pendentes:
            avancou = False
            for nome, tarefa in list(pendentes.items()):
                faltando = [dep for dep in tarefa.depende_de if dep not in {t.nome for t in prontas}]
                if not faltando:
                    prontas.append(tarefa)
                    del pendentes[nome]
                    avancou = True
            if not avancou:
                raise ValueError("ciclo ou dependencia inexistente em: %s" % sorted(pendentes))
        return prontas

    def rodar(self) -> dict:
        resultados = {}
        for tarefa in self.ordem():
            resultados[tarefa.nome] = tarefa.rodar()
        return resultados
