"""CLI do pipeline.

  python -m pipeline run --series todas --inicio 01/01/2020 --fim 31/12/2026
  python -m pipeline run --fixture            # offline, usa data/fixtures
  python -m pipeline relatorio
  python -m pipeline status
"""
from __future__ import annotations

import argparse
import json
import sys

from .config import SERIES
from .db import conectar, criar_schema
from .job import executar, relatorio, selecionar_series


def cmd_run(args) -> int:
    series = selecionar_series(args.series or ["todas"])
    print("pipeline: %d serie(s) | %s a %s | fonte: %s"
          % (len(series), args.inicio, args.fim, "fixture" if args.fixture else "API do BCB"))
    resumo = executar(
        series=series,
        inicio=args.inicio,
        fim=args.fim,
        usar_fixture=args.fixture,
        banco=args.banco,
    )
    print("execucao %d: %s" % (resumo.execucao_id, "OK" if resumo.ok else "FALHOU"))
    if resumo.erros:
        for erro in resumo.erros:
            print("  erro:", erro)
    return 0 if resumo.ok else 1


def cmd_relatorio(args) -> int:
    dados = relatorio(args.banco)
    if args.json:
        print(json.dumps(dados, ensure_ascii=False, indent=2))
        return 0
    if not dados:
        print("banco vazio - rode: python -m pipeline run --fixture")
        return 0
    print("%-16s %-12s %10s %12s %10s %8s" % ("serie", "referencia", "valor", "media", "obs", "var%"))
    for linha in dados:
        print("%-16s %-12s %10s %12s %10d %8s"
              % (
                  linha["serie"],
                  linha["data_referencia"],
                  linha["valor"],
                  linha["media_historica"],
                  linha["observacoes"],
                  linha["variacao_pct"],
              ))
    return 0


def cmd_status(args) -> int:
    conexao = conectar(args.banco)
    criar_schema(conexao)
    execucoes = conexao.execute(
        "SELECT id, iniciada_em, finalizada_em, status, series_ok, series_erro FROM execucao "
        "ORDER BY id DESC LIMIT 5"
    ).fetchall()
    cargas = conexao.execute(
        "SELECT serie, COUNT(*) AS n, SUM(linhas_novas) AS novas FROM carga GROUP BY serie ORDER BY serie"
    ).fetchall()
    print("ultimas execucoes:")
    for linha in execucoes:
        print("  #%d %s -> %s %-7s ok=%d erro=%d"
              % (linha["id"], linha["iniciada_em"], linha["finalizada_em"] or "-",
                 linha["status"], linha["series_ok"], linha["series_erro"]))
    print("cargas por serie:")
    for linha in cargas:
        print("  %-16s %2d cargas | %5s linhas novas" % (linha["serie"], linha["n"], linha["novas"]))
    conexao.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline", description="Pipeline de indicadores do BCB")
    sub = parser.add_subparsers(dest="comando", required=True)

    run = sub.add_parser("run", help="extrai, carrega e valida")
    run.add_argument("--series", nargs="*", choices=[s.nome for s in SERIES] + ["todas"])
    run.add_argument("--inicio", default="01/01/2015")
    run.add_argument("--fim", default="31/12/2026")
    run.add_argument("--fixture", action="store_true", help="nao usa rede (CI)")
    run.add_argument("--banco", default=None)
    run.set_defaults(func=cmd_run)

    rep = sub.add_parser("relatorio", help="ultimo valor de cada serie")
    rep.add_argument("--banco", default=None)
    rep.add_argument("--json", action="store_true")
    rep.set_defaults(func=cmd_relatorio)

    st = sub.add_parser("status", help="historico de execucoes")
    st.add_argument("--banco", default=None)
    st.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
