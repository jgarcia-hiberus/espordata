#!/usr/bin/env python3
"""
validate.py — Validador SHACL para el Pork Data Space

Uso:
  python validate.py --mode dcat   --data descriptor-dcat-lote.ttl
  python validate.py --mode payload --data payload-lotes-ejemplo.ttl
  python validate.py --mode dcat   --data mi-descriptor.ttl --shapes mi-shacl.ttl
  python validate.py --mode payload --data mi-payload.ttl   --shapes mi-shacl.ttl
  python validate.py --mode dcat   --data mi-descriptor.ttl --output reporte.txt

Modos:
  dcat    — valida un descriptor DCAT contra shacl-dcat-lote.ttl
  payload — valida registros pig:Batch contra shacl-payload-lote.ttl

Opciones:
  --data    PATH    fichero de datos a validar (obligatorio)
  --shapes  PATH    shapes SHACL (opcional, sobreescribe el default del modo)
  --format  FORMAT  formato del fichero de datos: turtle (default), json-ld, n3
  --output  PATH    guardar reporte en fichero de texto (opcional)
  --strict          salir con código 1 también si hay warnings (default: solo violations)
  --quiet           solo mostrar el resultado final (CONFORME / NO CONFORME)
"""

import sys
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

try:
    from pyshacl import validate
    from rdflib.namespace import SH
except ImportError:
    print("ERROR: pyshacl no está instalado. Ejecuta: pip install pyshacl")
    sys.exit(2)

# ── Rutas por defecto (relativas al script) ──────────────
SCRIPT_DIR = Path(__file__).parent
DEFAULTS = {
    "dcat":    SCRIPT_DIR / "shacl-dcat-lote.ttl",
    "payload": SCRIPT_DIR / "shacl-payload-lote.ttl",
}

# ── Colores ANSI ─────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    RED    = "\033[31m"
    YELLOW = "\033[33m"
    GREEN  = "\033[32m"
    BLUE   = "\033[34m"
    GRAY   = "\033[90m"

def strip_ansi(text):
    import re
    return re.sub(r"\033\[[0-9;]*m", "", text)

# ── Parseo de resultados ─────────────────────────────────
def parse_results(results_graph):
    seen = set()
    by_node = defaultdict(lambda: {"Violation": [], "Warning": [], "Info": []})

    for result in results_graph.subjects(predicate=SH.resultSeverity):
        sev   = str(results_graph.value(result, SH.resultSeverity) or "").split("#")[-1]
        msg   = str(results_graph.value(result, SH.resultMessage) or "(sin mensaje)")
        path  = str(results_graph.value(result, SH.resultPath) or "").split("#")[-1].split("/")[-1]
        focus = str(results_graph.value(result, SH.focusNode) or "").split("/")[-1]
        value = str(results_graph.value(result, SH.value) or "")[:80]

        key = (sev, msg[:80], focus, path)
        if key in seen:
            continue
        seen.add(key)

        if sev in ("Violation", "Warning", "Info"):
            by_node[focus][sev].append({
                "msg":   msg,
                "path":  path or "—",
                "value": value,
            })

    return by_node

# ── Formateo del reporte ─────────────────────────────────
def format_report(mode, data_file, shapes_file, conforms, by_node, elapsed):
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    v_total = sum(len(n["Violation"]) for n in by_node.values())
    w_total = sum(len(n["Warning"])   for n in by_node.values())

    lines.append(f"\n{C.BOLD}{'='*64}{C.RESET}")
    lines.append(f"{C.BOLD}  Validación SHACL — modo: {mode.upper()}{C.RESET}")
    lines.append(f"{'='*64}")
    lines.append(f"  Datos    : {data_file}")
    lines.append(f"  Shapes   : {shapes_file}")
    lines.append(f"  Ejecutado: {now}  ({elapsed:.2f}s)")
    lines.append(f"  Nodos    : {len(by_node)}   "
                 f"{C.RED}{v_total} violación(es){C.RESET}   "
                 f"{C.YELLOW}{w_total} advertencia(s){C.RESET}")
    lines.append(f"{'='*64}\n")

    for node in sorted(by_node.keys()):
        violations = by_node[node]["Violation"]
        warnings   = by_node[node]["Warning"]

        if violations:
            status = f"{C.RED}✗ {len(violations)} violación(es){C.RESET}"
        else:
            status = f"{C.GREEN}✓ OK{C.RESET}"

        warn_str = f"  {C.YELLOW}{len(warnings)} warning(s){C.RESET}" if warnings else ""
        lines.append(f"  {C.BOLD}[{node}]{C.RESET}  {status}{warn_str}")

        for item in violations:
            lines.append(f"    {C.RED}VIOLATION{C.RESET}  {item['path']}: {item['msg']}")
            if item["value"]:
                lines.append(f"    {C.GRAY}           valor: {item['value']}{C.RESET}")

        for item in warnings:
            lines.append(f"    {C.YELLOW}WARNING{C.RESET}    {item['path']}: {item['msg']}")
            if item["value"]:
                lines.append(f"    {C.GRAY}           valor: {item['value']}{C.RESET}")

        if violations or warnings:
            lines.append("")

    lines.append(f"{'─'*64}")
    if conforms:
        lines.append(f"\n  {C.BOLD}{C.GREEN}RESULTADO: CONFORME{C.RESET} — el fichero cumple el perfil.\n")
    else:
        lines.append(f"\n  {C.BOLD}{C.RED}RESULTADO: NO CONFORME{C.RESET}"
                     f" — {v_total} violación(es) que corregir.\n")
    lines.append(f"{'─'*64}\n")

    return "\n".join(lines)

# ── Main ─────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Validador SHACL para el Pork Data Space",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--mode",   required=True, choices=["dcat", "payload"],
                        help="Modo de validación")
    parser.add_argument("--data",   required=True,
                        help="Fichero de datos a validar")
    parser.add_argument("--shapes", default=None,
                        help="Shapes SHACL (opcional, sobreescribe el default)")
    parser.add_argument("--format", default="turtle",
                        choices=["turtle", "json-ld", "n3"],
                        dest="fmt",
                        help="Formato del fichero de datos (default: turtle)")
    parser.add_argument("--output", default=None,
                        help="Guardar reporte en fichero")
    parser.add_argument("--strict", action="store_true",
                        help="Salir con código 1 también si hay warnings")
    parser.add_argument("--quiet",  action="store_true",
                        help="Solo mostrar resultado final")
    args = parser.parse_args()

    # Resolver paths
    data_path   = Path(args.data)
    shapes_path = Path(args.shapes) if args.shapes else DEFAULTS[args.mode]

    if not data_path.exists():
        print(f"ERROR: no se encuentra el fichero de datos: {data_path}")
        sys.exit(2)
    if not shapes_path.exists():
        print(f"ERROR: no se encuentra el fichero de shapes: {shapes_path}")
        print(f"  Asegúrate de que {shapes_path.name} está en el mismo directorio que validate.py")
        print(f"  o usa --shapes para indicar su ruta.")
        sys.exit(2)

    if not args.quiet:
        print(f"\n  Validando {data_path.name} ...", end="", flush=True)

    import time
    t0 = time.time()

    conforms, results_graph, _ = validate(
        data_graph=str(data_path),
        shacl_graph=str(shapes_path),
        data_graph_format=args.fmt,
        shacl_graph_format="turtle",
        inference="rdfs",
        advanced=True,
        js=False,
        abort_on_first=False,
        allow_warnings=True,
        meta_shacl=False,
        debug=False,
    )

    elapsed = time.time() - t0
    by_node = parse_results(results_graph)
    w_total = sum(len(n["Warning"]) for n in by_node.values())

    if args.quiet:
        v = sum(len(n["Violation"]) for n in by_node.values())
        if conforms:
            print(f"CONFORME")
        else:
            print(f"NO CONFORME — {v} violación(es)")
    else:
        report = format_report(
            args.mode, data_path, shapes_path, conforms, by_node, elapsed
        )
        print(report)

        if args.output:
            out_path = Path(args.output)
            out_path.write_text(strip_ansi(report), encoding="utf-8")
            print(f"  Reporte guardado en: {out_path}\n")

    # Código de salida
    if not conforms:
        sys.exit(1)
    if args.strict and w_total > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()