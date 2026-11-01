"""``dsops`` command line.

    dsops dag run   pipelines/orders.py:dag --param date=2026-11-01 --run-id 2026-11-01 --checkpoint-dir .ckpt
    dsops dag graph pipelines/orders.py:dag          # Mermaid flowchart
    dsops dag list  my_pkg.pipelines:dag
    dsops ring --nodes worker-a,worker-b,worker-c user:1 user:2
    dsops serve --port 9100                          # /healthz /readyz /metrics
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from . import __version__
from .distributed import ConsistentHashRing
from .health import serve_ops
from .logs import configure_logging
from .pipeline.dag import DAG


def load_object(ref: str) -> Any:
    """Resolve ``module.path:attr`` or ``path/to/file.py:attr``."""
    if ":" not in ref:
        raise SystemExit(f"expected MODULE:ATTRIBUTE, got {ref!r}")
    target, attr = ref.rsplit(":", 1)
    if target.endswith(".py") or "/" in target:
        path = Path(target).resolve()
        if not path.exists():
            raise SystemExit(f"no such file: {path}")
        spec = importlib.util.spec_from_file_location(path.stem, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[path.stem] = module
        sys.path.insert(0, str(path.parent))
        spec.loader.exec_module(module)
    else:
        sys.path.insert(0, str(Path.cwd()))
        module = importlib.import_module(target)
    obj = module
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def _load_dag(ref: str) -> DAG:
    obj = load_object(ref)
    if callable(obj) and not isinstance(obj, DAG):
        obj = obj()
    if not isinstance(obj, DAG):
        raise SystemExit(f"{ref} is not a dsops DAG (got {type(obj).__name__})")
    return obj


def _parse_params(pairs: Sequence[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep:
            raise SystemExit(f"--param expects KEY=VALUE, got {pair!r}")
        try:
            out[key] = json.loads(value)
        except json.JSONDecodeError:
            out[key] = value
    return out


def cmd_dag_run(args: argparse.Namespace) -> int:
    dag = _load_dag(args.dag)
    if args.only:
        dag = dag.subset(args.only.split(","))
    result = dag.run(params=_parse_params(args.param), run_id=args.run_id, max_workers=args.workers,
                     checkpoint_dir=args.checkpoint_dir, fail_fast=args.fail_fast)
    print(result.summary(), file=sys.stderr)
    return 0 if result.ok else 1


def cmd_dag_graph(args: argparse.Namespace) -> int:
    print(_load_dag(args.dag).to_mermaid())
    return 0


def cmd_dag_list(args: argparse.Namespace) -> int:
    dag = _load_dag(args.dag)
    for name in dag.validate():
        t = dag.tasks[name]
        deps = ", ".join(t.depends_on) or "-"
        print(f"{name:<30} depends_on={deps}  retries={t.retries}")
    return 0


def cmd_ring(args: argparse.Namespace) -> int:
    ring = ConsistentHashRing([n for n in args.nodes.split(",") if n], replicas=args.replicas)
    for key in args.keys:
        nodes = ring.get_nodes(key, args.replication)
        print(f"{key}\t{','.join(nodes)}")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    server = serve_ops(args.port, host=args.host)
    print(f"ops server on http://{args.host}:{server.server_address[1]} (/healthz /readyz /metrics)", file=sys.stderr)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.shutdown()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dsops", description="DevOps toolkit for distributed systems and data engineering")
    p.add_argument("--version", action="version", version=f"dsops {__version__}")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-format", choices=("json", "text"), default="text")
    sub = p.add_subparsers(dest="command", required=True)

    dag = sub.add_parser("dag", help="run and inspect pipelines").add_subparsers(dest="dag_command", required=True)
    run = dag.add_parser("run", help="execute a DAG")
    run.add_argument("dag", help="MODULE:ATTR or FILE.py:ATTR pointing to a DAG (or a factory returning one)")
    run.add_argument("--param", action="append", default=[], help="KEY=VALUE (JSON values allowed); repeatable")
    run.add_argument("--run-id")
    run.add_argument("--workers", type=int, default=4)
    run.add_argument("--checkpoint-dir")
    run.add_argument("--fail-fast", action="store_true")
    run.add_argument("--only", help="comma-separated tasks to run (with their upstream)")
    run.set_defaults(func=cmd_dag_run)

    graph = dag.add_parser("graph", help="print a Mermaid flowchart")
    graph.add_argument("dag")
    graph.set_defaults(func=cmd_dag_graph)

    lst = dag.add_parser("list", help="list tasks in execution order")
    lst.add_argument("dag")
    lst.set_defaults(func=cmd_dag_list)

    ring = sub.add_parser("ring", help="show consistent-hash placement of keys")
    ring.add_argument("--nodes", required=True, help="comma-separated node names")
    ring.add_argument("--replicas", type=int, default=160)
    ring.add_argument("--replication", type=int, default=1, help="nodes per key")
    ring.add_argument("keys", nargs="+")
    ring.set_defaults(func=cmd_ring)

    serve = sub.add_parser("serve", help="run the ops HTTP server")
    serve.add_argument("--port", type=int, default=9100)
    serve.add_argument("--host", default="0.0.0.0")
    serve.set_defaults(func=cmd_serve)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level, json_output=args.log_format == "json")
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
