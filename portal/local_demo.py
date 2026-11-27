"""Run the existing admin backend locally, restoring a backup only on first use."""
import argparse
import os
import sqlite3
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8090)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    runtime = root / 'runtime' / 'demo'
    runtime.mkdir(parents=True, exist_ok=True)
    target = runtime / 'urbanflow.sqlite3'
    os.environ['URBANFLOW_RUNTIME'] = str(runtime)
    os.environ['URBANFLOW_HOME'] = str(root)
    os.environ['URBANFLOW_ADMIN'] = '1'
    if not target.exists():
        backups = sorted((root / 'estado' / 'respaldos').glob('urbanflow-*.sqlite3'))
        if not backups:
            raise SystemExit('No hay respaldo en estado/respaldos. Crea un respaldo del portal primero.')
        source = sqlite3.connect(backups[-1].resolve().as_uri() + '?mode=ro', uri=True)
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            source.close()
            destination.close()
        print(f'Restaurado desde {backups[-1].name}. El respaldo original se conserva.')
    from .db import initialize
    initialize()
    import uvicorn
    print(f'Laboratorio: http://127.0.0.1:{args.port}/laboratory', flush=True)
    print(f'Base persistente: {target}', flush=True)
    print('La demo local no inicia Spark ni crea recursos AWS.', flush=True)
    uvicorn.run('portal.app:app', host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
