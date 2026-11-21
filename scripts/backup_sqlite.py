"""Crea una copia consistente de una base SQLite mientras la aplicacion sigue activa."""

import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()

    source = sqlite3.connect(f"file:{args.source}?mode=ro", uri=True)
    destination = sqlite3.connect(args.destination)
    try:
        source.backup(destination)
        result = destination.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise RuntimeError(f"La copia no supero integrity_check: {result}")
    finally:
        destination.close()
        source.close()


if __name__ == "__main__":
    main()
