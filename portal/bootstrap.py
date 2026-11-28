import argparse
import re

from .db import connect, initialize


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare")
    args = parser.parse_args()
    initialize()
    if args.prepare is None:
        return
    months = args.prepare.split(",")
    if not 1 <= len(months) <= 24 or not all(re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", m) for m in months):
        parser.error("Selecciona entre uno y 24 meses YYYY-MM.")
    numbers = [int(m[:4]) * 12 + int(m[5:]) for m in months]
    if numbers != list(range(numbers[0], numbers[0] + len(numbers))):
        parser.error("Los meses deben ser consecutivos, en orden y sin repeticiones.")
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM releases WHERE name='mobility'").fetchone():
            print("Se conserva la versión publicada. Usa un clúster nuevo para cambiar el intervalo inicial.")
        elif not conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running')").fetchone():
            job = conn.execute("INSERT INTO jobs(kind,months,status) VALUES('prepare',?,'queued')",
                               (args.prepare,)).lastrowid
            print(f"Preparación en cola, trabajo {job}.")


if __name__ == "__main__":
    main()
