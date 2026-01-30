import os
import re
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import CONFIG   # ⬅️ configuración externa


def sanitize_table_name(name: str) -> str:
    base = os.path.splitext(os.path.basename(name))[0].lower().strip()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")

    if not base:
        base = "table"
    if base[0].isdigit():
        base = "t_" + base

    return base


def resolve_value(cfg_value, cli_value, default=None):
    if cfg_value is not None:
        return cfg_value
    if cli_value is not None:
        return cli_value
    return default


def get_csv_files(csv_dir: str, csv_names: list[str] | None) -> list[str]:
    if not os.path.isdir(csv_dir):
        raise NotADirectoryError(f"No es una carpeta válida: {csv_dir}")

    if csv_names:
        files = []
        for name in csv_names:
            path = name if os.path.isabs(name) else os.path.join(csv_dir, name)
            if not os.path.exists(path):
                raise FileNotFoundError(f"No existe el CSV: {path}")
            files.append(path)
        return files

    files = [
        os.path.join(csv_dir, f)
        for f in os.listdir(csv_dir)
        if f.lower().endswith(".csv")
    ]

    if not files:
        raise FileNotFoundError(f"No hay CSV en: {csv_dir}")

    return sorted(files)


def load_csv_to_postgres(engine, csv_path, schema, sep, encoding, chunksize):
    table_name = sanitize_table_name(csv_path)

    print(f"\n📄 Procesando CSV: {os.path.basename(csv_path)}")
    print(f"📌 Tabla destino: {schema}.{table_name}")

    df = pd.read_csv(csv_path, sep=sep, encoding=encoding)

    df.columns = [
        re.sub(r"[^a-zA-Z0-9_]+", "_", str(c)).strip("_").lower() or "col"
        for c in df.columns
    ]

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="fail",
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        print(f"✅ Tabla creada e insertada ({len(df)} filas)")

    except (ValueError, SQLAlchemyError) as e:
        print(f"❌ ERROR: La tabla '{schema}.{table_name}' ya existe o ocurrió un error.")
        print("🛑 Proceso detenido.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Carga CSV a PostgreSQL (se detiene si la tabla existe)."
    )

    parser.add_argument("--db", help="URL PostgreSQL")
    parser.add_argument("--dir", help="Carpeta CSV")
    parser.add_argument("--csv", nargs="*", help="CSV específicos")
    parser.add_argument("--schema", help="Schema destino")
    parser.add_argument("--sep", help="Separador CSV")
    parser.add_argument("--encoding", help="Encoding CSV")
    parser.add_argument("--chunksize", type=int, help="Batch size")

    args = parser.parse_args()

    # Resolver configuración final
    db_url = resolve_value(CONFIG["DB_URL"], args.db)
    if not db_url:
        print("❌ DB_URL no definida en config.py ni en CLI")
        sys.exit(1)

    csv_dir = resolve_value(CONFIG["CSV_DIR"], args.dir, ".")
    csv_names = resolve_value(CONFIG["CSV_NAMES"], args.csv)
    schema = resolve_value(CONFIG["SCHEMA"], args.schema, "public")
    sep = resolve_value(CONFIG["CSV_SEPARATOR"], args.sep, ",")
    encoding = resolve_value(CONFIG["CSV_ENCODING"], args.encoding, "utf-8")
    chunksize = resolve_value(CONFIG["CHUNKSIZE"], args.chunksize, 2000)

    engine = create_engine(db_url, future=True)

    csv_files = get_csv_files(csv_dir, csv_names)

    for csv in csv_files:
        load_csv_to_postgres(
            engine=engine,
            csv_path=csv,
            schema=schema,
            sep=sep,
            encoding=encoding,
            chunksize=chunksize,
        )

    print("\n🎉 Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()
