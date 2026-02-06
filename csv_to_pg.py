# Importa modulo para rutas y operaciones del sistema de archivos
import os
# Importa modulo para expresiones regulares
import re
# Importa modulo para salir del programa con codigo de error
import sys
# Importa modulo para parsear argumentos de linea de comandos
import argparse
# Importa pandas para leer CSV y manejar DataFrame
import pandas as pd
# Importa creador de engines de SQLAlchemy
from sqlalchemy import create_engine
# Importa tipo de error base de SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError

# Importa configuracion externa desde config.py
from config import CONFIG   # configuracion externa


# Define funcion que convierte nombre de archivo a nombre de tabla valido

def sanitize_table_name(name: str) -> str:
    # Obtiene nombre base del archivo (sin ruta ni extension) en minusculas
    # Ejemplo: "C:/data/Ventas 2024-Ano.csv" -> "ventas 2024-ano"
    base = os.path.splitext(os.path.basename(name))[0].lower().strip()
    # Reemplaza caracteres invalidos por guion bajo
    # Ejemplo: "ventas 2024-ano" -> "ventas_2024_ano"
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    # Colapsa multiples guiones bajos y elimina extremos
    # Ejemplo: "__ventas__2024__" -> "ventas_2024"
    base = re.sub(r"_+", "_", base).strip("_")

    # Si queda vacio, usa un nombre generico
    if not base:
        # Asigna nombre por defecto
        # Ejemplo: "!!!!.csv" -> "table"
        base = "table"
    # Si empieza con numero, agrega prefijo
    if base[0].isdigit():
        # Prefija con t_ para evitar identificador invalido
        # Ejemplo: "123.csv" -> "t_123"
        base = "t_" + base

    # Devuelve el nombre normalizado
    return base


# Define funcion para resolver valor entre config, CLI y default

def resolve_value(cfg_value, cli_value, default=None):
    # Si el valor de config no es None, lo usa
    # Ejemplo: cfg_value="A", cli_value="B" -> retorna "A"
    if cfg_value is not None:
        # Retorna el valor de config
        return cfg_value
    # Si el valor de CLI no es None, lo usa
    # Ejemplo: cfg_value=None, cli_value="B" -> retorna "B"
    if cli_value is not None:
        # Retorna el valor de CLI
        return cli_value
    # Si ninguno esta definido, retorna el default
    # Ejemplo: cfg_value=None, cli_value=None, default="C" -> retorna "C"
    return default


# Define funcion que devuelve la lista de CSV a procesar

def get_csv_files(csv_dir: str, csv_names: list[str] | None) -> list[str]:
    # Extensiones soportadas
    allowed_exts = (".csv", ".xlsx", ".xls")
    # Valida que csv_dir sea un directorio existente
    # Ejemplo: csv_dir="C:/no-existe" -> lanza NotADirectoryError
    if not os.path.isdir(csv_dir):
        # Lanza error si no es carpeta valida
        raise NotADirectoryError(f"No es una carpeta valida: {csv_dir}")

    # Si csv_names tiene valores, usa esa lista
    # Ejemplo: csv_names=["a.csv","b.csv"] -> valida solo esos
    if csv_names:
        # Inicializa lista de archivos validos
        files = []
        # Recorre cada nombre dado
        for name in csv_names:
            # Si no es ruta absoluta, la combina con el directorio
            # Ejemplo: csv_dir="C:/csvs", name="a.csv" -> "C:/csvs/a.csv"
            path = name if os.path.isabs(name) else os.path.join(csv_dir, name)
            # Verifica que el archivo exista
            if not os.path.exists(path):
                # Lanza error si falta el CSV
                raise FileNotFoundError(f"No existe el CSV: {path}")
            # Agrega la ruta valida a la lista
            files.append(path)
        # Devuelve la lista validada
        return files

    # Si no hay nombres especificos, busca todos los .csv del directorio
    # Ejemplo: en "C:/csvs" encuentra ["a.csv","b.csv"] y los devuelve ordenados
    files = [
        # Construye la ruta completa al archivo
        os.path.join(csv_dir, f)
        # Recorre los archivos del directorio
        for f in os.listdir(csv_dir)
        # Filtra solo los que terminan en .csv
        if f.lower().endswith(allowed_exts)
    ]

    # Si no encontro ningun CSV, falla
    if not files:
        # Lanza error si no hay CSV
        raise FileNotFoundError(f"No hay CSV/XLSX en: {csv_dir}")

    # Devuelve lista ordenada para procesamiento estable
    return sorted(files)


# Define funcion que carga un CSV en PostgreSQL

def load_csv_to_postgres(engine, csv_path, schema, sep, encoding, chunksize):
    # Obtiene nombre de tabla a partir del nombre del archivo
    # Ejemplo: csv_path="C:/csvs/ventas 2024.csv" -> table_name="ventas_2024"
    table_name = sanitize_table_name(csv_path)

    # Imprime el nombre del CSV en consola
    print(f"\nProcesando CSV: {os.path.basename(csv_path)}")
    # Imprime la tabla destino
    print(f"Tabla destino: {schema}.{table_name}")

    # Lee el archivo a un DataFrame segun su extension
    ext = os.path.splitext(csv_path)[1].lower()
    try:
        if ext == ".csv":
            # Lee todo como texto para preservar ceros a la izquierda
            df = pd.read_csv(
                csv_path,
                sep=sep,
                encoding=encoding,
                dtype=str,
                keep_default_na=False,
            )
        elif ext in (".xlsx", ".xls"):
            # Lee todo como texto para preservar ceros a la izquierda
            df = pd.read_excel(
                csv_path,
                dtype=str,
                keep_default_na=False,
            )
        else:
            raise ValueError(f"Extension no soportada: {ext}")
    except ImportError as e:
        print(f"ERROR: Falta dependencia para leer Excel ({e}).")
        print("Instala: pip install openpyxl")
        sys.exit(1)

    # Normaliza nombres de columnas del DataFrame
    df.columns = [
        # Reemplaza caracteres invalidos, limpia, pasa a minusculas
        # Ejemplo: "Fecha de Venta" -> "fecha_de_venta"
        re.sub(r"[^a-zA-Z0-9_]+", "_", str(c)).strip("_").lower() or "col"
        # Recorre cada columna original
        for c in df.columns
    ]

    # Intenta insertar el DataFrame en la base
    try:
        # Usa to_sql para crear tabla e insertar filas
        df.to_sql(
            # Nombre de la tabla destino
            name=table_name,
            # Engine de conexion
            con=engine,
            # Schema destino
            schema=schema,
            # Falla si la tabla ya existe
            if_exists="fail",
            # No incluye indice como columna
            index=False,
            # Inserta en lotes para rendimiento
            # Ejemplo: chunksize=2000 -> inserta en lotes de 2000 filas
            chunksize=chunksize,
            # Usa multi insert
            method="multi",
        )
        # Confirma exito con numero de filas
        print(f"Tabla creada e insertada ({len(df)} filas)")

    # Captura errores de SQLAlchemy o de valor
    except (ValueError, SQLAlchemyError) as e:
        # Imprime el error real para diagnostico
        print(f"ERROR real: {e}")
        # Indica que se detiene el proceso
        print("Proceso detenido.")
        # Finaliza el programa con error
        sys.exit(1)


# Define funcion principal del script

def main():
    # Crea el parser de argumentos CLI
    parser = argparse.ArgumentParser(
        # Texto de ayuda del programa
        description="Carga CSV a PostgreSQL (se detiene si la tabla existe)."
    )

    # Define argumento para URL de base de datos
    # Ejemplo: --db postgresql+psycopg2://user:pass@localhost:5432/db
    parser.add_argument("--db", help="URL PostgreSQL")
    # Define argumento para directorio de CSV
    # Ejemplo: --dir C:/csvs
    parser.add_argument("--dir", help="Carpeta CSV")
    # Define argumento para lista de CSV especificos
    # Ejemplo: --csv a.csv b.csv
    parser.add_argument("--csv", nargs="*", help="CSV especificos")
    # Define argumento para schema destino
    # Ejemplo: --schema public
    parser.add_argument("--schema", help="Schema destino")
    # Define argumento para separador del CSV
    # Ejemplo: --sep ";"
    parser.add_argument("--sep", help="Separador CSV")
    # Define argumento para encoding del CSV
    # Ejemplo: --encoding latin-1
    parser.add_argument("--encoding", help="Encoding CSV")
    # Define argumento para tamano de lote
    # Ejemplo: --chunksize 5000
    parser.add_argument("--chunksize", type=int, help="Batch size")

    # Parsea los argumentos CLI
    args = parser.parse_args()

    # Resuelve configuracion final
    # Prioridad: config.py -> CLI -> default
    # Ejemplo: si CONFIG["DB_URL"] tiene valor, ignora --db
    db_url = resolve_value(CONFIG["DB_URL"], args.db)
    # Si no hay URL definida, aborta
    if not db_url:
        # Informa error al usuario
        print("ERROR: DB_URL no definida en config.py ni en CLI")
        # Termina con error
        sys.exit(1)

    # Resuelve directorio CSV con default "."
    # Ejemplo: si no hay config ni CLI, usa "." (directorio actual)
    csv_dir = resolve_value(CONFIG["CSV_DIR"], args.dir, ".")
    # Resuelve lista de CSV a procesar
    csv_names = resolve_value(CONFIG["CSV_NAMES"], args.csv)
    # Resuelve schema destino
    schema = resolve_value(CONFIG["SCHEMA"], args.schema, "public")
    # Resuelve separador
    sep = resolve_value(CONFIG["CSV_SEPARATOR"], args.sep, ",")
    # Resuelve encoding
    encoding = resolve_value(CONFIG["CSV_ENCODING"], args.encoding, "utf-8")
    # Resuelve tamanio de lote
    chunksize = resolve_value(CONFIG["CHUNKSIZE"], args.chunksize, 2000)

    # Crea el engine de SQLAlchemy
    engine = create_engine(db_url, future=True)

    # Obtiene la lista de CSVs a procesar
    csv_files = get_csv_files(csv_dir, csv_names)

    # Recorre cada CSV y lo carga
    for csv in csv_files:
        # Llama a la funcion de carga
        load_csv_to_postgres(
            # Pasa el engine
            engine=engine,
            # Pasa la ruta del CSV
            csv_path=csv,
            # Pasa el schema
            schema=schema,
            # Pasa el separador
            sep=sep,
            # Pasa el encoding
            encoding=encoding,
            # Pasa el tamanio de lote
            chunksize=chunksize,
        )

    # Mensaje final de exito
    print("\nProceso finalizado correctamente.")


# Si se ejecuta directamente este archivo
if __name__ == "__main__":
    # Ejecuta la funcion principal
    main()
