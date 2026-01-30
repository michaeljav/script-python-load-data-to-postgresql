"""
Archivo de configuracion del proyecto CSV -> PostgreSQL

Si una variable esta en None, el script principal intentara
obtenerla desde los argumentos de linea de comandos (CLI).
"""

CONFIG = {
    # ============================
    # REQUERIDO
    # ============================
    # URL de conexion a PostgreSQL usada por SQLAlchemy
    # PON AQUI TUS DATOS: reemplaza None por la URL real.
    # Formato: postgresql+psycopg2://USUARIO:CLAVE@HOST:PUERTO/NOMBRE_BD
    # Ejemplo: postgresql+psycopg2://postgres:mi_clave@localhost:5432/mi_base
    # Si queda en None, el script la exigira por CLI con --db
    "DB_URL": "postgresql+psycopg2://postgres:mi_clave@localhost:5432/mi_base",

    # ============================
    # OPCIONAL
    # ============================
    # Directorio donde estan los CSV
    # Si queda en None, se tomara el directorio pasado por CLI (o ".")
    "CSV_DIR": "./csvs",   # Ej: "./csvs"

    # Lista de CSV especificos a procesar
    # None => procesa todos los CSV del directorio
    # [] (lista vacia) => no procesa ninguno
    "CSV_NAMES": None,  # Ej: ["provincias.csv", "municipios.csv"]

    # Schema destino para crear las tablas
    "SCHEMA": "public",

    # Separador del CSV (coma, punto y coma, tab, etc.)
    "CSV_SEPARATOR": ",",

    # Encoding del CSV (utf-8, latin-1, etc.)
    "CSV_ENCODING": "utf-8",

    # Tamano de lote para insercion con to_sql (mas grande = menos viajes)
    "CHUNKSIZE": 2000,
}
