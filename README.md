
# CSV → PostgreSQL (Script Python paso a paso)

Este proyecto permite cargar uno o varios archivos CSV en PostgreSQL,
creando **una tabla por cada CSV**.

⛔ Si la tabla ya existe, el proceso **SE DETIENE**
⛔ No sobrescribe tablas existentes
⛔ No inserta datos si la tabla existe

> Nota importante:
> Este script **NO deduplica filas**.
> El control es a nivel de **tabla (estructura)**.

---

## 1) Requisitos previos

### Python
- Python **3.10 o superior**

Verificar:
```bash
python --version
```

### PostgreSQL
- Base de datos PostgreSQL accesible
- Usuario con permisos para **crear tablas**

---

## 2) Estructura del proyecto

```
csv_to_pg/
├── csv_to_pg.py
├── config.py
├── README.md
└── csvs/
    ├── provincias.csv
    └── municipios.csv
```

---

## 3) Crear ambiente virtual (env / venv)

### Windows (PowerShell)
```powershell
cd C:\ruta\csv_to_pg
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Si aparece error de permisos:
```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

---

### Windows (Git Bash)
```bash
cd /c/ruta/csv_to_pg
python -m venv .venv
source .venv/Scripts/activate
```

---

### Linux / macOS
```bash
cd /ruta/csv_to_pg
python3 -m venv .venv
source .venv/bin/activate
```

Cuando el ambiente esté activo verás:
```
(.venv)
```

---

## 4) Instalar dependencias

```bash
pip install --upgrade pip
pip install pandas sqlalchemy psycopg2-binary
```

---

## 5) Archivo de configuración (config.py)

```python
CONFIG = {
    "DB_URL": "postgresql+psycopg2://user:pass@localhost:5432/dbname",
    "CSV_DIR": "./csvs",
    "CSV_NAMES": None,
    "SCHEMA": "public",
    "CSV_SEPARATOR": ",",
    "CSV_ENCODING": "utf-8",
    "CHUNKSIZE": 2000,
}
```

---

## 6) Ejecutar el script

### Usando solo config.py
```bash
python csv_to_pg.py
```

### Usando parámetros CLI
```bash
python csv_to_pg.py --db "postgresql+psycopg2://user:pass@localhost:5432/dbname" --dir "./csvs"
```

---

## 7) Qué pasa si la tabla existe

```
❌ ERROR: La tabla ya existe.
🛑 El proceso se ha detenido.
```

---

## 8) Salir del ambiente virtual
```bash
deactivate
```
