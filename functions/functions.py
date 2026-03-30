from __future__ import annotations

import re
import time
import shutil
import unicodedata
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, types as sat


####################################################################################################################
########################################## Usar Tabla SQL a PyDF ###################################################
####################################################################################################################

#from __future__ import annotations

from typing import Any, Iterator, Optional

import pyodbc


def _import_pandas():
    import pandas as pd  # type: ignore
    return pd


def _import_polars():
    import polars as pl  # type: ignore
    return pl




def _detect_df_engine(df: Any, prefer: tuple[str, ...] = ("polars", "pandas")) -> str:
    """
    Detecta el engine del DataFrame recibido.
    Retorna: "pandas" o "polars"

    - prefer: orden de preferencia cuando ambos están instalados (no afecta si el tipo es claro).
    """
    # Detección por módulos instalados + isinstance
    for eng in prefer:
        if eng == "pandas":
            try:
                pd = _import_pandas()
                # pandas.DataFrame
                if isinstance(df, pd.DataFrame):
                    return "pandas"
            except Exception:
                pass

        elif eng == "polars":
            try:
                pl = _import_polars()
                # polars.DataFrame o polars.LazyFrame (si quisieras soportarlo)
                if isinstance(df, pl.DataFrame):
                    return "polars"
                # Opcional: soportar LazyFrame convirtiendo a DF (ojo RAM)
                # if isinstance(df, pl.LazyFrame):
                #     return "polars"
            except Exception:
                pass

    # Heurística fallback: si tiene "to_pandas" suele ser polars
    if hasattr(df, "to_pandas"):
        return "polars"

    # Heurística fallback: si tiene "iloc" y "itertuples" suele ser pandas
    if hasattr(df, "iloc") and hasattr(df, "itertuples"):
        return "pandas"

    raise TypeError(
        "No se pudo detectar el tipo de df. Esperaba pandas.DataFrame o polars.DataFrame. "
        f"Recibido: {type(df)!r}"
    )


def _iter_rows(cursor: pyodbc.Cursor, chunksize: int) -> Iterator[list[tuple]]:
    """Itera resultados en chunks como lista de tuplas (menos overhead que pyodbc.Row)."""
    while True:
        batch = cursor.fetchmany(chunksize)
        if not batch:
            break
        # Convertir a tuplas reduce overhead y evita retener referencias al cursor
        yield [tuple(r) for r in batch]


def query_to_df(
    sql: str,
    connection_string: str,
    engine: str = "auto",
    *,
    chunksize: int = 50_000,
    return_iter: bool = False,
) -> Any:
    """
    Ejecuta una query vía pyodbc y devuelve un DataFrame (o un iterador de DataFrames por chunks).

    - chunksize: tamaño de lote para evitar cargar todo en RAM.
    - return_iter=True: devuelve un iterador de DataFrames (streaming).
    """
    engine = (engine or "auto").lower().strip()
    if engine == "auto":
        engine = _detect_df_engine()

    # Conexión y cursor con context managers
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [c[0] for c in cursor.description] if cursor.description else []

            # Si no hay resultados
            if not columns:
                if engine == "pandas":
                    pd = _import_pandas()
                    return pd.DataFrame()
                if engine == "polars":
                    pl = _import_polars()
                    return pl.DataFrame()
                return []

            # --- Modo iterador (más seguro para memoria) ---
            if return_iter:
                if engine == "pandas":
                    pd = _import_pandas()

                    def _it() -> Iterator[Any]:
                        for data in _iter_rows(cursor, chunksize):
                            yield pd.DataFrame.from_records(data, columns=columns)

                    return _it()

                elif engine == "polars":
                    pl = _import_polars()

                    def _it() -> Iterator[Any]:
                        for data in _iter_rows(cursor, chunksize):
                            yield pl.DataFrame(data, schema=columns)

                    return _it()

                else:
                    raise ValueError("engine inválido. Usa 'auto', 'polars' o 'pandas'.")

            # --- Modo “devuelve DF completo” (concatena chunks) ---
            if engine == "pandas":
                pd = _import_pandas()
                frames = []
                for data in _iter_rows(cursor, chunksize):
                    frames.append(pd.DataFrame.from_records(data, columns=columns))
                return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)

            elif engine == "polars":
                pl = _import_polars()
                frames = []
                for data in _iter_rows(cursor, chunksize):
                    frames.append(pl.DataFrame(data, schema=columns))
                return pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=columns)

            else:
                raise ValueError("engine inválido. Usa 'auto', 'polars' o 'pandas'.")
            

####################################################################################################################
########################################## Usar Tabla SQL a PyDF ###################################################
####################################################################################################################

#from __future__ import annotations

from typing import Any, Iterator, Optional, Sequence, Iterable, Literal
import math


def _qident_sqlserver(name: str) -> str:
    """Quote identificadores para SQL Server con corchetes."""
    return f"[{name.replace(']', ']]')}]"


def _full_table_sqlserver(schema: str, table: str) -> str:
    return f"{_qident_sqlserver(schema)}.{_qident_sqlserver(table)}"


def _is_nan(x: Any) -> bool:
    # NaN es el único valor donde x != x es True
    try:
        return x != x  # noqa: PLR0124
    except Exception:
        return False


def _normalize_value(x: Any) -> Any:
    """Convierte NaN -> None (pyodbc suele fallar con NaN en varios tipos)."""
    if x is None:
        return None
    if _is_nan(x):
        return None
    return x


def _iter_rows_from_df(
    df: Any,
    engine: str,
    chunksize: int,
    columns: Sequence[str],
) -> Iterator[list[tuple]]:
    """
    Itera el DataFrame en chunks, devolviendo listas de tuplas (para cursor.executemany).
    Normaliza NaN -> None.
    """
    if engine == "pandas":
        pd = _import_pandas()
        n = len(df)
        for start in range(0, n, chunksize):
            chunk = df.iloc[start:start + chunksize]
            # itertuples es rápido; normalizamos NaN->None en python
            rows = []
            for r in chunk.itertuples(index=False, name=None):
                rows.append(tuple(_normalize_value(v) for v in r))
            yield rows

    elif engine == "polars":
        # polars es columnar; slice + iter_rows
        n = df.height
        for start in range(0, n, chunksize):
            chunk = df.slice(start, chunksize)
            rows = []
            for r in chunk.iter_rows(named=False):
                rows.append(tuple(_normalize_value(v) for v in r))
            yield rows

    else:
        raise ValueError("engine inválido. Usa pandas o polars.")


def _build_insert_sql_sqlserver(schema: str, table: str, columns: Sequence[str]) -> str:
    full = _full_table_sqlserver(schema, table)
    cols = ", ".join(_qident_sqlserver(c) for c in columns)
    params = ", ".join("?" for _ in columns)
    return f"INSERT INTO {full} ({cols}) VALUES ({params})"


def _build_delete_in_sql_sqlserver(schema: str, table: str, col: str, n_params: int) -> str:
    full = _full_table_sqlserver(schema, table)
    placeholders = ", ".join("?" for _ in range(n_params))
    return f"DELETE FROM {full} WHERE {_qident_sqlserver(col)} IN ({placeholders})"


def _build_merge_sql_sqlserver(
    schema: str,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    stage_table: str = "#stage",
) -> str:
    """
    Genera un MERGE SQL Server:
      - MATCHED: UPDATE columnas no-clave
      - NOT MATCHED: INSERT
    """
    full = _full_table_sqlserver(schema, table)

    cols_q = [_qident_sqlserver(c) for c in columns]
    keys_q = [_qident_sqlserver(k) for k in key_columns]

    on_clause = " AND ".join([f"T.{k} = S.{k}" for k in keys_q])

    non_keys = [c for c in columns if c not in set(key_columns)]
    if non_keys:
        set_clause = ", ".join([f"T.{_qident_sqlserver(c)} = S.{_qident_sqlserver(c)}" for c in non_keys])
        when_matched = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    else:
        # si todo son claves, no tiene sentido actualizar
        when_matched = ""

    insert_cols = ", ".join(cols_q)
    insert_vals = ", ".join([f"S.{c}" for c in cols_q])

    merge_sql = f"""
MERGE INTO {full} AS T
USING {stage_table} AS S
ON {on_clause}
{when_matched}
WHEN NOT MATCHED BY TARGET THEN
  INSERT ({insert_cols}) VALUES ({insert_vals});
"""
    return merge_sql.strip()


def df_to_db(
    df: Any,
    *,
    connection_string: str,
    schema: str,
    table: str,
    mode: Literal["append", "truncate_append", "replace_partition", "upsert"] = "append",
    engine: str = "auto",
    chunksize: int = 20_000,
    columns: Optional[Sequence[str]] = None,
    # replace_partition
    partition_column: Optional[str] = None,
    partition_values: Optional[Sequence[Any]] = None,
    partition_batch: int = 900,  # SQL Server limit práctico para IN params (<=2100)
    # upsert
    key_columns: Optional[Sequence[str]] = None,
    # perf/tx
    commit_every_chunk: bool = False,
) -> dict[str, Any]:
    """
    Inserta/actualiza tabla desde un DataFrame con pyodbc y chunking.

    Modos:
    - append: inserta todo
    - truncate_append: TRUNCATE TABLE + insert
    - replace_partition: borra por partition_column IN (values) y luego inserta solo esas particiones (o todo df si no filtras)
    - upsert: staging temp table + MERGE (SQL Server)

    Retorna resumen: filas insertadas, filas borradas, etc.
    """
    if engine == "auto":
        engine = _detect_df_engine(df)
    engine = engine.lower().strip()

    if columns is None:
        # Extrae columnas del DF según engine
        if engine in ("pandas", "polars"):
            columns = list(df.columns)
        else:
            raise ValueError("engine inválido. Usa pandas o polars.")
    else:
        columns = list(columns)

    summary = {
        "engine": engine,
        "mode": mode,
        "schema": schema,
        "table": table,
        "columns": list(columns),
        "chunksize": chunksize,
        "rows_inserted": 0,
        "rows_deleted": 0,
        "rows_staged": 0,
        "notes": [],
    }

    insert_sql = _build_insert_sql_sqlserver(schema, table, columns)

    # Conexión manual para controlar autocommit/transacciones
    conn = pyodbc.connect(connection_string)
    try:
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.fast_executemany = True

        full = _full_table_sqlserver(schema, table)

        # 1) TRUNCATE si aplica
        if mode == "truncate_append":
            cursor.execute(f"TRUNCATE TABLE {full}")
            summary["notes"].append("TRUNCATE ejecutado.")

        # 2) replace_partition: delete por IN (...) antes de insertar
        if mode == "replace_partition":
            if not partition_column:
                raise ValueError("mode='replace_partition' requiere partition_column.")
            # si no pasas partition_values, los saco del df (distinct)
            if partition_values is None:
                if engine == "pandas":
                    partition_values = list(df[partition_column].dropna().unique())
                elif engine == "polars":
                    partition_values = df.select(partition_column).unique().to_series().to_list()
                else:
                    partition_values = []

            vals = list(partition_values)
            if vals:
                # Borrado en lotes para no exceder 2100 params
                for i in range(0, len(vals), partition_batch):
                    batch = vals[i:i + partition_batch]
                    del_sql = _build_delete_in_sql_sqlserver(schema, table, partition_column, len(batch))
                    cursor.execute(del_sql, batch)
                    summary["rows_deleted"] += cursor.rowcount if cursor.rowcount != -1 else 0
                summary["notes"].append(f"DELETE por partición {partition_column} aplicado a {len(vals)} valores.")
            else:
                summary["notes"].append("No hubo valores de partición para borrar (lista vacía).")

        # 3) UPSERT: staging + merge
        if mode == "upsert":
            if not key_columns:
                raise ValueError("mode='upsert' requiere key_columns (lista de columnas clave).")
            key_columns = list(key_columns)

            # Crear #stage con mismo schema del destino (SQL Server)
            cursor.execute(f"IF OBJECT_ID('tempdb..#stage') IS NOT NULL DROP TABLE #stage;")
            cursor.execute(f"SELECT TOP 0 * INTO #stage FROM {full};")

            # Para temp table, insert sin schema:
            cols = ", ".join(_qident_sqlserver(c) for c in columns)
            params = ", ".join("?" for _ in columns)
            stage_insert_sql = f"INSERT INTO #stage ({cols}) VALUES ({params})"

            for rows in _iter_rows_from_df(df, engine, chunksize, columns):
                if not rows:
                    continue
                cursor.executemany(stage_insert_sql, rows)
                summary["rows_staged"] += len(rows)
                if commit_every_chunk:
                    conn.commit()

            # MERGE stage -> target
            merge_sql = _build_merge_sql_sqlserver(schema, table, columns, key_columns, stage_table="#stage")
            cursor.execute(merge_sql)

            # Limpieza
            cursor.execute("DROP TABLE #stage;")

        else:
            # 4) INSERT normal (append/truncate_append/replace_partition)
            for rows in _iter_rows_from_df(df, engine, chunksize, columns):
                if not rows:
                    continue
                cursor.executemany(insert_sql, rows)
                summary["rows_inserted"] += len(rows)
                if commit_every_chunk:
                    conn.commit()

        conn.commit()
        return summary

    except Exception as e:
        conn.rollback()
        summary["notes"].append(f"ROLLBACK por error: {e!r}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

####################################################################################################################
###################################### Conexión con el servidor SQL ################################################
####################################################################################################################

#from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple#, Any, Optional

#import re
#import pyodbc
#from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


OnFail = Literal["warn", "silent", "raise"]


@dataclass
class SqlServerDiagnostics:
    ok: bool
    stage: str                      # "connect" | "query" | "sqlalchemy_engine"
    odbc_connection_string: str
    sqlalchemy_url: str
    sqlstate: str | None = None
    native_code: int | None = None
    message: str | None = None
    hint: str | None = None
    exception_type: str | None = None


def _parse_pyodbc_error(e: pyodbc.Error) -> Tuple[str | None, int | None, str]:
    """
    Intenta extraer:
      - SQLSTATE (e.g. '08001', '28000', 'HYT00')
      - native_code (si aparece)
      - mensaje completo
    """
    # e.args puede ser: [('08001', '...'), ...] o strings sueltos
    msg = " | ".join(str(a) for a in getattr(e, "args", [str(e)]))
    sqlstate = None
    native_code = None

    # Caso típico: ('08001', '[08001] ...')
    if getattr(e, "args", None):
        first = e.args[0]
        if isinstance(first, str) and re.fullmatch(r"[0-9A-Z]{5}", first):
            sqlstate = first

    # Buscar SQLSTATE en texto
    if not sqlstate:
        m = re.search(r"\bSQLSTATE=([0-9A-Z]{5})\b", msg)
        if m:
            sqlstate = m.group(1)
        else:
            m = re.search(r"\[([0-9A-Z]{5})\]", msg)
            if m:
                sqlstate = m.group(1)

    # Buscar native error code (muy dependiente del driver)
    m = re.search(r"\bNativeError\s*=\s*(-?\d+)\b", msg, re.IGNORECASE)
    if m:
        native_code = int(m.group(1))

    return sqlstate, native_code, msg


def _diagnose(sqlstate: str | None, message: str) -> str:
    """
    Traduce a causa probable + qué revisar.
    (No es perfecto, pero cubre la mayoría de casos comunes)
    """
    m = message.lower()

    # Driver / librería
    if "data source name not found" in m or "can't open lib" in m or "driver manager" in m:
        return (
            "Driver ODBC no encontrado o nombre incorrecto. "
            "Verifica el parámetro driver (ej: 'ODBC Driver 17 for SQL Server' o 18) "
            "y que esté instalado en la máquina."
        )

    # TLS / Certificados / Encrypt
    if "certificate" in m or "ssl provider" in m or "encryption" in m or "tls" in m:
        return (
            "Problema TLS/Certificado al negociar cifrado. "
            "Prueba agregando 'Encrypt=yes;TrustServerCertificate=yes;' (temporal) "
            "o instala la CA/cert correcto. Con Driver 18 suele requerirse Encrypt."
        )

    # SSPI/Kerberos (Trusted Connection)
    if "sspi" in m or "kerberos" in m or "cannot generate sspi context" in m:
        return (
            "Fallo de autenticación integrada (SSPI/Kerberos). "
            "Revisa dominio/SPN, hora del equipo, VPN, y que el server soporte AD. "
            "Como workaround, prueba SQL Auth (usuario/clave) o configura SPN."
        )

    # Timeouts
    if sqlstate in ("HYT00", "HYT01") or "timeout" in m:
        return (
            "Timeout al conectar/ejecutar. Revisa latencia, VPN, firewall, "
            "puerto 1433, y considera aumentar 'timeout' en la conexión."
        )

    # Login failed
    if sqlstate == "28000" or "login failed" in m:
        return (
            "Credenciales inválidas o sin permisos. "
            "Si usas Trusted_Connection, verifica que tu usuario tenga acceso. "
            "Si usas SQL Auth, revisa usuario/clave y que SQL Server esté en modo mixto."
        )

    # Server not found / network
    if sqlstate == "08001" or "server was not found" in m or "tcp provider" in m or "named pipes provider" in m:
        return (
            "No se puede llegar al servidor (DNS/red/puerto/instancia). "
            "Verifica que 'server' esté correcto (HOST o HOST\\INSTANCIA), "
            "que haya conectividad (ping/DNS), que el firewall permita 1433 "
            "y que SQL Browser esté activo si usas instancia nombrada."
        )

    # DB access / doesn't exist
    if "cannot open database" in m or "unknown database" in m:
        return (
            "La base de datos no existe o tu usuario no tiene permisos sobre ella. "
            "Verifica el nombre 'database' y permisos (CONNECT/USER) en esa DB."
        )

    # Permission denied / SQL error
    if sqlstate == "42000" or "permission" in m or "denied" in m:
        return (
            "Error de permisos SQL. Aunque conecte, el usuario no puede ejecutar la consulta "
            "o acceder a la DB/objeto. Revisa roles/permisos."
        )

    return (
        "Error no clasificado. Revisa el mensaje original y valida: driver, server, "
        "instancia, red, credenciales, TLS/encrypt, y permisos."
    )


def build_sqlserver_engine(
    server: str,
    database: str,
    driver: str = "ODBC Driver 17 for SQL Server",
    trusted_connection: bool = True,
    # Auth SQL opcional (si trusted_connection=False)
    username: str | None = None,
    password: str | None = None,
    # Seguridad/conectividad
    timeout: int = 5,
    encrypt: bool | None = None,                 # None = no especifica; True/False fuerza
    trust_server_certificate: bool | None = None,
    # SQLAlchemy pooling
    fast_executemany: bool = True,
    pool_pre_ping: bool = True,
    pool_size: int = 5,
    max_overflow: int = 2,
    pool_recycle: int = 3600,
    on_fail: OnFail = "warn",
    return_diagnostics: bool = False,
) -> Engine | None | tuple[Engine | None, SqlServerDiagnostics]:
    """
    Crea un SQLAlchemy Engine para SQL Server si hay conectividad, y opcionalmente
    retorna diagnósticos detallados si falla.

    Si return_diagnostics=True:
        retorna (engine|None, diagnostics)
    """

    # 1) ODBC connection string (para test rápido)
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"Connection Timeout={timeout}",
    ]

    if trusted_connection:
        parts.append("Trusted_Connection=yes")
    else:
        # SQL Auth
        if username is not None:
            parts.append(f"UID={username}")
        if password is not None:
            parts.append(f"PWD={password}")

    # TLS flags (útil sobre todo en Driver 18)
    if encrypt is True:
        parts.append("Encrypt=yes")
    elif encrypt is False:
        parts.append("Encrypt=no")

    if trust_server_certificate is True:
        parts.append("TrustServerCertificate=yes")
    elif trust_server_certificate is False:
        parts.append("TrustServerCertificate=no")

    odbc_connection_string = ";".join(parts) + ";"

    # 2) SQLAlchemy URL (siempre con Trusted_Connection)
    driver_url = driver.replace(" ", "+")
    sqlalchemy_url = (
        f"mssql+pyodbc://{server}/{database}"
        f"?trusted_connection=yes&driver={driver_url}"
    )

    diag = SqlServerDiagnostics(
        ok=False,
        stage="connect",
        odbc_connection_string=odbc_connection_string,
        sqlalchemy_url=sqlalchemy_url,
    )

    # 3) Test de conectividad + query
    try:
        with pyodbc.connect(odbc_connection_string) as conn:
            diag.stage = "query"
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                row = cursor.fetchone()
                if not (row is not None and row[0] == 1):
                    diag.message = "Conectó pero SELECT 1 no devolvió el valor esperado."
                    diag.hint = "Revisa permisos mínimos o políticas que bloqueen queries."
                    raise RuntimeError(diag.message)

        # 4) Engine listo para usar
        try:
            engine = create_engine(
                sqlalchemy_url,
                fast_executemany=fast_executemany,
                pool_pre_ping=pool_pre_ping,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
            )
            diag.ok = True
            diag.stage = "sqlalchemy_engine"
            if return_diagnostics:
                return engine, diag
            return engine

        except SQLAlchemyError as e:
            diag.ok = False
            diag.stage = "sqlalchemy_engine"
            diag.exception_type = type(e).__name__
            diag.message = str(e)
            diag.hint = (
                "Falló la creación del Engine. Revisa la URL, driver, y parámetros. "
                "Si usas SQL Auth, evita caracteres especiales sin URL-encoding o usa odbc_connect."
            )

    except pyodbc.Error as e:
        sqlstate, native_code, msg = _parse_pyodbc_error(e)
        diag.ok = False
        diag.stage = "connect" if diag.stage == "connect" else diag.stage
        diag.sqlstate = sqlstate
        diag.native_code = native_code
        diag.message = msg
        diag.exception_type = type(e).__name__
        diag.hint = _diagnose(sqlstate, msg)

    except Exception as e:
        diag.ok = False
        diag.exception_type = type(e).__name__
        diag.message = str(e)
        diag.hint = "Excepción no-ODBC. Revisa el stacktrace y el flujo de la función."

    # 5) Fallback según on_fail
    msg = (
        "[WARN] No se pudo conectar/crear engine para SQL Server.\n"
        f"Stage: {diag.stage}\n"
        f"SQLSTATE: {diag.sqlstate}\n"
        f"Native: {diag.native_code}\n"
        f"Error: {diag.message}\n"
        f"Hint: {diag.hint}\n"
    )

    if on_fail == "warn":
        print(msg)
        if return_diagnostics:
            return None, diag
        return None

    if on_fail == "silent":
        if return_diagnostics:
            return None, diag
        return None

    if on_fail == "raise":
        # Incluye diagnóstico completo
        raise RuntimeError(msg)

    raise ValueError("on_fail debe ser: 'warn', 'silent' o 'raise'")

####################################################################################################################
########################################### SQL sin devolver DF ################################################
####################################################################################################################

def exec_sql(
    sql: str,
    connection_string: str,
) -> dict:
    """
    Ejecuta SQL sin devolver DataFrame (para DDL/DML).
    Devuelve un resumen simple.
    """
    import pyodbc
    t0 = time.time()
    conn = pyodbc.connect(connection_string)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(sql)

        # Consumir nextset si aplica
        while True:
            try:
                if not cur.nextset():
                    break
            except pyodbc.Error:
                break

        conn.commit()
        return {"ok": True, "seconds": round(time.time() - t0, 3)}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": repr(e), "seconds": round(time.time() - t0, 3)}
    finally:
        conn.close()


####################################################################################################################
########################################### Homologación de nombres ################################################
####################################################################################################################

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))


def normalize_name(s: str) -> str:
    s = _strip_accents(s).lower().strip()
    s = s.replace("°", "")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s