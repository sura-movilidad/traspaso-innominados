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

from typing import Any, Iterator, Optional, Dict, Literal, Tuple, Sequence, Iterable, List, Mapping

import pyodbc
import math
from dataclasses import dataclass
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from IPython.display import HTML, display
import base64
import io

####################################################################################################################
############################################## Pretty table ########################################################
####################################################################################################################

def _sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara un DataFrame para Parquet (pyarrow):
    - object -> string
    - bytes/bytearray -> utf-8 (replace)
    """
    df2 = df.copy()

    for col in df2.columns:
        if df2[col].dtype == "object":
            df2[col] = df2[col].map(
                lambda x: (
                    x.decode("utf-8", "replace")
                    if isinstance(x, (bytes, bytearray))
                    else x
                )
            ).astype("string")

    return df2

# from IPython.display import HTML, display
# from typing import Optional, Dict, Any, List
# import pandas as pd
#import base64
#import io

def pretty_table(
    data: Any,
    n: int = 10,
    show_types: bool = True,
    color_scheme: Optional[Dict[str, str]] = None,
    title_color: str = "#2C3E50",
    header_color: str = "#34495E",
    stripe_color: str = "#F8F9FA",
    hover_color: str = "#E8F4F8",
    enable_download: bool = True,
    filename_base: str = "tabla",
    # para pintar categóricas
    highlight_col: Optional[str] = None,
    highlight_palette: Optional[Dict[str, str]] = None,
    highlight_cols: Optional[List[str]] = None,
    highlight_cols_palette: Optional[Dict[str, str]] = None,
    title: Optional[str] = None,
) -> None:
    """
    Muestra un DataFrame con estilo visual tipo tibble de R y,
    opcionalmente, agrega botones para descargar los datos en CSV, Excel, HTML o parquet.

    Soporta: Polars DataFrame, PySpark DataFrame, Pandas DataFrame.
    """
    
    # Esquema de colores por defecto
    if color_scheme is None:
        color_scheme = {
            "str": "#FF6B6B",       # Rojo para string/character
            "object": "#FF6B6B",    # Pandas object -> string
            "Utf8": "#FF6B6B",      # Polars string
            "String": "#FF6B6B",    # Polars/Spark string
            "string": "#FF6B6B",    # Spark string type
            "float": "#4ECDC4",     # Turquesa para float/double
            "float64": "#4ECDC4",
            "float32": "#4ECDC4",
            "Float64": "#4ECDC4",   # Polars
            "Float32": "#4ECDC4",
            "double": "#4ECDC4",    # Spark
            "int": "#45B7D1",       # Azul para integer
            "int64": "#45B7D1",
            "int32": "#45B7D1",
            "Int64": "#45B7D1",     # Polars
            "Int32": "#45B7D1",
            "long": "#45B7D1",      # Spark
            "integer": "#45B7D1",
            "bool": "#96CEB4",      # Verde para boolean/logical
            "Boolean": "#96CEB4",   # Polars
            "boolean": "#96CEB4",   # Spark
            "date": "#DDA15E",      # Naranja para date
            "Date": "#DDA15E",      # Polars
            "datetime": "#DDA15E",
            "Datetime": "#DDA15E",  # Polars
            "timestamp": "#DDA15E", # Spark
            "category": "#FFEAA7",  # Amarillo para factor/category
            "Categorical": "#FFEAA7", # Polars
            "default": "#95A5A6"    # Gris para otros
        }
    
    # Detectar tipo de DataFrame y convertir a Pandas
    df_type = type(data).__name__
    module_name = type(data).__module__
    original_types = {}
    
    if "polars" in module_name.lower():
        # Polars DataFrame
        original_types = {col: str(dtype) for col, dtype in zip(data.columns, data.dtypes)}
        n_rows = data.height
        n_cols = data.width
        col_names = data.columns
        df_pandas = data.head(n).to_pandas()
        source_lib = "Polars"
        
    elif "pyspark" in module_name.lower():
        # PySpark DataFrame
        schema = data.schema
        original_types = {
            field.name: str(field.dataType).replace("Type()", "") 
            for field in schema.fields
        }
        n_rows = data.count()
        n_cols = len(schema.fields)
        col_names = data.columns
        df_pandas = data.limit(n).toPandas()
        source_lib = "Spark"
        
    elif "pandas" in module_name.lower() or df_type == "DataFrame":
        # Pandas DataFrame
        original_types = {col: str(dtype) for col, dtype in data.dtypes.items()}
        n_rows = len(data)
        n_cols = len(data.columns)
        col_names = list(data.columns)
        df_pandas = data.head(n)
        source_lib = "Pandas"
        
    else:
        raise TypeError(
            f"Tipo de DataFrame no soportado: {df_type}. "
            "Use Polars, PySpark o Pandas."
        )
    
    rows_hidden = max(0, n_rows - n)
    
    rows_hidden = max(0, n_rows - n)

    # =========================
    # Coloreado por fila
    # =========================
    row_color_map: Dict[str, str] = {}

    if highlight_col is not None and highlight_col in df_pandas.columns:
        # Lista base de colores suaves (pasteles) para las filas
        base_row_colors = [
            "#FFF5F5",  # rojo muy suave
            "#FFF9E6",  # amarillo muy suave
            "#F5FFF5",  # verde muy suave
            "#E6F6FF",  # azul muy suave
            "#F9F2FF",  # violeta muy suave
            "#FFEFF7",  # rosado muy suave
        ]

        # Valores únicos de la columna usada para resaltar
        unique_vals = [
            str(v)
            for v in df_pandas[highlight_col].dropna().unique()
        ]

        for i, val in enumerate(unique_vals):
            if highlight_palette and val in highlight_palette:
                # Si el usuario entrega un color para ese valor, se respeta
                row_color_map[val] = highlight_palette[val]
            else:
                # Asignar un color de la paleta base, ciclando si hay más categorías que colores
                row_color_map[val] = base_row_colors[i % len(base_row_colors)]
    
    # =========================
    # Coloreado por columna
    # =========================
    col_color_map: Dict[str, str] = {}

    if highlight_cols:
        # Paleta base para columnas (puedes cambiarla si quieres otra estética)
        base_col_colors = [
            "#55A868",  # verde
            "#8172B3",  # morado
            "#DD8452",  # naranja
            "#C44E52",  # rojo
            "#4C72B0",  # azul
        ]

        for i, col in enumerate(highlight_cols):
            if highlight_cols_palette and col in highlight_cols_palette:
                # Si el usuario pasa un color específico para esa columna
                col_color_map[col] = highlight_cols_palette[col]
            else:
                # Color de la paleta base, ciclando si hay más columnas que colores
                col_color_map[col] = base_col_colors[i % len(base_col_colors)]

    # Función auxiliar para obtener color de fondo de la fila
    def get_row_bg_color(row_value: Any) -> str:
        if pd.isna(row_value):
            return ""
        key = str(row_value)
        # Priorizar palette personalizada si viene
        if highlight_palette and key in highlight_palette:
            return highlight_palette[key]
        return row_color_map.get(key, "")
    
    # ---------------------------------------------

    # Función auxiliar para obtener color según tipo
    def get_type_color(type_str: str) -> str:
        if type_str in color_scheme:
            return color_scheme[type_str]
        
        type_lower = type_str.lower()
        for key, color in color_scheme.items():
            if key.lower() in type_lower or type_lower in key.lower():
                return color
        
        return color_scheme.get("default", "#95A5A6")
    
    # ID único para la tabla
    table_id = f"pretty-table-{id(data)}"
    
    # ---- CSS Tabla + Botones ----
    css = f"""
    <style>
    #{table_id} {{
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 13px;
        border-collapse: collapse;
        margin: 1em 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-radius: 8px;
        overflow: hidden;
    }}
    #{table_id} .title-row {{
        background-color: {title_color};
        color: white;
        font-weight: bold;
        font-size: 14px;
        text-align: left;
        padding: 12px 16px;
    }}
    /* Estilos para título y subtítulo */
    #{table_id} .main-title {{
        font-size: 15px;
        font-weight: 600;
        margin-right: 10px;
    }}
    #{table_id} .subtitle {{
        font-size: 12px;
        font-weight: 400;
        opacity: 0.9;
    }}
    #{table_id} .title-row .source-badge {{
        background-color: rgba(255,255,255,0.2);
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        margin-left: 10px;
    }}
    #{table_id} th {{
        background-color: {header_color};
        color: white;
        padding: 10px 12px;
        text-align: left;
        border-right: 1px solid rgba(255,255,255,0.1);
        vertical-align: bottom;
    }}
    #{table_id} th:last-child {{
        border-right: none;
    }}
    #{table_id} .col-name {{
        font-weight: bold;
        margin-bottom: 4px;
    }}
    #{table_id} .type-badge {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 3px;
        font-size: 10px;
        font-weight: bold;
        color: white;
    }}
    #{table_id} tbody tr {{
        background-color: #FFFFFF;
        color: #2C3E50;
    }}
    #{table_id} tr:nth-child(even) {{
        background-color: {stripe_color};
    }}
    #{table_id} td {{
        padding: 8px 12px;
        border-right: 1px solid #ECF0F1;
        border-bottom: 1px solid #ECF0F1;
        color: #2C3E50;
    }}
    #{table_id} td:last-child {{
        border-right: none;
    }}
    #{table_id} tbody tr:hover {{
        background-color: {hover_color};
    }}
    #{table_id} .footer-row {{
        background-color: #F8F9FA;
        color: #7F8C8D;
        font-style: italic;
        font-size: 12px;
        padding: 8px 16px;
        text-align: left;
        border-top: 2px solid #ECF0F1;
    }}

    /* Contenedor y estilo de botones de descarga */
    #{table_id}-downloads {{
        margin: 0.5em 0 0.2em 0;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 11px;
        color: #7F8C8D;
    }}
    #{table_id}-downloads .download-btn {{
        background: none;
        border: none;
        padding: 0;
        margin-right: 6px;
        font-size: 11px;
        color: #7F8C8D !important;
        text-decoration: none;
        cursor: pointer;
    }}
    #{table_id}-downloads .download-btn:hover {{
        color: #2C3E50 !important;
        text-decoration: underline;
    }}
    </style>
    """
    
    # Encabezado de título
    dim_text = f"- Pseudo-tibble: {n_rows:,} × {n_cols}"
    if title is not None and str(title).strip() != "":
        # Con título: mostramos título principal y subtítulo
        title_html = f"""
        <tr>
            <td colspan="{n_cols}" class="title-row">
                <span class="main-title">{title}</span>
                <span class="subtitle">{dim_text}</span>
                <span class="source-badge">{source_lib}</span>
            </td>
        </tr>
        """
    else:
        # Sin título: comportamiento anterior
        title_html = f"""
        <tr>
            <td colspan="{n_cols}" class="title-row">
                {dim_text}
                <span class="source-badge">{source_lib}</span>
            </td>
        </tr>
        """
    
    # Encabezados de columna con tipos
    headers_html = "<tr>"
    for col in col_names:
        type_str = original_types.get(col, "unknown")
        type_color = get_type_color(type_str)
        
        type_display = type_str.split(".")[-1]
        if len(type_display) > 12:
            type_display = type_display[:10] + "…"
        
        if show_types:
            headers_html += f"""
            <th>
                <div class="col-name">{col}</div>
                <span class="type-badge" style="background-color: {type_color};">
                    &lt;{type_display}&gt;
                </span>
            </th>
            """
        else:
            headers_html += f"<th><div class='col-name'>{col}</div></th>"
    headers_html += "</tr>"
    
    # Filas de datos
    rows_html = ""
    for idx, row in df_pandas.iterrows():
        # Estilo potencial de la fila (por highlight_col)
        row_style = ""
        if highlight_col is not None and highlight_col in df_pandas.columns:
            bg_color = get_row_bg_color(row[highlight_col])
            if bg_color:
                row_style = f' style="background-color: {bg_color};"'

        rows_html += f"<tr{row_style}>"

        for col in col_names:
            val = row[col]

            # --- formateo del contenido (igual que antes) ---
            if pd.isna(val):
                cell_content = "<span style='color:#BDC3C7;'>NA</span>"
            elif isinstance(val, float):
                cell_content = f"{val:.4g}"
            else:
                cell_content = str(val)
                if len(cell_content) > 50:
                    cell_content = cell_content[:47] + "…"

            # --- 🔹 PRIORIDAD: color por COLUMNA ---
            cell_style = ""
            if col in col_color_map:
                # si la columna está marcada, este color manda
                cell_style = f' style="background-color: {col_color_map[col]};"'

            rows_html += f"<td{cell_style}>{cell_content}</td>"

        rows_html += "</tr>"

    
    # Pie de página
    if rows_hidden > 0:
        footer_text = f"- … {rows_hidden:,} filas sin mostrar"
    else:
        footer_text = "- Mostrando todas las filas"
    
    footer_html = f"""
    <tr>
        <td colspan="{n_cols}" class="footer-row">
            {footer_text}
        </td>
    </tr>
    """

    # ------- HTML de la tabla (sin botones) - reutilizable -
    table_html = f"""
    <table id="{table_id}">
        <thead>
            {title_html}
            {headers_html}
        </thead>
        <tbody>
            {rows_html}
        </tbody>
        <tfoot>
            {footer_html}
        </tfoot>
    </table>
    """

    # --------- Botones de descarga ----------

    downloads_html = ""
    if enable_download:
        # CSV
        csv_str = df_pandas.to_csv(index=False)
        b64_csv = base64.b64encode(csv_str.encode("utf-8")).decode("utf-8")
        
        # HTML CON MISMO ESTILO QUE LA VISTA                         # <<<
        # Documento HTML completo (standalone)
        html_document = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="utf-8">
            <title>{filename_base}</title>
            {css}
        </head>
        <body>
            {table_html}
        </body>
        </html>
        """
        b64_html = base64.b64encode(html_document.encode("utf-8")).decode("utf-8")

        # Excel
        excel_buffer = io.BytesIO()
        df_pandas.to_excel(excel_buffer, index=False, engine="openpyxl")
        excel_buffer.seek(0)
        b64_excel = base64.b64encode(excel_buffer.read()).decode("utf-8")

        # Parquet
        try:
            parquet_buffer = io.BytesIO()
            df_parquet = _sanitize_for_parquet(df_pandas)
            df_parquet.to_parquet(parquet_buffer, index=False, engine="pyarrow")
            parquet_buffer.seek(0)
            b64_parquet = base64.b64encode(parquet_buffer.read()).decode("utf-8")
        except Exception as e:
            # fallback: no rompe la tabla completa
            b64_parquet = None
        
        if b64_parquet:
            parquet_btn = f"""
            <a class="download-btn"
            download="{filename_base}.parquet"
            href="data:application/octet-stream;base64,{b64_parquet}">
            Descargar Parquet
            </a>
            """
        else:
            parquet_btn = """
            <span style="color:#E67E22; font-size:11px;">
                Parquet no disponible (tipos mixtos)
            </span>
            """

        downloads_html = f"""
        <div id="{table_id}-downloads">
            <a class="download-btn" 
               download="{filename_base}.csv" 
               href="data:text/csv;base64,{b64_csv}">
               Descargar CSV
            </a>
            <a class="download-btn" 
               download="{filename_base}.xlsx" 
               href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_excel}">
               Descargar Excel
            </a>
            <a class="download-btn" 
               download="{filename_base}.html" 
               href="data:text/html;base64,{b64_html}">
               Descargar HTML
            </a>
            <a class="download-btn" 
               download="{filename_base}.parquet" 
               href="data:application/octet-stream;base64,{b64_parquet}">
               Descargar Parquet
            </a>
        </div>
        """

    # ================================================================
    # Modo estático (comportamiento por defecto)
    # ================================================================
    full_html = f"""
    {css}
    {downloads_html}
    {table_html}
    """
    
    display(HTML(full_html))

####################################################################################################################
########################################## Usar Tabla SQL a PyDF ###################################################
####################################################################################################################

#from __future__ import annotations

#from typing import Any, Iterator, Optional

#import pyodbc


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
########################################## Usar PyDF a SQL #########################################################
####################################################################################################################

#from __future__ import annotations

#from typing import Any, Iterator, Optional, Sequence, Iterable, Literal
#import math


def _qident_sqlserver(name: str) -> str:
    """Quote identificadores para SQL Server con corchetes."""
    return f"[{name.replace(']', ']]')}]"


def _full_table_sqlserver(schema: str, table: str) -> str:
    return f"{_qident_sqlserver(schema)}.{_qident_sqlserver(table)}"


def _is_nan(x: Any) -> bool:
    # Maneja None rápido
    if x is None:
        return False

    # Maneja pandas.NA / pandas.NaT sin booleano ambiguo
    try:
        import pandas as pd  # type: ignore
        if x is pd.NA:
            return True
        # pd.isna maneja NA/NaN/NaT bien
        if pd.isna(x):
            return True
    except Exception:
        pass

    # Fallback general NaN (float)
    try:
        return x != x  # NaN es el único valor donde x != x es True
    except Exception:
        return False


def _normalize_value(x: Any) -> Any:
    """Convierte pd.NA/NaN/NaT -> None y numpy scalars -> python scalars."""
    if x is None:
        return None

    # pandas NA/NaT
    try:
        import pandas as pd  # type: ignore
        if x is pd.NA or pd.isna(x):
            return None
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime()
    except Exception:
        pass

    # numpy scalars (np.int64, np.float64, np.bool_, np.datetime64, etc.)
    try:
        import numpy as np  # type: ignore
        if isinstance(x, np.generic):
            return x.item()
    except Exception:
        pass

    # fallback NaN clásico
    try:
        return None if (x != x) else x
    except Exception:
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

#from dataclasses import dataclass
#from typing import Literal, Tuple, Any, Optional

#import re
#import pyodbc
#from sqlalchemy import create_engine
#from sqlalchemy.engine import Engine
#from sqlalchemy.exc import SQLAlchemyError


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
    #import pyodbc
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
############################### Detección y carga del archivo más reciente Excel ###################################
####################################################################################################################

#from __future__ import annotations

#import time
#import shutil
#from pathlib import Path
#from datetime import datetime

#import pandas as pd


def _norm_sheet_name(s) -> str:
    """Normaliza nombres de hoja: colapsa espacios y pasa a minúsculas."""
    return " ".join(str(s).split()).lower()


def get_latest_file(
    ruta_archivos: Path,
    exts: set[str] | tuple[str, ...] | list[str],
    *,
    recursive: bool = False,
) -> Path:
    """
    Valida que exista una carpeta, filtra archivos por extensión y devuelve el más reciente.

    Args:
        ruta_archivos: Carpeta donde buscar.
        exts: Extensiones válidas (ej: {'.xlsx', '.xlsm'}). Deben incluir el punto.
        recursive: Si True busca recursivamente (rglob). Si False, solo primer nivel.

    Returns:
        Path del archivo más reciente (por mtime).

    Raises:
        AssertionError: si no existe carpeta o si no hay archivos válidos.
    """
    ruta_archivos = Path(ruta_archivos)
    assert ruta_archivos.is_dir(), f"No existe carpeta: {ruta_archivos}"

    exts_norm = {e.lower() if str(e).startswith(".") else f".{str(e).lower()}" for e in exts}

    iterator = ruta_archivos.rglob("*") if recursive else ruta_archivos.iterdir()
    archivos = [p for p in iterator if p.is_file() and p.suffix.lower() in exts_norm]

    archivos.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    assert archivos, "No se encontraron Excel válidos."

    archivo_origen = archivos[0]
    ts = datetime.fromtimestamp(archivo_origen.stat().st_mtime)
    print(f"Archivo más reciente: {archivo_origen.name}  |  {ts:%Y-%m-%d %H:%M:%S}")

    return archivo_origen


def pick_target_sheet(
    archivo_origen: Path,
    preferred_sheets: list[str] | tuple[str, ...] | set[str],
    *,
    fallback_sheet_index: int = 0,
    engine: str = "openpyxl",
    copy_on_permission_error: bool = True,
) -> tuple[str, Path, Path | None, list[str]]:
    """
    Abre un Excel y selecciona hoja objetivo:
      - Si encuentra alguna hoja cuyo nombre normalizado esté en preferred_sheets -> usa esa
      - Si no -> usa la hoja en fallback_sheet_index
      - Si el archivo está bloqueado y copy_on_permission_error=True -> copia temporal y abre esa copia

    Args:
        archivo_origen: Ruta al Excel.
        preferred_sheets: Lista de nombres preferidos (ej: ["Base", "Datos"]).
        fallback_sheet_index: Índice de hoja fallback si no hay match.
        engine: Engine para pd.ExcelFile (por defecto openpyxl).
        copy_on_permission_error: Si True, crea copia temporal cuando el Excel está bloqueado.

    Returns:
        (target_sheet_name, excel_path_used, tmp_copy_path_or_none, sheet_names)

    Raises:
        SystemExit: si no se puede abrir el Excel o no hay hojas.
        IndexError: si fallback_sheet_index está fuera de rango (hojas insuficientes).
    """
    archivo_origen = Path(archivo_origen)
    tmp_copy_path: Path | None = None

    pref_norm = {_norm_sheet_name(n) for n in preferred_sheets}

    def _select_from_sheetnames(sheet_names: list[str]) -> str:
        if not sheet_names:
            raise SystemExit("El Excel no contiene hojas.")

        target = None
        for s in sheet_names:
            if _norm_sheet_name(s) in pref_norm:
                target = s
                break

        if target is None:
            target = sheet_names[fallback_sheet_index]

        return target

    try:
        try:
            with pd.ExcelFile(archivo_origen, engine=engine) as xls:
                sheet_names = list(xls.sheet_names)
                print("Hojas:", sheet_names)
                target = _select_from_sheetnames(sheet_names)
                print(f"Hoja objetivo: {target}")
                return target, archivo_origen, None, sheet_names

        except PermissionError:
            if not copy_on_permission_error:
                raise

            tmp_copy_path = archivo_origen.parent / f"__tmp_copy_{int(time.time()*1000)}{archivo_origen.suffix}"
            shutil.copy2(archivo_origen, tmp_copy_path)
            print(f"No se pudo abrir el archivo original, usando copia temporal: {tmp_copy_path.name}")

            with pd.ExcelFile(tmp_copy_path, engine=engine) as xls:
                sheet_names = list(xls.sheet_names)
                print("Hojas:", sheet_names)
                target = _select_from_sheetnames(sheet_names)
                print(f"Hoja objetivo (copia): {target}")
                return target, tmp_copy_path, tmp_copy_path, sheet_names

    except Exception as e:
        raise SystemExit(f"Error al abrir el Excel: {e}")


def get_latest_excel_and_sheet(
    ruta_archivos: Path,
    exts: set[str] | tuple[str, ...] | list[str],
    preferred_sheets: list[str] | tuple[str, ...] | set[str],
    *,
    fallback_sheet_index: int = 0,
    recursive: bool = False,
    engine: str = "openpyxl",
) -> dict:
    """
    Función orquestadora:
      1) Encuentra el Excel más reciente en ruta_archivos
      2) Determina la hoja objetivo (con preferencia + fallback)
      3) Maneja bloqueo con copia temporal

    Returns:
        dict con:
          - archivo_origen
          - excel_path_used (puede ser copia temporal)
          - tmp_copy_path
          - target_sheet
          - sheet_names
    """
    archivo_origen = get_latest_file(ruta_archivos, exts, recursive=recursive)
    target_sheet, excel_path_used, tmp_copy_path, sheet_names = pick_target_sheet(
        archivo_origen,
        preferred_sheets,
        fallback_sheet_index=fallback_sheet_index,
        engine=engine,
        copy_on_permission_error=True,
    )

    return {
        "archivo_origen": archivo_origen,
        "excel_path_used": excel_path_used,
        "tmp_copy_path": tmp_copy_path,
        "target_sheet": target_sheet,
        "sheet_names": sheet_names,
    }

####################################################################################################################
########################################### Lectura segura de Excel ################################################
####################################################################################################################

#import pandas as pd


def read_excel_safe_no_header(io, sheet_name):
    """
    Lee una hoja de Excel sin encabezados (header=None) usando openpyxl.
    Si falla, detiene la ejecución con un mensaje claro (SystemExit).

    Args:
        io: Ruta al archivo, buffer o file-like que acepte pandas.read_excel.
        sheet_name: Nombre o índice de la hoja a leer.

    Returns:
        pd.DataFrame: DataFrame leído desde Excel sin encabezados.

    Raises:
        SystemExit: Si la hoja no se puede leer por cualquier motivo.
    """
    try:
        return pd.read_excel(io, sheet_name=sheet_name, header=None, engine="openpyxl")
    except Exception as e:
        raise SystemExit(f"No se pudo leer la hoja '{sheet_name}': {e}")


def _is_nullish(v):
    """
    Determina si un valor debe considerarse "vacío" o "nulo" para limpieza.

    Considera vacío:
      - NaN/NaT (pd.isna)
      - strings vacíos o equivalentes: "", "none", "nan" (ignorando espacios y mayúsculas)

    Args:
        v: Valor a evaluar (cualquier tipo).

    Returns:
        bool: True si se considera vacío/nulo, False en caso contrario.
    """
    if pd.isna(v):
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"", "none", "nan"}
    return False


def drop_initial_empty_rows(
    df: pd.DataFrame,
    max_check_rows=2,
    empty_threshold=0.8,
    verbose=True,
    *,
    preview_with_pretty_table: bool = True,
    preview_rows: int = 10,
):
    """
    Elimina filas iniciales (hasta max_check_rows) si tienen >= empty_threshold de celdas vacías.

    "Vacío" se define por _is_nullish (NaN o strings vacíos/none/nan).

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        max_check_rows (int): Número máximo de filas iniciales a evaluar.
        empty_threshold (float): Umbral [0, 1].
        verbose (bool): Imprime mensajes de diagnóstico.
        preview_with_pretty_table (bool): Si True, usa pretty_table para visualizar.
        preview_rows (int): Filas a mostrar en el preview.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - df_limpio
            - df_preview_eliminadas
    """
    # Defensa ligera
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser un pandas.DataFrame")

    if df.empty:
        if verbose:
            print("DataFrame vacío, nada que eliminar.")
        return df, pd.DataFrame()

    # Sanitizar parámetros
    try:
        max_check_rows_int = max(0, int(max_check_rows))
    except Exception:
        max_check_rows_int = 0

    try:
        empty_threshold_float = float(empty_threshold)
    except Exception:
        empty_threshold_float = 0.8

    rows_to_drop = []
    for i in range(min(max_check_rows_int, len(df))):
        row = df.iloc[i]
        n_total = len(row)
        n_empty = sum(_is_nullish(x) for x in row)
        ratio = (n_empty / n_total) if n_total else 1.0

        if ratio >= empty_threshold_float:
            rows_to_drop.append(i)
            if verbose:
                print(f"🧹 Fila inicial {i} eliminada ({ratio:.0%} vacía).")
        else:
            if verbose:
                print(f"Fila inicial {i} conservada ({ratio:.0%} vacía).")

    removed = pd.DataFrame()

    if rows_to_drop:
        removed = df.iloc[rows_to_drop].copy()

        if verbose and not removed.empty:
            print("\nFilas eliminadas (preview):")

            if preview_with_pretty_table:
                # Visualización robusta
                pretty_table(
                    removed,
                    n=preview_rows,
                    title="Filas iniciales eliminadas"#,
                    #enable_download=False,
                )
            else:
                print(removed.to_string(index=True))

        df = df.drop(index=rows_to_drop).reset_index(drop=True)

    else:
        if verbose:
            print("No se eliminaron filas iniciales.")

    return df, removed

#import re
#import unicodedata
#from pathlib import Path
#from datetime import datetime

MESES = {
    "ENERO": "01", "ENE": "01",
    "FEBRERO": "02", "FEB": "02",
    "MARZO": "03", "MAR": "03",
    "ABRIL": "04", "ABR": "04",
    "MAYO": "05", "MAY": "05",
    "JUNIO": "06", "JUN": "06",
    "JULIO": "07", "JUL": "07",
    "AGOSTO": "08", "AGO": "08",
    "SEPTIEMBRE": "09", "SETIEMBRE": "09", "SEP": "09", "SET": "09",
    "OCTUBRE": "10", "OCT": "10",
    "NOVIEMBRE": "11", "NOV": "11",
    "DICIEMBRE": "12", "DIC": "12",
}

def _strip_accents(s: str) -> str:
    """Quita tildes/acentos: 'Márzo' -> 'Marzo'."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _extract_yyyymm_from_name(nombre: str) -> str:
    """
    Intenta extraer YYYYMM desde el nombre del archivo (sin importar la extensión).
    Cubre:
      - YYYYMM
      - YYYY[-_/ .]?MM
      - MM[-_/ .]?YYYY
      - MES(ES) + YYYY (con tildes/abreviaturas) en cualquier orden
    Lanza ValueError si no encuentra período.
    """
    stem = Path(nombre).stem
    stem_norm = _strip_accents(stem).upper()

    # 1) YYYYMM pegado (e.g., 202503)
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", stem_norm)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    # 2) YYYY separador MM (e.g., 2025-03, 2025_03, 2025 03)
    m = re.search(r"(20\d{2})[-_/.\s]?(0[1-9]|1[0-2])", stem_norm)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    # 3) MM separador YYYY (e.g., 03-2025)
    m = re.search(r"(0[1-9]|1[0-2])[-_/.\s]?(20\d{2})", stem_norm)
    if m:
        return f"{m.group(2)}{m.group(1)}"

    # 4) Mes en texto + año (en cualquier orden, pero aquí se busca año y luego mes)
    m_year = re.search(r"(20\d{2})", stem_norm)
    if m_year:
        year = m_year.group(1)
        for mes_txt, mm in MESES.items():
            # Asegura "palabra completa": evita matchear dentro de otras palabras/códigos
            if re.search(rf"(?<![A-Z0-9]){mes_txt}(?![A-Z0-9])", stem_norm):
                return f"{year}{mm}"

    raise ValueError(f"No pude extraer YYYYMM desde el nombre: {nombre}")

def canonicalizar_planes(nombre: str) -> str:
    """
    Devuelve un nombre canónico estandarizado: 'Facturacion_Cesantia_YYYYMM.xlsx'
    Si no logra extraer el período (YYYYMM), devuelve un nombre genérico con aviso.
    """
    try:
        yyyymm = _extract_yyyymm_from_name(nombre)
        return f"Facturacion_Cesantia_{yyyymm}.xlsx"
    except ValueError as e:
        print(
            f"No se pudo extraer período desde el nombre '{nombre}'. "
            f"Se usará un nombre genérico. Detalle: {e}"
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"Facturacion_Cesantia_{timestamp}.xlsx"


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

####################################################################################################################
############################################ Modificaciones al PyDF ###############################################
####################################################################################################################

def make_unique(names):
    seen = {}
    out = []
    for n in names:
        base = n if n else "col"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def _row_nullish_ratio(row: pd.Series, exclude=()):
    cols = [c for c in row.index if c not in exclude]
    if not cols:
        return 1.0
    n_null = sum(_is_nullish(row[c]) for c in cols)
    return n_null / len(cols)


def drop_trailing_mostly_null(
    df: pd.DataFrame,
    null_check_exclude=("Nombre_de_archivo",),
    also_exclude_money_cols=("Prima_Bruta_mensual","IVA","Prima_Neta","Diferencia_CCLA"),
    null_ratio_threshold=0.80,
    verbose=True,
):
    """
    Elimina filas al final del DF mientras tengan un ratio alto de nulos.
    Además MUESTRA las filas eliminadas con todo su contenido.
    """
    if df is None or df.empty:
        if verbose:
            print("DF vacío: nada que hacer.")
        return df

    out = df.copy()
    removed_rows = []   # 👈 aquí guardaremos (index_original, fila_entera)

    exclude = set(null_check_exclude) | set(also_exclude_money_cols)

    i = len(out) - 1
    while i >= 0:
        row = out.iloc[i]
        ratio = _row_nullish_ratio(row, exclude=exclude)

        if verbose:
            print(f"Fila índice {out.index[i]} → null_ratio={ratio:.2%}")

        if ratio >= null_ratio_threshold:
            # Guardar la fila completa ANTES de eliminarla
            removed_rows.append((out.index[i], row.copy()))

            i -= 1
        else:
            break

    # Si no hubo nada para eliminar
    if not removed_rows:
        if verbose:
            print("❎ No se detectaron filas finales mayoritariamente nulas.")
        return out

    # Mostrar filas eliminadas
    if verbose:
        print(f"\n🧹 Eliminando {len(removed_rows)} fila(s) finales mayoritariamente nulas:\n")
        for idx, row in removed_rows:
            print(f"--- Fila eliminada (índice original {idx}) ---")
            print(row.to_frame().T)   # muestra la fila completa en formato bonito
            print("\n")

    # Finalmente eliminar del DF
    drop_indices = [idx for idx, _ in removed_rows]
    out = out.drop(index=drop_indices).reset_index(drop=True)

    return out

def pick(df, *names):
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None]*len(df), index=df.index)

#from __future__ import annotations

#from typing import Iterable, Mapping, Sequence
#import numpy as np
#import pandas as pd

def to_num_series(s: pd.Series) -> pd.Series:
    """
    Convierte una Serie a numérica de forma robusta:
    - Si ya no es object: usa pd.to_numeric directo.
    - Si es object/string: limpia espacios y normaliza vacíos/None->NaN
    - Devuelve float (con NaN) y luego el caller castea a Int64/float64 según corresponda.
    """
    if not pd.api.types.is_object_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    s2 = (
        s.astype(str)
         .str.strip()
         .replace({"": np.nan, "None": np.nan, "none": np.nan, "nan": np.nan, "NaN": np.nan})
    )
    return pd.to_numeric(s2, errors="coerce")


def cast_numeric_columns(
    df: pd.DataFrame,
    *,
    bigint_cols: Iterable[str] = (),
    int_cols: Iterable[str] = (),
    float_cols: Iterable[str] = (),
) -> pd.DataFrame:
    """
    Castea columnas numéricas si existen en df:
    - bigint_cols / int_cols -> pandas 'Int64' (nullable, soporta NaN)
    - float_cols -> numpy 'float64'

    Devuelve el df (mutado) por conveniencia.
    """
    for c in bigint_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("Int64")

    for c in int_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("Int64")

    for c in float_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("float64")

    return df


def normalize_dv_column(df: pd.DataFrame, dv_col: str) -> pd.DataFrame:
    """
    Normaliza DV (dígito verificador) a:
    - tipo string nullable
    - strip, uppercase
    - primer caracter (char(1))
    """
    if dv_col in df.columns:
        df[dv_col] = (
            df[dv_col]
            .astype("string")
            .str.strip()
            .str.upper()
            .map(lambda x: x[:1] if pd.notna(x) and len(x) > 0 else pd.NA)
        )
    return df


def trim_string_columns(
    df: pd.DataFrame,
    limits: Mapping[str, int],
    *,
    strip: bool = True,
) -> pd.DataFrame:
    """
    Para cada columna en `limits` si existe:
    - convierte a string nullable
    - opcionalmente strip
    - corta a largo máximo
    """
    for col, max_len in limits.items():
        if col in df.columns:
            s = df[col].astype("string")
            if strip:
                s = s.str.strip()
            df[col] = s.str.slice(0, int(max_len))
    return df


def report_nulls(df: pd.DataFrame, critical_cols: Sequence[str]) -> None:
    """
    Imprime conteo de nulos en columnas críticas (si existen).
    """
    present = [c for c in critical_cols if c in df.columns]
    if not present:
        return

    print("\n🔎 Nulos en columnas críticas:")
    for c in present:
        print(f" - {c}: {int(df[c].isna().sum())} nulos")


def build_sql_frame(df: pd.DataFrame, cols_sql: Sequence[str]) -> pd.DataFrame:
    """
    Crea un df con las columnas en cols_sql que existan en df, preservando el orden.
    """
    cols_present = [c for c in cols_sql if c in df.columns]
    return df[cols_present].copy()
