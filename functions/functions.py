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
    Prepara un DataFrame para escribirlo en Parquet (usualmente con pyarrow).

    Objetivo:
    - Evitar errores de serialización típicos cuando hay columnas tipo `object`
      que contienen mezclas de tipos (por ejemplo: str y bytes).
    - Normalizar las columnas `object` a un tipo consistente: `string` (dtype de pandas).
    - Convertir valores `bytes`/`bytearray` a texto UTF-8 usando "replace"
      para no romper el proceso si hay bytes inválidos.

    Transformaciones:
    - object -> string (pandas StringDtype)
    - bytes/bytearray -> str (decodificado UTF-8 con reemplazo de caracteres inválidos)
    """

    # 1) Hacemos una copia para NO modificar el DataFrame original (evita side-effects).
    df2 = df.copy()

    # 2) Recorremos todas las columnas del DataFrame copiado.
    for col in df2.columns:

        # 3) Sólo trabajamos con columnas cuyo dtype es "object".
        #    En pandas, "object" suele significar "mezcla de tipos" o strings "a la antigua".
        #    Parquet/pyarrow puede fallar si en una misma columna hay bytes, strings, None, etc.
        if df2[col].dtype == "object":

            # 4) Aplicamos un mapeo elemento a elemento (map) para transformar sólo
            #    los valores que sean bytes/bytearray.
            df2[col] = df2[col].map(
                lambda x: (
                    # 4a) Si el valor es bytes o bytearray, lo decodificamos a texto.
                    #     - "utf-8" es el encoding esperado.
                    #     - "replace" evita levantar excepción en secuencias inválidas,
                    #       sustituyendo caracteres problemáticos por el símbolo de reemplazo.
                    x.decode("utf-8", "replace")
                    if isinstance(x, (bytes, bytearray))
                    # 4b) Si NO es bytes/bytearray, lo dejamos tal cual (por ejemplo str, None, etc.).
                    else x
                )
            # 5) Finalmente, convertimos toda la columna al dtype "string" de pandas.
            #    Esto normaliza la columna para que pyarrow la trate como texto consistente.
            ).astype("string")

    # 6) Retornamos el DataFrame "sanitizado" listo para exportación a Parquet.
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
    # Import "perezoso" (lazy import) de pandas:
    # - Evita requerir pandas como dependencia obligatoria si no se usa.
    # - Reduce el tiempo/costo de import global si esta utilidad vive en un módulo grande.
    # - Permite capturar el error de import en tiempo de ejecución.
    import pandas as pd  # type: ignore

    # Retornamos el módulo importado para poder usarlo localmente
    # (por ejemplo: pd.DataFrame) sin importarlo arriba del archivo.
    return pd


def _import_polars():
    # Import "perezoso" (lazy import) de polars por las mismas razones:
    # - No forzar la dependencia si no se necesita.
    # - Permitir detectar si está instalado mediante try/except.
    import polars as pl  # type: ignore

    # Retornamos el módulo importado para usar pl.DataFrame, etc.
    return pl




def _detect_df_engine(df: Any, prefer: tuple[str, ...] = ("polars", "pandas")) -> str:
    """
    Detecta el engine (biblioteca) del DataFrame recibido.

    Retorna:
        - "pandas"  si df es instancia de pandas.DataFrame
        - "polars"  si df es instancia de polars.DataFrame

    Parámetros:
        - df: objeto a inspeccionar (se espera un DataFrame).
        - prefer: orden de preferencia cuando:
            (a) ambos paquetes están instalados, y
            (b) quieres priorizar un chequeo antes que el otro.
          Nota: si el tipo del objeto es claro, el orden no cambia el resultado;
          sólo cambia qué chequeo se intenta primero.

    Estrategia:
        1) Intentar detección fuerte con isinstance() usando imports protegidos.
        2) Si no se pudo (por falta de librerías o tipos no exactos), usar heurísticas:
            - si tiene to_pandas -> probablemente Polars
            - si tiene iloc e itertuples -> probablemente Pandas
        3) Si nada aplica, error.
    """

    # =========================================================
    # 1) Detección por módulos instalados + isinstance
    # =========================================================
    # Recorremos el orden indicado por `prefer` y probamos cada engine.
    for eng in prefer:
        if eng == "pandas":
            try:
                # Intentamos importar pandas; si no está instalado, cae al except.
                pd = _import_pandas()

                # Detección "fuerte": si es un pandas.DataFrame, devolvemos "pandas".
                if isinstance(df, pd.DataFrame):
                    return "pandas"

            except Exception:
                # Si falla el import o algo raro ocurre, ignoramos y seguimos con el siguiente engine.
                pass

        elif eng == "polars":
            try:
                # Intentamos importar polars; si no está instalado, cae al except.
                pl = _import_polars()

                # Detección "fuerte": si es un polars.DataFrame, devolvemos "polars".
                if isinstance(df, pl.DataFrame):
                    return "polars"

                # Nota: podrías soportar LazyFrame, pero convertirlo puede usar RAM.
                # if isinstance(df, pl.LazyFrame):
                #     return "polars"

            except Exception:
                # Si falla el import o algo raro ocurre, ignoramos y seguimos.
                pass

    # =========================================================
    # 2) Heurísticas de fallback (cuando no pudimos usar isinstance)
    # =========================================================
    # Si tiene método to_pandas, suele ser un DataFrame de Polars
    # (o un objeto compatible que exporta a pandas).
    if hasattr(df, "to_pandas"):
        return "polars"

    # Si tiene iloc e itertuples, suele ser pandas
    # (son APIs muy típicas de pandas DataFrame).
    if hasattr(df, "iloc") and hasattr(df, "itertuples"):
        return "pandas"

    # =========================================================
    # 3) Error final si no se puede detectar
    # =========================================================
    raise TypeError(
        "No se pudo detectar el tipo de df. Esperaba pandas.DataFrame o polars.DataFrame. "
        f"Recibido: {type(df)!r}"
    )


def _iter_rows(cursor: pyodbc.Cursor, chunksize: int) -> Iterator[list[tuple]]:
    """
    Itera los resultados de un cursor pyodbc en bloques (chunks).

    Devuelve:
        Un iterador que produce listas de tuplas, donde cada lista
        representa un batch de filas del tamaño `chunksize`.

    Motivación:
        - Evita cargar todos los resultados en memoria de una sola vez.
        - Convertir a `tuple` reduce overhead frente a `pyodbc.Row`.
        - Rompe la referencia al cursor para facilitar el garbage collection.
    """

    # Bucle infinito: se corta manualmente cuando no hay más filas
    while True:
        # fetchmany trae hasta `chunksize` filas desde el cursor
        batch = cursor.fetchmany(chunksize)

        # Si el batch está vacío, no hay más resultados -> salir
        if not batch:
            break

        # Convertimos cada fila a tuple:
        # - pyodbc.Row mantiene referencia al cursor
        # - tuple es más liviano y seguro para downstream
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
    Ejecuta una query SQL usando pyodbc y devuelve los resultados
    como DataFrame (Pandas o Polars), o como iterador de DataFrames.

    Parámetros:
        - sql: query SQL a ejecutar.
        - connection_string: string de conexión ODBC.
        - engine:
            * "auto"   -> detecta automáticamente (pandas / polars)
            * "pandas" -> fuerza Pandas
            * "polars" -> fuerza Polars
        - chunksize: número de filas por batch (controla uso de RAM).
        - return_iter:
            * False (default): devuelve un DataFrame completo
            * True: devuelve un iterador de DataFrames (streaming)

    Retorna:
        - pandas.DataFrame
        - polars.DataFrame
        - o Iterator[pandas.DataFrame | polars.DataFrame]
    """

    # Normalizamos el engine (robustez ante None, mayúsculas, espacios)
    engine = (engine or "auto").lower().strip()

    # Si engine es automático, se detecta el motor preferido
    # (normalmente según lo que esté instalado o configurado)
    if engine == "auto":
        engine = _detect_df_engine()

    # =========================================================
    # 1) Conexión a la base de datos
    # =========================================================
    # Se usan context managers para asegurar:
    # - cierre correcto de conexión
    # - cierre correcto del cursor
    # incluso si ocurre una excepción.
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:

            # Ejecutamos la query SQL
            cursor.execute(sql)

            # Extraemos los nombres de las columnas desde el cursor
            columns = (
                [c[0] for c in cursor.description]
                if cursor.description
                else []
            )

            # =================================================
            # 2) Caso: query sin resultados (DDL, INSERT, etc.)
            # =================================================
            if not columns:
                if engine == "pandas":
                    pd = _import_pandas()
                    return pd.DataFrame()

                if engine == "polars":
                    pl = _import_polars()
                    return pl.DataFrame()

                # Fallback genérico
                return []

            # =================================================
            # 3) Modo iterador (streaming, bajo consumo de RAM)
            # =================================================
            if return_iter:
                if engine == "pandas":
                    pd = _import_pandas()

                    # Definimos un generador que produce DataFrames Pandas
                    def _it() -> Iterator[Any]:
                        for data in _iter_rows(cursor, chunksize):
                            yield pd.DataFrame.from_records(
                                data, columns=columns
                            )

                    return _it()

                elif engine == "polars":
                    pl = _import_polars()

                    # Generador equivalente para Polars
                    def _it() -> Iterator[Any]:
                        for data in _iter_rows(cursor, chunksize):
                            yield pl.DataFrame(
                                data, schema=columns
                            )

                    return _it()

                else:
                    raise ValueError(
                        "engine inválido. Usa 'auto', 'polars' o 'pandas'."
                    )

            # =================================================
            # 4) Modo DataFrame completo (concatena chunks)
            # =================================================
            # Más simple de usar, pero potencialmente más costoso en RAM.
            if engine == "pandas":
                pd = _import_pandas()

                frames = []
                for data in _iter_rows(cursor, chunksize):
                    frames.append(
                        pd.DataFrame.from_records(
                            data, columns=columns
                        )
                    )

                # Concatenamos todos los DataFrames parciales
                # ignore_index=True para índices limpios
                return (
                    pd.concat(frames, ignore_index=True)
                    if frames
                    else pd.DataFrame(columns=columns)
                )

            elif engine == "polars":
                pl = _import_polars()

                frames = []
                for data in _iter_rows(cursor, chunksize):
                    frames.append(
                        pl.DataFrame(data, schema=columns)
                    )

                # Concatenación vertical en Polars
                return (
                    pl.concat(frames, how="vertical")
                    if frames
                    else pl.DataFrame(schema=columns)
                )

            else:
                raise ValueError(
                    "engine inválido. Usa 'auto', 'polars' o 'pandas'."
                )

            

####################################################################################################################
########################################## Usar PyDF a SQL #########################################################
####################################################################################################################

#from __future__ import annotations

#from typing import Any, Iterator, Optional, Sequence, Iterable, Literal
#import math


def _qident_sqlserver(name: str) -> str:
    """
    Escapa (quotea) un identificador SQL para SQL Server usando corchetes [].

    Uso típico:
        - nombres de columnas
        - nombres de tablas
        - nombres de esquemas

    Motivo:
        - Evita errores cuando el identificador:
            * contiene espacios
            * coincide con palabras reservadas
            * contiene caracteres especiales
        - Previene inyección accidental vía identificadores
          (no valores; esto NO reemplaza parámetros SQL).
    """

    # SQL Server escapa ']' duplicándolo: ']' -> ']]'
    # Luego se envuelve todo el identificador entre corchetes.
    #
    # Ejemplo:
    #   name = "col]name"
    #   -> "[col]]name]"
    return f"[{name.replace(']', ']]')}]"


def _full_table_sqlserver(schema: str, table: str) -> str:
    """
    Construye un nombre de tabla totalmente calificado para SQL Server.

    Retorna:
        [schema].[table]

    Ventajas:
        - Evita ambigüedad entre esquemas.
        - Funciona incluso si los nombres contienen caracteres especiales.
        - Centraliza la lógica de quoting.
    """

    # Se reutiliza _qident_sqlserver para asegurar escape correcto
    # tanto del esquema como del nombre de la tabla.
    return f"{_qident_sqlserver(schema)}.{_qident_sqlserver(table)}"


def _is_nan(x: Any) -> bool:
    """
    Determina si un valor debe considerarse "NaN / NA / NaT".

    Retorna:
        True  -> el valor representa ausencia de dato
        False -> el valor es un valor válido (incluye None)

    Maneja correctamente:
        - pandas.NA
        - pandas.NaT
        - numpy.nan
        - float('nan')
        - valores escalares comunes

    Importante:
        - None NO se considera NaN aquí (decisión explícita).
        - Evita errores de "booleano ambiguo" de pandas.
    """

    # =========================================================
    # 1) Caso rápido: None
    # =========================================================
    # None se trata como "valor válido" en este helper,
    # porque en muchos contextos se maneja distinto a NaN.
    if x is None:
        return False

    # =========================================================
    # 2) Manejo específico de pandas (si está disponible)
    # =========================================================
    try:
        import pandas as pd  # type: ignore

        # pandas.NA NO puede evaluarse directamente como boolean
        if x is pd.NA:
            return True

        # pd.isna maneja correctamente:
        # - NaN
        # - NA
        # - NaT
        # - numpy.nan
        if pd.isna(x):
            return True

    except Exception:
        # Si pandas no está instalado o algo falla,
        # seguimos con el fallback genérico.
        pass

    # =========================================================
    # 3) Fallback genérico para NaN (float)
    # =========================================================
    try:
        # NaN es el único valor en Python donde:
        #   x != x  -> True
        return x != x
    except Exception:
        # Si el objeto no soporta comparación,
        # asumimos que NO es NaN.
        return False


def _normalize_value(x: Any) -> Any:
    """
    Normaliza un valor individual para uso en SQL / DB-API.

    Reglas de normalización:
        - pandas.NA / NaN / NaT        -> None
        - numpy scalars (np.int64, etc.) -> tipos nativos de Python
        - pandas.Timestamp            -> datetime.datetime
        - None                         -> None (se mantiene)

    Objetivo:
        - Evitar errores de pyodbc / DB-API al hacer executemany.
        - Evitar boolean ambiguity de pandas (pd.NA).
        - Asegurar compatibilidad con drivers ODBC.
    """

    # =========================================================
    # 1) Caso trivial: None
    # =========================================================
    # None ya es el valor esperado por DB-API para representar NULL.
    if x is None:
        return None

    # =========================================================
    # 2) Manejo específico de pandas (si está disponible)
    # =========================================================
    try:
        import pandas as pd  # type: ignore

        # pandas.NA no puede evaluarse como boolean
        # y debe convertirse explícitamente a None.
        if x is pd.NA:
            return None

        # pd.isna maneja correctamente:
        # - NaN
        # - NA
        # - NaT
        if pd.isna(x):
            return None

        # pandas.Timestamp no siempre es aceptado por drivers ODBC;
        # lo convertimos a datetime.datetime estándar.
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime()

    except Exception:
        # Si pandas no está instalado o falla algo,
        # seguimos con otras estrategias.
        pass

    # =========================================================
    # 3) Manejo de numpy scalars
    # =========================================================
    try:
        import numpy as np  # type: ignore

        # numpy usa tipos escalares propios (np.int64, np.float64, etc.)
        # .item() devuelve el valor equivalente en Python nativo.
        if isinstance(x, np.generic):
            return x.item()

    except Exception:
        # numpy no está instalado o x no es numpy scalar
        pass

    # =========================================================
    # 4) Fallback genérico para NaN clásico
    # =========================================================
    try:
        # NaN es el único valor tal que:
        #   x != x  -> True
        return None if (x != x) else x
    except Exception:
        # Si no se puede comparar, devolvemos el valor original
        return x


def _iter_rows_from_df(
    df: Any,
    engine: str,
    chunksize: int,
    columns: Sequence[str],
) -> Iterator[list[tuple]]:
    """
    Itera un DataFrame en bloques (chunks) y devuelve listas de tuplas,
    listas para ser usadas directamente en cursor.executemany().

    Características:
        - Soporta Pandas y Polars.
        - Iteración por chunks para controlar uso de memoria.
        - Normaliza valores (NaN -> None, numpy -> python).
        - Produce estructuras simples: list[tuple].

    Retorna:
        Un iterador que produce batches de filas:
            [
              (v11, v12, ...),
              (v21, v22, ...),
              ...
            ]
    """

    # =========================================================
    # 1) Implementación para Pandas
    # =========================================================
    if engine == "pandas":
        pd = _import_pandas()

        # Número total de filas
        n = len(df)

        # Iteramos por ventanas [start : start + chunksize]
        for start in range(0, n, chunksize):
            chunk = df.iloc[start:start + chunksize]

            # itertuples(index=False, name=None):
            # - Más rápido que iterrows
            # - Devuelve tuplas puras
            rows = []
            for r in chunk.itertuples(index=False, name=None):
                # Normalizamos cada valor de la fila
                rows.append(
                    tuple(_normalize_value(v) for v in r)
                )

            yield rows

    # =========================================================
    # 2) Implementación para Polars
    # =========================================================
    elif engine == "polars":
        # Polars es columnar, por lo que:
        # - slice es barato
        # - iter_rows es la forma correcta de iterar fila a fila
        n = df.height

        for start in range(0, n, chunksize):
            chunk = df.slice(start, chunksize)

            rows = []
            for r in chunk.iter_rows(named=False):
                rows.append(
                    tuple(_normalize_value(v) for v in r)
                )

            yield rows

    # =========================================================
    # 3) Engine no soportado
    # =========================================================
    else:
        raise ValueError("engine inválido. Usa pandas o polars.")


def _build_insert_sql_sqlserver(
    schema: str,
    table: str,
    columns: Sequence[str],
) -> str:
    """
    Construye una sentencia INSERT parametrizada para SQL Server.

    Ejemplo de salida:
        INSERT INTO [dbo].[mi_tabla] ([col1], [col2])
        VALUES (?, ?)

    Características:
        - Escapa correctamente esquema, tabla y columnas.
        - Usa placeholders '?' compatibles con pyodbc.
        - Evita inyección SQL en identificadores.
        - Lista para usar con cursor.executemany().
    """

    # =========================================================
    # 1) Nombre de tabla completamente calificado
    # =========================================================
    # Usa [schema].[table], escapado con corchetes
    full = _full_table_sqlserver(schema, table)

    # =========================================================
    # 2) Lista de columnas escapadas
    # =========================================================
    # Cada columna se pasa por _qident_sqlserver para:
    # - manejar palabras reservadas
    # - soportar espacios o caracteres especiales
    #
    # Resultado ejemplo:
    #   [col1], [col2], [fecha_creación]
    cols = ", ".join(_qident_sqlserver(c) for c in columns)

    # =========================================================
    # 3) Placeholders de parámetros
    # =========================================================
    # SQL Server vía ODBC usa '?' como placeholder posicional.
    # Debe haber exactamente uno por columna.
    params = ", ".join("?" for _ in columns)

    # =========================================================
    # 4) Ensamblado final del SQL
    # =========================================================
    return f"INSERT INTO {full} ({cols}) VALUES ({params})"


def _build_delete_in_sql_sqlserver(
    schema: str,
    table: str,
    col: str,
    n_params: int,
) -> str:
    """
    Construye una sentencia DELETE con cláusula IN parametrizada
    para SQL Server.

    Ejemplo de salida (n_params=3):
        DELETE FROM [dbo].[mi_tabla]
        WHERE [id] IN (?, ?, ?)

    Uso típico:
        - Borrado masivo por lista de IDs
        - Uso con cursor.execute(sql, params)
    """

    # =========================================================
    # 1) Nombre de tabla completamente calificado
    # =========================================================
    full = _full_table_sqlserver(schema, table)

    # =========================================================
    # 2) Placeholders para la cláusula IN
    # =========================================================
    # Se generan tantos '?' como valores tenga la lista.
    #
    # IMPORTANTE:
    #   - SQL Server NO acepta un solo '?' para una lista.
    #   - Cada valor debe tener su propio placeholder.
    placeholders = ", ".join("?" for _ in range(n_params))

    # =========================================================
    # 3) Ensamblado final del SQL
    # =========================================================
    # La columna también se escapa para evitar problemas
    # con nombres reservados o caracteres especiales.
    return (
        f"DELETE FROM {full} "
        f"WHERE {_qident_sqlserver(col)} IN ({placeholders})"
    )


def _build_merge_sql_sqlserver(
    schema: str,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    stage_table: str = "#stage",
) -> str:
    """
    Construye una sentencia MERGE para SQL Server.

    Comportamiento del MERGE generado:
        - MATCHED (cuando la fila ya existe):
            * UPDATE de todas las columnas que NO son clave
        - NOT MATCHED BY TARGET (cuando no existe):
            * INSERT de todas las columnas

    Suposiciones:
        - Existe una tabla staging (por defecto #stage) con las mismas columnas.
        - key_columns identifica de forma única una fila.
        - columns incluye tanto claves como no-claves.
    """

    # =========================================================
    # 1) Nombre completo de la tabla destino
    # =========================================================
    # Genera: [schema].[table], escapado correctamente
    full = _full_table_sqlserver(schema, table)

    # =========================================================
    # 2) Columnas y claves escapadas
    # =========================================================
    # Todas las columnas (para INSERT)
    cols_q = [_qident_sqlserver(c) for c in columns]

    # Columnas clave (para el ON del MERGE)
    keys_q = [_qident_sqlserver(k) for k in key_columns]

    # =========================================================
    # 3) Cláusula ON (match entre tabla destino y staging)
    # =========================================================
    # Ejemplo:
    #   ON T.[id] = S.[id] AND T.[fecha] = S.[fecha]
    on_clause = " AND ".join(
        [f"T.{k} = S.{k}" for k in keys_q]
    )

    # =========================================================
    # 4) Construcción del UPDATE (WHEN MATCHED)
    # =========================================================
    # Sólo se actualizan columnas NO-clave
    non_keys = [c for c in columns if c not in set(key_columns)]

    if non_keys:
        # SET T.[col] = S.[col], ...
        set_clause = ", ".join(
            [
                f"T.{_qident_sqlserver(c)} = S.{_qident_sqlserver(c)}"
                for c in non_keys
            ]
        )

        when_matched = (
            f"WHEN MATCHED THEN UPDATE SET {set_clause}"
        )
    else:
        # Si todas las columnas son clave:
        # - no tiene sentido actualizar
        # - omitimos WHEN MATCHED completamente
        when_matched = ""

    # =========================================================
    # 5) Construcción del INSERT (WHEN NOT MATCHED)
    # =========================================================
    # Columnas destino
    insert_cols = ", ".join(cols_q)

    # Valores desde la tabla staging (prefijo S.)
    insert_vals = ", ".join([f"S.{c}" for c in cols_q])

    # =========================================================
    # 6) Ensamblado final del MERGE
    # =========================================================
    merge_sql = f"""
    MERGE INTO {full} AS T
    USING {stage_table} AS S
    ON {on_clause} 
    {when_matched}
    WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    # strip() elimina saltos de línea iniciales/finales
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
    partition_batch: int = 900,  # límite práctico para IN params en SQL Server (<=2100)
    # upsert
    key_columns: Optional[Sequence[str]] = None,
    # perf/tx
    commit_every_chunk: bool = False,
) -> dict[str, Any]:
    """
    Carga un DataFrame hacia SQL Server (vía pyodbc) con soporte de chunking y varios modos.

    Modos soportados:
    - append:
        Inserta todas las filas del DataFrame en la tabla destino.
    - truncate_append:
        Ejecuta TRUNCATE TABLE (borra todo) y luego inserta todas las filas.
    - replace_partition:
        Borra filas existentes donde partition_column IN (partition_values) y luego inserta
        (típico para "reprocesar" particiones específicas).
        Si partition_values no se entrega, se calcula desde el df (valores únicos).
        Se hace batching para no exceder el límite de parámetros de SQL Server (2100).
    - upsert:
        Implementa "UPSERT" (update si existe, insert si no existe) usando:
            1) tabla temporal #stage con la misma estructura que destino
            2) inserción masiva al stage por chunks
            3) MERGE de #stage -> destino basado en key_columns
            4) drop del stage

    Parámetros clave:
    - engine: "pandas" | "polars" | "auto"
        Controla cómo iterar el DF y cómo extraer columnas.
    - chunksize:
        Cantidad de filas por lote para reducir uso de memoria y mejorar performance en executemany.
    - commit_every_chunk:
        Si True, hace COMMIT por cada chunk insertado.
        Útil para cargas enormes o para reducir tamaño de transacción,
        pero puede afectar performance y atomicidad.

    Retorna:
        Un resumen (dict) con contadores y notas:
            - rows_inserted / rows_deleted / rows_staged
            - configuración efectiva y mensajes de ejecución
    """

    # =========================================================
    # 1) Determinar engine (pandas/polars) y normalizar string
    # =========================================================
    # Si engine es "auto", se detecta a partir del tipo real del df.
    if engine == "auto":
        engine = _detect_df_engine(df)

    # Normalización defensiva: minúsculas y sin espacios
    engine = engine.lower().strip()

    # =========================================================
    # 2) Determinar columnas a usar
    # =========================================================
    # Si no vienen columnas explícitas:
    # - se asume que insertaremos todas las columnas del df en su orden actual.
    # Si el engine no es válido, levantamos error.
    if columns is None:
        # Extrae columnas del DF según engine
        if engine in ("pandas", "polars"):
            columns = list(df.columns)
        else:
            raise ValueError("engine inválido. Usa pandas o polars.")
    else:
        # Si el usuario pasa columnas, se fuerza a lista
        # (para poder iterar varias veces de forma segura).
        columns = list(columns)

    # =========================================================
    # 3) Objeto summary (resultado final)
    # =========================================================
    # Se inicializa un resumen con configuración y contadores.
    summary = {
        "engine": engine,
        "mode": mode,
        "schema": schema,
        "table": table,
        "columns": list(columns),
        "chunksize": chunksize,
        "rows_inserted": 0,  # filas insertadas en destino (no aplica en upsert)
        "rows_deleted": 0,   # filas borradas (replace_partition)
        "rows_staged": 0,    # filas cargadas a staging (upsert)
        "notes": [],         # mensajes para auditoría / debugging
    }

    # =========================================================
    # 4) SQL base para INSERT en tabla destino
    # =========================================================
    # Genera: INSERT INTO [schema].[table] ([c1],[c2],...) VALUES (?,?,...)
    insert_sql = _build_insert_sql_sqlserver(schema, table, columns)

    # =========================================================
    # 5) Conexión manual para controlar transacciones
    # =========================================================
    # Usamos conexión "manual" (no with) porque queremos:
    # - autocommit = False
    # - commit/rollback explícitos
    # - poder hacer múltiples statements en la misma transacción
    conn = pyodbc.connect(connection_string)
    try:
        # Desactiva autocommit para controlar atomicidad
        conn.autocommit = False

        # Cursor de trabajo
        cursor = conn.cursor()

        # fast_executemany acelera ejecutemany con pyodbc (muy útil en inserts masivos)
        cursor.fast_executemany = True

        # Nombre fully qualified y escapado: [schema].[table]
        full = _full_table_sqlserver(schema, table)

        # =====================================================
        # 6) TRUNCATE si corresponde
        # =====================================================
        # En truncate_append, se elimina todo el contenido de la tabla (más rápido que DELETE)
        if mode == "truncate_append":
            cursor.execute(f"TRUNCATE TABLE {full}")
            summary["notes"].append("TRUNCATE ejecutado.")

        # =====================================================
        # 7) replace_partition: borrar particiones antes de insertar
        # =====================================================
        # Patrón típico: "reemplazar" sólo ciertos cortes de datos (mes, día, región, etc.)
        if mode == "replace_partition":
            # Validación: requiere columna de partición
            if not partition_column:
                raise ValueError("mode='replace_partition' requiere partition_column.")

            # Si no se entregan valores de partición, se derivan del df:
            # - pandas: df[col].dropna().unique()
            # - polars: select/unique
            if partition_values is None:
                if engine == "pandas":
                    partition_values = list(df[partition_column].dropna().unique())
                elif engine == "polars":
                    partition_values = (
                        df.select(partition_column).unique().to_series().to_list()
                    )
                else:
                    partition_values = []

            # Copiamos a lista para poder hacer slicing por lotes
            vals = list(partition_values)

            if vals:
                # SQL Server tiene un límite duro de 2100 parámetros por statement.
                # Se borra en lotes (partition_batch) para no reventar ese límite.
                for i in range(0, len(vals), partition_batch):
                    batch = vals[i:i + partition_batch]

                    # Genera: DELETE FROM [schema].[table] WHERE [partition_column] IN (?,?,...)
                    del_sql = _build_delete_in_sql_sqlserver(
                        schema, table, partition_column, len(batch)
                    )

                    # Ejecuta con parámetros (seguro y eficiente)
                    cursor.execute(del_sql, batch)

                    # rowcount puede ser -1 dependiendo del driver/setting,
                    # por eso se suma sólo si es un número válido.
                    summary["rows_deleted"] += cursor.rowcount if cursor.rowcount != -1 else 0

                summary["notes"].append(
                    f"DELETE por partición {partition_column} aplicado a {len(vals)} valores."
                )
            else:
                summary["notes"].append(
                    "No hubo valores de partición para borrar (lista vacía)."
                )

        # =====================================================
        # 8) upsert: staging + MERGE
        # =====================================================
        if mode == "upsert":
            # Validación: para upsert necesitamos claves
            if not key_columns:
                raise ValueError(
                    "mode='upsert' requiere key_columns (lista de columnas clave)."
                )
            key_columns = list(key_columns)

            # -------------------------------------------------
            # 8.1) Preparar tabla temporal #stage
            # -------------------------------------------------
            # Limpieza defensiva: si existe, se elimina
            cursor.execute(
                "IF OBJECT_ID('tempdb..#stage') IS NOT NULL DROP TABLE #stage;"
            )

            # Crea #stage con estructura idéntica a destino:
            # SELECT TOP 0 * INTO #stage FROM [schema].[table]
            cursor.execute(f"SELECT TOP 0 * INTO #stage FROM {full};")

            # -------------------------------------------------
            # 8.2) INSERT parametrizado hacia #stage
            # -------------------------------------------------
            # Para #stage NO se usa schema (es tabla temporal).
            cols = ", ".join(_qident_sqlserver(c) for c in columns)
            params = ", ".join("?" for _ in columns)
            stage_insert_sql = f"INSERT INTO #stage ({cols}) VALUES ({params})"

            # -------------------------------------------------
            # 8.3) Cargar el DF en #stage en chunks
            # -------------------------------------------------
            # _iter_rows_from_df:
            # - itera por chunks
            # - normaliza NaN/NA/NaT -> None
            # - convierte numpy/pandas scalars a tipos Python
            for rows in _iter_rows_from_df(df, engine, chunksize, columns):
                if not rows:
                    continue

                cursor.executemany(stage_insert_sql, rows)
                summary["rows_staged"] += len(rows)

                # Si se quiere commit por chunk, se hace aquí.
                # Ojo: esto reduce atomicidad (parte puede quedar cargada aunque luego falle algo).
                if commit_every_chunk:
                    conn.commit()

            # -------------------------------------------------
            # 8.4) MERGE #stage -> destino (UPSERT)
            # -------------------------------------------------
            merge_sql = _build_merge_sql_sqlserver(
                schema,
                table,
                columns,
                key_columns,
                stage_table="#stage"
            )
            cursor.execute(merge_sql)

            # -------------------------------------------------
            # 8.5) Limpieza de staging
            # -------------------------------------------------
            cursor.execute("DROP TABLE #stage;")

        else:
            # =================================================
            # 9) Modo insert normal:
            #    append / truncate_append / replace_partition
            # =================================================
            # Inserta por chunks en tabla destino usando fast_executemany
            for rows in _iter_rows_from_df(df, engine, chunksize, columns):
                if not rows:
                    continue

                cursor.executemany(insert_sql, rows)
                summary["rows_inserted"] += len(rows)

                # Commit por chunk (opcional)
                if commit_every_chunk:
                    conn.commit()

        # =====================================================
        # 10) Commit final (si no hubo commit por chunk, este es el principal)
        # =====================================================
        conn.commit()
        return summary

    except Exception as e:
        # =====================================================
        # 11) Rollback si ocurre cualquier error
        # =====================================================
        conn.rollback()

        # Guardamos nota diagnóstica en el summary
        summary["notes"].append(f"ROLLBACK por error: {e!r}")

        # Re-lanzamos la excepción para que el caller la maneje
        raise

    finally:
        # =====================================================
        # 12) Cierre defensivo de conexión
        # =====================================================
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


# ============================================================
# 1) Política de manejo de errores
# ============================================================
# Define cómo debe reaccionar el caller ante una falla:
# - "warn"   : registrar advertencia, pero continuar
# - "silent" : ignorar silenciosamente
# - "raise"  : relanzar la excepción
OnFail = Literal["warn", "silent", "raise"]


# ============================================================
# 2) Resultado estructurado de diagnóstico SQL Server
# ============================================================
@dataclass
class SqlServerDiagnostics:
    """
    Estructura estándar para reportar diagnósticos de conexión
    o ejecución contra SQL Server.

    Se usa típicamente para:
        - health checks
        - validación de credenciales
        - troubleshooting de drivers ODBC / SQLAlchemy
        - logging estructurado
    """

    # Indica si la operación fue exitosa o no
    ok: bool

    # Etapa donde ocurrió el resultado o el error:
    #   - "connect"             -> conexión ODBC
    #   - "query"               -> ejecución de SQL
    #   - "sqlalchemy_engine"   -> creación de engine SQLAlchemy
    stage: str

    # Connection string ODBC utilizada (normalmente sanitizada)
    odbc_connection_string: str

    # URL SQLAlchemy equivalente (útil para debugging)
    sqlalchemy_url: str

    # SQLSTATE estándar (ej: '08001', '28000', 'HYT00')
    sqlstate: str | None = None

    # Código de error nativo del driver / SQL Server (si existe)
    native_code: int | None = None

    # Mensaje completo de error (normalizado)
    message: str | None = None

    # Hint amigable para el usuario (ej: "verifique credenciales")
    hint: str | None = None

    # Tipo de excepción capturada (ej: 'pyodbc.Error')
    exception_type: str | None = None


# ============================================================
# 3) Parser de errores pyodbc
# ============================================================
def _parse_pyodbc_error(e: pyodbc.Error) -> Tuple[str | None, int | None, str]:
    """
    Extrae información estructurada desde una excepción pyodbc.Error.

    Intenta identificar:
        - SQLSTATE      : código estándar ODBC (5 caracteres)
        - native_code  : código nativo del driver o SQL Server
        - mensaje      : texto completo del error

    Motivación:
        - pyodbc devuelve errores con formatos inconsistentes
        - e.args puede variar entre drivers y versiones
        - esta función normaliza el error para logging y diagnóstico
    """

    # =========================================================
    # 1) Construcción del mensaje completo
    # =========================================================
    # e.args puede contener:
    #   - tuplas: ('08001', '[08001] ...')
    #   - strings sueltos
    #   - combinaciones de ambos
    #
    # Se concatena todo en un solo string legible.
    msg = " | ".join(
        str(a) for a in getattr(e, "args", [str(e)])
    )

    sqlstate = None
    native_code = None

    # =========================================================
    # 2) Caso típico: SQLSTATE como primer argumento
    # =========================================================
    # Ejemplo común:
    #   e.args = ('08001', '[08001] TCP Provider...')
    if getattr(e, "args", None):
        first = e.args[0]

        # SQLSTATE válido: exactamente 5 caracteres alfanuméricos
        if isinstance(first, str) and re.fullmatch(r"[0-9A-Z]{5}", first):
            sqlstate = first

    # =========================================================
    # 3) Búsqueda de SQLSTATE dentro del texto
    # =========================================================
    # Algunos drivers incluyen el SQLSTATE embebido en el mensaje,
    # por ejemplo:
    #   "SQLSTATE=28000"
    #   "[HYT00] Login timeout expired"
    if not sqlstate:
        # Formato explícito: SQLSTATE=XXXXX
        m = re.search(r"\bSQLSTATE=([0-9A-Z]{5})\b", msg)
        if m:
            sqlstate = m.group(1)
        else:
            # Formato entre corchetes: [XXXXX]
            m = re.search(r"\[([0-9A-Z]{5})\]", msg)
            if m:
                sqlstate = m.group(1)

    # =========================================================
    # 4) Búsqueda de código de error nativo
    # =========================================================
    # El código nativo depende del driver (ODBC, FreeTDS, MS SQL, etc.)
    # Ejemplo:
    #   "NativeError = 18456"
    m = re.search(
        r"\bNativeError\s*=\s*(-?\d+)\b",
        msg,
        re.IGNORECASE
    )
    if m:
        native_code = int(m.group(1))

    # =========================================================
    # 5) Retorno normalizado
    # =========================================================
    return sqlstate, native_code, msg


def _diagnose(sqlstate: str | None, message: str) -> str:
    """
    Traduce un error de conexión/ejecución SQL Server a una
    causa probable + acciones recomendadas.

    Entradas:
        - sqlstate:
            Código SQLSTATE ODBC (si está disponible), por ejemplo:
            '08001', '28000', 'HYT00', etc.
        - message:
            Mensaje completo del error (texto libre del driver).

    Salida:
        - Un string "humano" con:
            * causa más probable
            * qué revisar / cómo mitigarlo

    Nota:
        - Es una heurística basada en patrones comunes.
        - No es perfecta, pero cubre la gran mayoría de errores reales
          vistos en SQL Server + pyodbc.
    """

    # =========================================================
    # 1) Normalización del mensaje
    # =========================================================
    # Se pasa a minúsculas para facilitar búsquedas por substring
    # sin preocuparnos por mayúsculas/minúsculas del driver.
    m = message.lower()

    # =========================================================
    # 2) Problemas de driver ODBC / librerías
    # =========================================================
    # Casos típicos:
    # - driver no instalado
    # - nombre del driver mal escrito
    # - error del Driver Manager
    if (
        "data source name not found" in m
        or "can't open lib" in m
        or "driver manager" in m
    ):
        return (
            "Driver ODBC no encontrado o nombre incorrecto. "
            "Verifica el parámetro driver (ej: 'ODBC Driver 17 for SQL Server' o 18) "
            "y que esté instalado en la máquina."
        )

    # =========================================================
    # 3) TLS / SSL / Certificados / Encrypt
    # =========================================================
    # Muy común desde ODBC Driver 18 (Encrypt=yes por defecto).
    # Incluye:
    # - certificados no confiables
    # - errores de handshake TLS
    if (
        "certificate" in m
        or "ssl provider" in m
        or "encryption" in m
        or "tls" in m
    ):
        return (
            "Problema TLS/Certificado al negociar cifrado. "
            "Prueba agregando 'Encrypt=yes;TrustServerCertificate=yes;' (temporal) "
            "o instala la CA/cert correcto. Con Driver 18 suele requerirse Encrypt."
        )

    # =========================================================
    # 4) SSPI / Kerberos / Trusted Connection
    # =========================================================
    # Errores típicos en autenticación integrada (Windows/AD):
    # - SPN mal configurado
    # - reloj desincronizado
    # - VPN / dominio
    if (
        "sspi" in m
        or "kerberos" in m
        or "cannot generate sspi context" in m
    ):
        return (
            "Fallo de autenticación integrada (SSPI/Kerberos). "
            "Revisa dominio/SPN, hora del equipo, VPN, y que el server soporte AD. "
            "Como workaround, prueba SQL Auth (usuario/clave) o configura SPN."
        )

    # =========================================================
    # 5) Timeouts (conexión o ejecución)
    # =========================================================
    # SQLSTATE típicos:
    # - HYT00: timeout
    # - HYT01: timeout extendido
    if sqlstate in ("HYT00", "HYT01") or "timeout" in m:
        return (
            "Timeout al conectar/ejecutar. Revisa latencia, VPN, firewall, "
            "puerto 1433, y considera aumentar 'timeout' en la conexión."
        )

    # =========================================================
    # 6) Login failed / credenciales
    # =========================================================
    # SQLSTATE 28000 es estándar para error de autenticación.
    if sqlstate == "28000" or "login failed" in m:
        return (
            "Credenciales inválidas o sin permisos. "
            "Si usas Trusted_Connection, verifica que tu usuario tenga acceso. "
            "Si usas SQL Auth, revisa usuario/clave y que SQL Server esté en modo mixto."
        )

    # =========================================================
    # 7) Servidor no accesible / red / instancia
    # =========================================================
    # Incluye:
    # - DNS incorrecto
    # - instancia nombrada mal escrita
    # - SQL Browser apagado
    # - firewall bloqueando 1433
    if (
        sqlstate == "08001"
        or "server was not found" in m
        or "tcp provider" in m
        or "named pipes provider" in m
    ):
        return (
            "No se puede llegar al servidor (DNS/red/puerto/instancia). "
            "Verifica que 'server' esté correcto (HOST o HOST\\INSTANCIA), "
            "que haya conectividad (ping/DNS), que el firewall permita 1433 "
            "y que SQL Browser esté activo si usas instancia nombrada."
        )

    # =========================================================
    # 8) Base de datos inexistente o sin acceso
    # =========================================================
    if "cannot open database" in m or "unknown database" in m:
        return (
            "La base de datos no existe o tu usuario no tiene permisos sobre ella. "
            "Verifica el nombre 'database' y permisos (CONNECT/USER) en esa DB."
        )

    # =========================================================
    # 9) Permisos SQL insuficientes
    # =========================================================
    # SQLSTATE 42000 = syntax error or access violation
    if sqlstate == "42000" or "permission" in m or "denied" in m:
        return (
            "Error de permisos SQL. Aunque conecte, el usuario no puede ejecutar la consulta "
            "o acceder a la DB/objeto. Revisa roles/permisos."
        )

    # =========================================================
    # 10) Fallback genérico
    # =========================================================
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
    # Seguridad / conectividad
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
    Crea un SQLAlchemy Engine para SQL Server validando conectividad real
    antes de devolverlo, y opcionalmente entrega diagnóstico detallado.

    Flujo general:
        1) Construye connection string ODBC
        2) Prueba conexión real vía pyodbc + SELECT 1
        3) Si OK, crea SQLAlchemy Engine
        4) Si falla, genera SqlServerDiagnostics con causa probable

    Retorno:
        - engine                           (si todo OK)
        - None                             (si falla y on_fail != 'raise')
        - (engine|None, diagnostics)       (si return_diagnostics=True)
    """

    # =========================================================
    # 1) Construcción del ODBC connection string
    # =========================================================
    # Se usa primero para probar conectividad real (pyodbc),
    # antes de crear el Engine de SQLAlchemy.
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"Connection Timeout={timeout}",
    ]

    # ---------------------------------------------------------
    # Autenticación
    # ---------------------------------------------------------
    if trusted_connection:
        # Autenticación integrada (Windows / AD)
        parts.append("Trusted_Connection=yes")
    else:
        # SQL Authentication (usuario / contraseña)
        if username is not None:
            parts.append(f"UID={username}")
        if password is not None:
            parts.append(f"PWD={password}")

    # ---------------------------------------------------------
    # Flags TLS / Encrypt (muy relevantes en ODBC Driver 18)
    # ---------------------------------------------------------
    # encrypt:
    #   None  -> no se especifica (driver decide)
    #   True  -> Encrypt=yes
    #   False -> Encrypt=no
    if encrypt is True:
        parts.append("Encrypt=yes")
    elif encrypt is False:
        parts.append("Encrypt=no")

    # trust_server_certificate:
    #   True  -> TrustServerCertificate=yes
    #   False -> TrustServerCertificate=no
    if trust_server_certificate is True:
        parts.append("TrustServerCertificate=yes")
    elif trust_server_certificate is False:
        parts.append("TrustServerCertificate=no")

    # Connection string final
    odbc_connection_string = ";".join(parts) + ";"

    # =========================================================
    # 2) Construcción de SQLAlchemy URL
    # =========================================================
    # Nota:
    # - Aquí se usa siempre trusted_connection=yes en la URL
    # - Para SQL Auth complejo, normalmente se recomienda
    #   usar odbc_connect en lugar de URL directa.
    driver_url = driver.replace(" ", "+")
    sqlalchemy_url = (
        f"mssql+pyodbc://{server}/{database}"
        f"?trusted_connection=yes&driver={driver_url}"
    )

    # =========================================================
    # 3) Inicialización del diagnóstico
    # =========================================================
    diag = SqlServerDiagnostics(
        ok=False,
        stage="connect",
        odbc_connection_string=odbc_connection_string,
        sqlalchemy_url=sqlalchemy_url,
    )

    # =========================================================
    # 4) Test de conectividad real (pyodbc)
    # =========================================================
    try:
        # -----------------------------------------------------
        # 4.1) Abrir conexión ODBC
        # -----------------------------------------------------
        with pyodbc.connect(odbc_connection_string) as conn:
            diag.stage = "query"

            # -------------------------------------------------
            # 4.2) Ejecutar query mínima
            # -------------------------------------------------
            # SELECT 1 asegura:
            # - permisos mínimos
            # - ejecución real de SQL
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                row = cursor.fetchone()

                if not (row is not None and row[0] == 1):
                    diag.message = (
                        "Conectó pero SELECT 1 no devolvió el valor esperado."
                    )
                    diag.hint = (
                        "Revisa permisos mínimos o políticas que bloqueen queries."
                    )
                    raise RuntimeError(diag.message)

        # =====================================================
        # 5) Creación del SQLAlchemy Engine
        # =====================================================
        try:
            engine = create_engine(
                sqlalchemy_url,
                fast_executemany=fast_executemany,
                pool_pre_ping=pool_pre_ping,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
            )

            # Éxito total
            diag.ok = True
            diag.stage = "sqlalchemy_engine"

            if return_diagnostics:
                return engine, diag
            return engine

        except SQLAlchemyError as e:
            # Error al crear el Engine (URL / pooling / driver)
            diag.ok = False
            diag.stage = "sqlalchemy_engine"
            diag.exception_type = type(e).__name__
            diag.message = str(e)
            diag.hint = (
                "Falló la creación del Engine. Revisa la URL, driver y parámetros. "
                "Si usas SQL Auth, evita caracteres especiales sin URL-encoding "
                "o utiliza odbc_connect."
            )

    # =========================================================
    # 6) Error ODBC (conexión / query)
    # =========================================================
    except pyodbc.Error as e:
        sqlstate, native_code, msg = _parse_pyodbc_error(e)

        diag.ok = False
        diag.stage = "connect" if diag.stage == "connect" else diag.stage
        diag.sqlstate = sqlstate
        diag.native_code = native_code
        diag.message = msg
        diag.exception_type = type(e).__name__
        diag.hint = _diagnose(sqlstate, msg)

    # =========================================================
    # 7) Error genérico (no ODBC)
    # =========================================================
    except Exception as e:
        diag.ok = False
        diag.exception_type = type(e).__name__
        diag.message = str(e)
        diag.hint = (
            "Excepción no-ODBC. Revisa el stacktrace y el flujo de la función."
        )

    # =========================================================
    # 8) Manejo final según política on_fail
    # =========================================================
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
        # Incluye diagnóstico completo en la excepción
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
    Ejecuta una sentencia SQL sin devolver resultados tabulares.

    Pensado para:
        - DDL  (CREATE / ALTER / DROP)
        - DML  (INSERT / UPDATE / DELETE)
        - comandos administrativos simples

    Características:
        - Manejo explícito de transacción (commit / rollback).
        - Consume result sets adicionales (nextset) para evitar errores.
        - Devuelve un resumen simple con estado y duración.

    Retorna:
        {
            "ok": bool,          # True si ejecutó correctamente
            "seconds": float,    # tiempo total de ejecución
            "error": str         # (solo si ok=False) representación del error
        }
    """

    # =========================================================
    # 1) Medición de tiempo de ejecución
    # =========================================================
    # Se toma timestamp inicial para reportar duración total.
    t0 = time.time()

    # =========================================================
    # 2) Apertura de conexión ODBC
    # =========================================================
    # Se crea conexión directa (no SQLAlchemy),
    # ideal para ejecutar SQL "crudo".
    conn = pyodbc.connect(connection_string)

    try:
        # =====================================================
        # 3) Control explícito de transacción
        # =====================================================
        # Desactivamos autocommit para poder:
        # - hacer rollback en caso de error
        # - asegurar atomicidad del SQL ejecutado
        conn.autocommit = False

        # Cursor de ejecución
        cur = conn.cursor()

        # =====================================================
        # 4) Ejecución del SQL
        # =====================================================
        # Puede ser:
        # - una sola sentencia
        # - un batch con múltiples sentencias
        # - un procedimiento almacenado
        cur.execute(sql)

        # =====================================================
        # 5) Consumir result sets adicionales (nextset)
        # =====================================================
        # Algunos comandos (stored procedures, triggers, etc.)
        # generan múltiples result sets implícitos.
        #
        # Si no se consumen, pyodbc puede lanzar errores al cerrar
        # o al hacer commit.
        while True:
            try:
                # nextset() devuelve False cuando no hay más sets
                if not cur.nextset():
                    break
            except pyodbc.Error:
                # Algunos drivers lanzan error al no haber más sets;
                # lo ignoramos y salimos del loop.
                break

        # =====================================================
        # 6) Commit final
        # =====================================================
        conn.commit()

        # Ejecución exitosa
        return {
            "ok": True,
            "seconds": round(time.time() - t0, 3),
        }

    except Exception as e:
        # =====================================================
        # 7) Rollback en caso de error
        # =====================================================
        # Si cualquier cosa falla:
        # - se revierte la transacción
        # - se reporta el error al caller
        conn.rollback()

        return {
            "ok": False,
            "error": repr(e),
            "seconds": round(time.time() - t0, 3),
        }

    finally:
        # =====================================================
        # 8) Cierre defensivo de la conexión
        # =====================================================
        # Se asegura que la conexión se cierre siempre,
        # incluso si hubo excepción.
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
    """
    Normaliza un nombre de hoja (por ejemplo de Excel) para comparación consistente.

    Reglas de normalización:
        - Convierte el valor a string
        - Elimina espacios redundantes (colapsa múltiples espacios en uno)
        - Convierte todo a minúsculas

    Objetivo:
        - Permitir comparar nombres de hojas de forma robusta,
          evitando problemas por:
            * mayúsculas/minúsculas
            * espacios extra al inicio, medio o final
    """

    # str(s)         -> asegura que el input sea texto
    # split()        -> separa por cualquier whitespace y elimina duplicados
    # " ".join(...)  -> vuelve a unir con un solo espacio
    # lower()        -> normaliza a minúsculas
    return " ".join(str(s).split()).lower()


def get_latest_file(
    ruta_archivos: Path,
    exts: set[str] | tuple[str, ...] | list[str],
    *,
    recursive: bool = False,
) -> Path:
    """
    Busca archivos en una carpeta, filtra por extensión y devuelve el más reciente.

    Funcionalidad:
        - Valida que la ruta exista y sea una carpeta.
        - Filtra archivos por extensiones permitidas.
        - Soporta búsqueda recursiva (opcional).
        - Ordena por fecha de modificación (mtime).
        - Devuelve el archivo más reciente.

    Parámetros:
        ruta_archivos:
            Path o ruta a la carpeta donde buscar archivos.
        exts:
            Colección de extensiones válidas.
            Ejemplos:
                {'.xlsx', '.xlsm'}
                ['csv']
                ('parquet', '.pq')
            Nota: se normalizan automáticamente y no es sensible a mayúsculas.
        recursive:
            - False (default): busca solo en el primer nivel de la carpeta.
            - True: busca recursivamente en subcarpetas (rglob).

    Retorna:
        Path del archivo más recientemente modificado.

    Raises:
        AssertionError:
            - Si la carpeta no existe.
            - Si no se encuentran archivos válidos.
    """

    # =========================================================
    # 1) Normalización y validación de la ruta
    # =========================================================
    # Asegura que ruta_archivos sea un objeto Path
    ruta_archivos = Path(ruta_archivos)

    # Validación defensiva: la carpeta debe existir
    assert ruta_archivos.is_dir(), f"No existe carpeta: {ruta_archivos}"

    # =========================================================
    # 2) Normalización de extensiones
    # =========================================================
    # - Fuerza minúsculas
    # - Asegura que todas comiencen con '.'
    #
    # Ejemplos:
    #   'XLSX'   -> '.xlsx'
    #   '.Csv'   -> '.csv'
    exts_norm = {
        e.lower() if str(e).startswith(".") else f".{str(e).lower()}"
        for e in exts
    }

    # =========================================================
    # 3) Iterador de archivos
    # =========================================================
    # recursive=False -> iterdir() (solo nivel actual)
    # recursive=True  -> rglob('*') (recursivo)
    iterator = ruta_archivos.rglob("*") if recursive else ruta_archivos.iterdir()

    # =========================================================
    # 4) Filtrado de archivos válidos
    # =========================================================
    # Condiciones:
    # - debe ser archivo
    # - la extensión debe estar en exts_norm
    archivos = [
        p
        for p in iterator
        if p.is_file() and p.suffix.lower() in exts_norm
    ]

    # =========================================================
    # 5) Ordenar por fecha de modificación (mtime)
    # =========================================================
    # reverse=True -> más reciente primero
    archivos.sort(
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    # Validación: debe haber al menos un archivo
    assert archivos, "No se encontraron Excel válidos."

    # =========================================================
    # 6) Selección del archivo más reciente
    # =========================================================
    archivo_origen = archivos[0]

    # Timestamp legible para logging
    ts = datetime.fromtimestamp(archivo_origen.stat().st_mtime)

    print(
        f"Archivo más reciente: {archivo_origen.name}  |  "
        f"{ts:%Y-%m-%d %H:%M:%S}"
    )

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
    Abre un archivo Excel y selecciona la hoja objetivo a usar.

    Estrategia:
        1) Normaliza los nombres de las hojas y busca coincidencias
           con `preferred_sheets`.
        2) Si encuentra una hoja preferida -> la usa.
        3) Si no encuentra ninguna -> usa la hoja indicada por
           `fallback_sheet_index`.
        4) Si el archivo está bloqueado (PermissionError) y
           `copy_on_permission_error=True`, crea una copia temporal
           y trabaja sobre esa copia.

    Args:
        archivo_origen:
            Ruta al archivo Excel original.
        preferred_sheets:
            Nombres de hojas preferidas (ej: ["Base", "Datos"]).
            La comparación es normalizada (minúsculas y espacios).
        fallback_sheet_index:
            Índice de hoja a usar si no hay coincidencias por nombre.
        engine:
            Engine utilizado por pandas.ExcelFile (default: openpyxl).
        copy_on_permission_error:
            Si True, crea una copia temporal cuando el Excel
            está abierto/bloqueado.

    Returns:
        (
            target_sheet_name,   # nombre real de la hoja seleccionada
            excel_path_used,     # Path del Excel usado (original o copia)
            tmp_copy_path,       # Path de la copia temporal o None
            sheet_names,         # lista completa de hojas disponibles
        )

    Raises:
        SystemExit:
            Si no se puede abrir el Excel o no contiene hojas.
        IndexError:
            Si fallback_sheet_index está fuera de rango.
    """

    # =========================================================
    # 1) Normalización de paths y estado inicial
    # =========================================================
    archivo_origen = Path(archivo_origen)

    # Si se crea una copia temporal, se guarda aquí
    tmp_copy_path: Path | None = None

    # Normalizamos los nombres preferidos para comparación robusta
    # (usa _norm_sheet_name: minúsculas + colapso de espacios)
    pref_norm = {_norm_sheet_name(n) for n in preferred_sheets}

    # =========================================================
    # 2) Función auxiliar para elegir hoja desde una lista
    # =========================================================
    def _select_from_sheetnames(sheet_names: list[str]) -> str:
        """
        Dada una lista de nombres de hojas:
            - busca la primera coincidencia con preferred_sheets
            - si no hay match, usa fallback_sheet_index
        """

        # Validación: el Excel debe tener al menos una hoja
        if not sheet_names:
            raise SystemExit("El Excel no contiene hojas.")

        target = None

        # Búsqueda por nombre normalizado
        for s in sheet_names:
            if _norm_sheet_name(s) in pref_norm:
                target = s
                break

        # Fallback por índice si no hubo match por nombre
        if target is None:
            target = sheet_names[fallback_sheet_index]

        return target

    # =========================================================
    # 3) Apertura del Excel (archivo original)
    # =========================================================
    try:
        try:
            # Intento normal: abrir el Excel directamente
            with pd.ExcelFile(archivo_origen, engine=engine) as xls:
                sheet_names = list(xls.sheet_names)

                # Logging simple para trazabilidad
                print("Hojas:", sheet_names)

                # Selección de hoja objetivo
                target = _select_from_sheetnames(sheet_names)

                print(f"Hoja objetivo: {target}")

                # Retorna usando el archivo original
                return target, archivo_origen, None, sheet_names

        # =====================================================
        # 4) Manejo de archivo bloqueado (PermissionError)
        # =====================================================
        except PermissionError:
            # Si no está permitido copiar, se propaga el error
            if not copy_on_permission_error:
                raise

            # Se crea una copia temporal con timestamp
            tmp_copy_path = (
                archivo_origen.parent
                / f"__tmp_copy_{int(time.time() * 1000)}{archivo_origen.suffix}"
            )

            # Copia fiel (mantiene metadata)
            shutil.copy2(archivo_origen, tmp_copy_path)

            print(
                "No se pudo abrir el archivo original, "
                f"usando copia temporal: {tmp_copy_path.name}"
            )

            # Abrimos la copia temporal
            with pd.ExcelFile(tmp_copy_path, engine=engine) as xls:
                sheet_names = list(xls.sheet_names)

                print("Hojas:", sheet_names)

                target = _select_from_sheetnames(sheet_names)

                print(f"Hoja objetivo (copia): {target}")

                # Retorna usando la copia temporal
                return target, tmp_copy_path, tmp_copy_path, sheet_names

    # =========================================================
    # 5) Manejo de errores generales
    # =========================================================
    except Exception as e:
        # Se encapsula el error para dar un mensaje claro al usuario
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
    Función orquestadora para seleccionar automáticamente:
        1) El archivo Excel más reciente dentro de una carpeta.
        2) La hoja correcta dentro de ese Excel.

    Flujo completo:
        1) Busca el archivo más reciente según fecha de modificación (mtime).
        2) Intenta seleccionar una hoja preferida por nombre normalizado.
        3) Si no hay match por nombre, usa un índice fallback.
        4) Maneja archivos bloqueados creando una copia temporal si es necesario.

    Args:
        ruta_archivos:
            Carpeta donde se buscan los archivos Excel.
        exts:
            Extensiones válidas (ej: {'.xlsx', '.xlsm'}).
        preferred_sheets:
            Lista de nombres de hojas preferidas (ej: ["Base", "Datos"]).
        fallback_sheet_index:
            Índice de hoja a usar si no hay coincidencia por nombre.
        recursive:
            Si True, busca archivos recursivamente en subcarpetas.
        engine:
            Engine usado por pandas.ExcelFile (default: openpyxl).

    Returns:
        dict con las siguientes claves:
            - archivo_origen:
                Path del archivo Excel original seleccionado.
            - excel_path_used:
                Path del archivo realmente usado (original o copia temporal).
            - tmp_copy_path:
                Path de la copia temporal si se creó, o None.
            - target_sheet:
                Nombre de la hoja seleccionada.
            - sheet_names:
                Lista de todas las hojas disponibles en el Excel.
    """

    # =========================================================
    # 1) Seleccionar el archivo Excel más reciente
    # =========================================================
    # get_latest_file:
    # - valida carpeta
    # - filtra por extensión
    # - ordena por fecha de modificación
    # - devuelve el Path más reciente
    archivo_origen = get_latest_file(
        ruta_archivos,
        exts,
        recursive=recursive,
    )

    # =========================================================
    # 2) Seleccionar hoja objetivo dentro del Excel
    # =========================================================
    # pick_target_sheet:
    # - intenta abrir el Excel
    # - busca hoja preferida por nombre normalizado
    # - usa fallback por índice si no hay match
    # - crea copia temporal si el archivo está bloqueado
    (
        target_sheet,
        excel_path_used,
        tmp_copy_path,
        sheet_names,
    ) = pick_target_sheet(
        archivo_origen,
        preferred_sheets,
        fallback_sheet_index=fallback_sheet_index,
        engine=engine,
        copy_on_permission_error=True,
    )

    # =========================================================
    # 3) Retorno estructurado
    # =========================================================
    # Se devuelve un diccionario con toda la información relevante
    # para los siguientes pasos del pipeline (lectura, logging, cleanup).
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

def read_excel_safe(io, sheet_name):
    """
    Lee una hoja de Excel con encabezados usando openpyxl.
    Si falla, detiene la ejecución con un mensaje claro (SystemExit).
    """
    try:
        return pd.read_excel(io, sheet_name=sheet_name, dtype=str, engine="openpyxl")
    except Exception as e:
        raise SystemExit(f"No se pudo leer la hoja '{sheet_name}': {e}")


def _is_nullish(v):
    """
    Determina si un valor debe considerarse "vacío" o "nulo" durante limpieza de datos.

    Se usa típicamente en:
        - Limpieza de DataFrames antes de carga a DB
        - Validación de filas/columnas
        - Eliminación de filas irrelevantes
        - Normalización de inputs de Excel

    Considera como vacío:
        - NaN / NaT (detectado con pd.isna)
        - Strings vacíos o equivalentes semánticos:
            * ""
            * "none"
            * "nan"
          (ignorando espacios y mayúsculas)

    Args:
        v:
            Valor a evaluar (cualquier tipo).

    Returns:
        bool:
            True  -> el valor se considera vacío / nulo
            False -> el valor es válido
    """

    # =========================================================
    # 1) Valores nulos tipo pandas / numpy
    # =========================================================
    # pd.isna cubre:
    # - np.nan
    # - pd.NA
    # - pd.NaT
    if pd.isna(v):
        return True

    # =========================================================
    # 2) Strings "vacíos" semánticos
    # =========================================================
    # Se normaliza:
    # - strip()  -> elimina espacios
    # - lower()  -> ignora mayúsculas
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"", "none", "nan"}

    # =========================================================
    # 3) Cualquier otro valor se considera válido
    # =========================================================
    return False


#import pandas as pd

def drop_initial_empty_rows(
    df: pd.DataFrame,
    max_check_rows: int | None = 2,
    empty_threshold: float = 0.8,
    verbose: bool = True,
    *,
    preview_with_pretty_table: bool = True,
    preview_rows: int = 10,
    stop_at_first_non_empty: bool = True,
):
    """
    Elimina filas iniciales del DataFrame si están "mayoritariamente vacías".

    Se evalúan las primeras filas (hasta max_check_rows) y se eliminan
    aquellas cuya proporción de celdas vacías sea > empty_threshold.

    Definición de "vacío":
        Se usa el helper `_is_nullish`, que considera vacío:
            - NaN / NaT
            - strings vacíos
            - strings equivalentes: "none", "nan" (ignorando mayúsculas y espacios)

    Args:
        df:
            DataFrame de entrada (pandas).
        max_check_rows:
            Número máximo de filas iniciales a evaluar.
            - Si None o 0: no evalúa/elimina filas.
        empty_threshold:
            Umbral en rango [0, 1]. Ejemplo:
                0.8 -> se elimina la fila si > 80% de sus celdas están vacías.
            Importante:
                La comparación es estricta: ratio > empty_threshold (NO >=).
        verbose:
            Si True, imprime mensajes de diagnóstico.
        preview_with_pretty_table:
            Si True, usa `pretty_table` para mostrar un preview visual
            de las filas eliminadas (si existe en tu proyecto).
        preview_rows:
            Número de filas a mostrar en el preview.
        stop_at_first_non_empty:
            Si True, al encontrar la primera fila que NO cumple el criterio,
            deja de evaluar más filas (patrón típico para “basura” inicial).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            (
                df_limpio,               # DataFrame sin las filas iniciales vacías
                df_preview_eliminadas,   # DataFrame con las filas eliminadas
            )
    """

    # =========================================================
    # 1) Validaciones básicas de entrada
    # =========================================================
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser un pandas.DataFrame")

    if df.empty:
        if verbose:
            print("DataFrame vacío, nada que eliminar.")
        return df, pd.DataFrame()

    # Si no hay columnas, cualquier fila tiene 0 celdas -> ratio ambiguo.
    # En este caso, no eliminamos nada por seguridad.
    if df.shape[1] == 0:
        if verbose:
            print("DataFrame sin columnas; no se eliminan filas por seguridad.")
        return df, pd.DataFrame()

    # =========================================================
    # 2) Sanitización de parámetros
    # =========================================================
    # max_check_rows: None o <=0 => desactivar
    if max_check_rows is None:
        max_check_rows_int = 0
    else:
        try:
            max_check_rows_int = max(0, int(max_check_rows))
        except Exception:
            max_check_rows_int = 0

    # empty_threshold: float y clamp a [0,1]
    try:
        empty_threshold_float = float(empty_threshold)
    except Exception:
        empty_threshold_float = 0.8

    if empty_threshold_float < 0:
        empty_threshold_float = 0.0
    elif empty_threshold_float > 1:
        empty_threshold_float = 1.0

    # Si está desactivado, devolver tal cual
    if max_check_rows_int == 0:
        if verbose:
            print("max_check_rows es 0/None; no se eliminan filas iniciales.")
        return df, pd.DataFrame()

    # =========================================================
    # 3) Evaluación de filas iniciales
    # =========================================================
    rows_to_drop = []
    n_rows_to_check = min(max_check_rows_int, len(df))

    for i in range(n_rows_to_check):
        row = df.iloc[i]
        n_total = len(row)

        # Conteo de vacíos
        n_empty = sum(_is_nullish(x) for x in row)

        # Proporción
        ratio = (n_empty / n_total) if n_total else 0.0

        # ✅ CAMBIO CLAVE: comparación estricta (>)
        should_drop = ratio > empty_threshold_float

        if should_drop:
            rows_to_drop.append(i)
            if verbose:
                print(f"🧹 Fila inicial {i} eliminada ({ratio:.0%} vacía).")
        else:
            if verbose:
                print(f"Fila inicial {i} conservada ({ratio:.0%} vacía).")
            if stop_at_first_non_empty:
                break

    # =========================================================
    # 4) Construcción de outputs
    # =========================================================
    if rows_to_drop:
        df_preview_eliminadas = df.iloc[rows_to_drop].copy()
        df_limpio = df.drop(df.index[rows_to_drop]).reset_index(drop=True)
    else:
        df_preview_eliminadas = pd.DataFrame(columns=df.columns)
        df_limpio = df

    # =========================================================
    # 5) Preview opcional
    # =========================================================
    if verbose and rows_to_drop:
        try:
            if preview_with_pretty_table:
                pretty_table(df_preview_eliminadas.head(preview_rows))
            else:
                print(df_preview_eliminadas.head(preview_rows))
        except NameError:
            print(df_preview_eliminadas.head(preview_rows))

    return df_limpio, df_preview_eliminadas

    # =========================================================
    # 4) Eliminación efectiva y preview
    # =========================================================
    removed = pd.DataFrame()

    if rows_to_drop:
        # Guardamos las filas eliminadas para preview/auditoría
        removed = df.iloc[rows_to_drop].copy()

        # Preview opcional
        if verbose and not removed.empty:
            print("\nFilas eliminadas (preview):")

            if preview_with_pretty_table:
                # Visualización más legible (si está disponible)
                pretty_table(
                    removed,
                    n=preview_rows,
                    title="Filas iniciales eliminadas",
                    # enable_download=False,  # opcional
                )
            else:
                # Fallback textual
                print(removed.to_string(index=True))

        # Eliminamos las filas y reindexamos
        df = df.drop(index=rows_to_drop).reset_index(drop=True)

    else:
        if verbose:
            print("No se eliminaron filas iniciales.")

    # =========================================================
    # 5) Retorno final
    # =========================================================
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
    """
    Elimina tildes y acentos de un string.

    Ejemplo:
        "Márzo" -> "Marzo"
        "Septiémbre" -> "Septiembre"

    Objetivo:
        - Facilitar comparaciones de texto robustas
        - Evitar problemas por acentos en nombres de archivo
    """

    # Normaliza a forma NFD (descompone caracteres acentuados)
    # y elimina los caracteres de tipo "Mark, Nonspacing" (Mn),
    # que corresponden a las tildes/acentos.
    return "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _extract_yyyymm_from_name(nombre: str) -> str:
    """
    Intenta extraer un período en formato YYYYMM desde el nombre de un archivo.

    El análisis se hace sobre el nombre SIN extensión.

    Patrones soportados:
        1) YYYYMM pegado:
            - 202503
        2) YYYY con separador + MM:
            - 2025-03
            - 2025_03
            - 2025 03
            - 2025.03
        3) MM con separador + YYYY:
            - 03-2025
            - 03_2025
        4) Mes en texto (español) + año:
            - Marzo 2025
            - MAR_2025
            - SETIEMBRE-2024
            - Dic-2023

    La detección:
        - NO depende del orden exacto de palabras
        - Ignora acentos
        - No es sensible a mayúsculas/minúsculas

    Raises:
        ValueError:
            Si no se logra identificar un período válido.
    """

    # =========================================================
    # 1) Nombre base sin extensión
    # =========================================================
    stem = Path(nombre).stem

    # Normalización:
    # - quitar acentos
    # - pasar a mayúsculas
    stem_norm = _strip_accents(stem).upper()

    # =========================================================
    # 2) Caso 1: YYYYMM pegado (ej: 202503)
    # =========================================================
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", stem_norm)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    # =========================================================
    # 3) Caso 2: YYYY + separador + MM
    # =========================================================
    m = re.search(r"(20\d{2})[-_/.\s]?(0[1-9]|1[0-2])", stem_norm)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    # =========================================================
    # 4) Caso 3: MM + separador + YYYY
    # =========================================================
    m = re.search(r"(0[1-9]|1[0-2])[-_/.\s]?(20\d{2})", stem_norm)
    if m:
        return f"{m.group(2)}{m.group(1)}"

    # =========================================================
    # 5) Caso 4: Mes en texto + año
    # =========================================================
    # Primero buscamos un año
    m_year = re.search(r"(20\d{2})", stem_norm)
    if m_year:
        year = m_year.group(1)

        # Luego buscamos cualquier mes en texto
        for mes_txt, mm in MESES.items():
            # Asegura que sea "palabra completa"
            # (evita falsos positivos dentro de códigos largos)
            if re.search(
                rf"(?<![A-Z0-9]){mes_txt}(?![A-Z0-9])",
                stem_norm,
            ):
                return f"{year}{mm}"

    # =========================================================
    # 6) Fallo total
    # =========================================================
    raise ValueError(
        f"No pude extraer YYYYMM desde el nombre: {nombre}"
    )

def canonicalizar_planes(nombre: str) -> str:
    """
    Devuelve un nombre de archivo canónico estandarizado para planes de facturación.

    Formato esperado:
        Facturacion_Cesantia_YYYYMM.xlsx

    Comportamiento:
        - Intenta extraer el período (YYYYMM) desde el nombre original.
        - Si lo logra, construye el nombre canónico.
        - Si NO lo logra:
            * imprime un aviso
            * usa un timestamp como fallback
    """

    try:
        # Intento normal: extraer período
        yyyymm = _extract_yyyymm_from_name(nombre)
        return f"Facturacion_Cesantia_{yyyymm}.xlsx"

    except ValueError as e:
        # Fallback defensivo:
        # - no detiene el pipeline
        # - deja trazabilidad con timestamp
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
    """
    Elimina acentos y diacríticos de un string.

    Ejemplos:
        "Márzo"        -> "Marzo"
        "Número°"      -> "Numero°"
        "Árbol Ñandú"  -> "Arbol Nandu"

    Objetivo:
        - Normalizar texto para comparaciones y generación de nombres.
        - Evitar problemas por acentos en:
            * nombres de columnas
            * nombres de archivos
            * identificadores de DB
            * variables generadas automáticamente
    """

    # unicodedata.normalize('NFKD', ...):
    # - Descompone caracteres acentuados en:
    #     letra base + marca diacrítica
    #
    # unicodedata.combining(c):
    # - True si el carácter es una marca diacrítica (tilde, acento)
    #
    # El comprehension elimina esas marcas y deja sólo la letra base.
    return "".join(
        c
        for c in unicodedata.normalize("NFKD", str(s))
        if not unicodedata.combining(c)
    )


def normalize_name(s: str) -> str:
    """
    Normaliza un string para usarlo como nombre técnico seguro.

    Transformaciones aplicadas (en orden):
        1) Quita acentos y diacríticos.
        2) Convierte a minúsculas.
        3) Elimina espacios al inicio y final.
        4) Elimina el símbolo '°'.
        5) Reemplaza espacios por '_'.
        6) Reemplaza cualquier carácter no alfanumérico por '_'.
        7) Colapsa múltiples '_' consecutivos en uno solo.
        8) Elimina '_' al inicio y final.

    Ejemplos:
        " Número° de Plan "      -> "numero_de_plan"
        "Fecha (Inicio)"        -> "fecha_inicio"
        "Monto-$ Total"         -> "monto_total"
        "  Año  2024  "         -> "ano_2024"

    Objetivo:
        - Generar nombres consistentes y seguros para:
            * columnas de DataFrame
            * columnas SQL
            * variables
            * claves técnicas
        - Evitar caracteres problemáticos en SQL y Python.
    """

    # =========================================================
    # 1) Quitar acentos, pasar a minúsculas y limpiar bordes
    # =========================================================
    s = _strip_accents(s).lower().strip()

    # =========================================================
    # 2) Eliminar símbolos específicos problemáticos
    # =========================================================
    # El símbolo '°' es común en encabezados (ej: "N° contrato")
    s = s.replace("°", "")

    # =========================================================
    # 3) Reemplazar espacios por underscore
    # =========================================================
    # \s+ captura uno o más espacios, tabs, saltos de línea, etc.
    s = re.sub(r"\s+", "_", s)

    # =========================================================
    # 4) Reemplazar cualquier carácter no alfanumérico por '_'
    # =========================================================
    # \w permite [a-zA-Z0-9_]
    # [^\w] captura todo lo demás
    s = re.sub(r"[^\w]", "_", s)

    # =========================================================
    # 5) Colapsar underscores múltiples y limpiar bordes
    # =========================================================
    s = re.sub(r"_+", "_", s).strip("_")

    return s

####################################################################################################################
############################################ Modificaciones al PyDF ###############################################
####################################################################################################################

def make_unique(names):
    """
    Garantiza que una lista de nombres sea única, agregando sufijos incrementales
    cuando hay duplicados.

    Comportamiento:
        - Si un nombre aparece por primera vez, se deja tal cual.
        - Si se repite, se agrega un sufijo '_N' donde N es un contador incremental.
        - Si el nombre es None o vacío, se usa 'col' como base.

    Ejemplo:
        ["a", "b", "a", "a", "", None, "b"]
        ->
        ["a", "b", "a_2", "a_3", "col", "col_2", "b_2"]

    Args:
        names:
            Iterable de nombres (strings o valores falsy).

    Returns:
        list[str]:
            Lista de nombres únicos, preservando el orden original.
    """

    # Diccionario para llevar la cuenta de cuántas veces hemos visto cada nombre base
    seen = {}

    # Lista de salida con nombres ya garantizados como únicos
    out = []

    for n in names:
        # Si el nombre es vacío o None, usamos un nombre base genérico
        base = n if n else "col"

        # Caso 1: primera vez que vemos este nombre base
        if base not in seen:
            seen[base] = 1
            out.append(base)

        # Caso 2: nombre duplicado -> agregar sufijo incremental
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")

    return out


def _row_nullish_ratio(row: pd.Series, exclude=()):
    """
    Calcula la proporción de valores "vacíos" en una fila de un DataFrame.

    Definición de "vacío":
        Se utiliza el helper `_is_nullish`, que considera vacío:
            - NaN / NaT
            - strings vacíos
            - strings equivalentes a "none" o "nan"

    Args:
        row:
            Fila del DataFrame (pd.Series).
        exclude:
            Iterable de nombres de columnas a excluir del cálculo
            (por ejemplo, columnas técnicas o de control).

    Returns:
        float:
            Proporción de valores vacíos en la fila.
            - 0.0 -> fila completamente llena
            - 1.0 -> fila completamente vacía
    """

    # =========================================================
    # 1) Determinar columnas a evaluar
    # =========================================================
    # Excluimos explícitamente las columnas indicadas
    cols = [c for c in row.index if c not in exclude]

    # Si no quedan columnas a evaluar, consideramos la fila como "vacía"
    if not cols:
        return 1.0

    # =========================================================
    # 2) Conteo de valores vacíos
    # =========================================================
    n_null = sum(_is_nullish(row[c]) for c in cols)

    # =========================================================
    # 3) Proporción de vacíos
    # =========================================================
    return n_null / len(cols)


def drop_trailing_mostly_null(
    df: pd.DataFrame,
    null_check_exclude=("Nombre_de_archivo",),
    also_exclude_money_cols=("Prima_Bruta_mensual", "IVA", "Prima_Neta", "Diferencia_CCLA"),
    null_ratio_threshold=0.80,
    verbose=True,
):
    """
    Elimina filas al FINAL del DataFrame mientras tengan una proporción alta
    de valores nulos/vacíos.

    Se recorre el DataFrame desde la última fila hacia arriba y se eliminan
    consecutivamente las filas cuyo ratio de nulos sea >= null_ratio_threshold.
    La eliminación se detiene en cuanto se encuentra una fila "válida".

    Además:
        - Muestra por consola TODAS las filas eliminadas con su contenido completo
          (útil para auditoría y debugging).

    Definición de "nulo":
        Se usa `_row_nullish_ratio`, que a su vez utiliza `_is_nullish`:
            - NaN / NaT
            - strings vacíos
            - strings "none", "nan"

    Args:
        df:
            DataFrame de entrada.
        null_check_exclude:
            Columnas a excluir del cálculo de nulos
            (ej: columnas técnicas como Nombre_de_archivo).
        also_exclude_money_cols:
            Columnas monetarias que se excluyen del cálculo
            (suelen venir siempre vacías en filas de totales/notas).
        null_ratio_threshold:
            Umbral en rango [0, 1].
            Ejemplo:
                0.80 -> elimina filas con >= 80% de valores nulos.
        verbose:
            Si True, imprime diagnóstico detallado y filas eliminadas.

    Returns:
        pd.DataFrame:
            DataFrame limpio, sin filas finales mayoritariamente nulas.
    """

    # =========================================================
    # 1) Validaciones iniciales
    # =========================================================
    if df is None or df.empty:
        if verbose:
            print("DF vacío: nada que hacer.")
        return df

    # Trabajamos sobre una copia para no mutar el DF original
    out = df.copy()

    # Aquí guardamos las filas eliminadas:
    # (index_original, fila_completa)
    removed_rows = []

    # =========================================================
    # 2) Construcción del set de columnas excluidas
    # =========================================================
    # Se unen:
    # - columnas técnicas
    # - columnas monetarias
    exclude = set(null_check_exclude) | set(also_exclude_money_cols)

    # =========================================================
    # 3) Recorrido desde el final hacia arriba
    # =========================================================
    i = len(out) - 1

    while i >= 0:
        row = out.iloc[i]

        # Calcula proporción de valores nulos en la fila
        ratio = _row_nullish_ratio(row, exclude=exclude)

        if verbose:
            print(f"Fila índice {out.index[i]} → null_ratio={ratio:.2%}")

        # Si la fila es mayoritariamente nula, se marca para eliminar
        if ratio >= null_ratio_threshold:
            # Guardamos la fila COMPLETA antes de eliminarla
            removed_rows.append((out.index[i], row.copy()))

            i -= 1
        else:
            # En cuanto aparece una fila válida, se detiene el proceso
            break

    # =========================================================
    # 4) Caso: no se eliminó nada
    # =========================================================
    if not removed_rows:
        if verbose:
            print("❎ No se detectaron filas finales mayoritariamente nulas.")
        return out

    # =========================================================
    # 5) Mostrar filas eliminadas (auditoría)
    # =========================================================
    if verbose:
        print(
            f"\n🧹 Eliminando {len(removed_rows)} fila(s) finales mayoritariamente nulas:\n"
        )
        for idx, row in removed_rows:
            print(f"--- Fila eliminada (índice original {idx}) ---")
            # to_frame().T permite mostrar la fila horizontalmente
            print(row.to_frame().T)
            print("\n")

    # =========================================================
    # 6) Eliminación efectiva del DataFrame
    # =========================================================
    drop_indices = [idx for idx, _ in removed_rows]

    out = out.drop(index=drop_indices).reset_index(drop=True)

    return out

def pick(df, *names):
    """
    Devuelve la primera columna existente del DataFrame cuyo nombre
    esté presente en la lista de nombres candidatos.

    Comportamiento:
        - Recorre los nombres entregados en orden.
        - Devuelve la primera columna que exista en df.columns.
        - Si ninguna existe, devuelve una Series del mismo largo que el DF,
          llena con None y con el mismo índice.

    Args:
        df:
            pandas.DataFrame desde el cual se quiere extraer una columna.
        *names:
            Lista variable de nombres de columnas candidatos, en orden de prioridad.
            Ejemplo:
                pick(df, "rut", "rut_cliente", "id_cliente")

    Returns:
        pd.Series:
            - La columna encontrada (si existe alguna).
            - O una Series llena de None si ninguna columna existe.
    """

    # =========================================================
    # 1) Buscar la primera columna existente
    # =========================================================
    # Se respeta el orden de prioridad dado por *names
    for n in names:
        if n in df.columns:
            return df[n]

    # =========================================================
    # 2) Fallback: ninguna columna encontrada
    # =========================================================
    # Se devuelve una Series:
    # - del mismo largo que el DataFrame
    # - con el mismo índice
    # - llena de None (equivalente a NULL)
    return pd.Series(
        [None] * len(df),
        index=df.index,
    )

#from __future__ import annotations

#from typing import Iterable, Mapping, Sequence
#import numpy as np
#import pandas as pd

def to_num_series(s: pd.Series) -> pd.Series:
    """
    Convierte una Serie a numérica de forma robusta.

    Estrategia:
        - Si la Serie NO es de tipo object/string:
            usa directamente pd.to_numeric con errors="coerce".
        - Si la Serie ES object/string:
            * convierte todo a string
            * limpia espacios
            * normaliza valores vacíos o textuales a NaN
            * convierte finalmente a numérico

    Resultado:
        - Devuelve una Serie de tipo float (con NaN).
        - El caller puede luego castear explícitamente a:
            * 'Int64' (entero nullable)
            * 'float64'
            según el caso de uso.

    Args:
        s:
            pd.Series de entrada (numérica u object).

    Returns:
        pd.Series:
            Serie convertida a numérica (float), con NaN donde no fue posible convertir.
    """

    # =========================================================
    # 1) Caso simple: la Serie NO es object/string
    # =========================================================
    # Ejemplos:
    #   int64, float64, Int64, boolean, etc.
    # En estos casos, pd.to_numeric es suficiente.
    if not pd.api.types.is_object_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    # =========================================================
    # 2) Caso complejo: Serie tipo object / string
    # =========================================================
    # Aquí suelen aparecer problemas típicos de Excel:
    #   - espacios
    #   - strings vacíos
    #   - "None", "nan", "NaN" como texto
    #   - mezcla de números y texto
    #
    # Paso a paso:
    s2 = (
        # Convertimos todo a string para poder aplicar .str
        s.astype(str)

         # Quitamos espacios al inicio y final
         .str.strip()

         # Normalizamos strings "vacíos" a np.nan
         .replace({
             "": np.nan,
             "None": np.nan,
             "none": np.nan,
             "nan": np.nan,
             "NaN": np.nan,
         })
    )

    # =========================================================
    # 3) Conversión final a numérico
    # =========================================================
    # errors="coerce":
    #   - valores no convertibles -> NaN
    return pd.to_numeric(s2, errors="coerce")


def cast_numeric_columns(
    df: pd.DataFrame,
    *,
    bigint_cols: Iterable[str] = (),
    int_cols: Iterable[str] = (),
    float_cols: Iterable[str] = (),
) -> pd.DataFrame:
    """
    Castea columnas numéricas del DataFrame si existen.

    Reglas:
        - bigint_cols / int_cols:
            * Se convierten a pandas 'Int64' (entero nullable).
            * Soporta NaN sin romper (a diferencia de int64 nativo).
        - float_cols:
            * Se convierten a 'float64'.

    La conversión es robusta:
        - Usa `to_num_series` para limpiar strings sucios antes de castear.
        - Valores no convertibles terminan como NaN.

    Nota:
        - El DataFrame se MUTA in-place.
        - Se devuelve el mismo df por conveniencia (patrón fluido).

    Args:
        df:
            DataFrame de entrada.
        bigint_cols:
            Columnas que representan enteros grandes (IDs, montos grandes).
        int_cols:
            Columnas enteras estándar.
        float_cols:
            Columnas decimales.

    Returns:
        pd.DataFrame:
            El mismo DataFrame, con las columnas casteadas cuando existían.
    """

    # =========================================================
    # 1) Columnas enteras grandes (bigint)
    # =========================================================
    # Se castea a 'Int64' (nullable integer de pandas)
    for c in bigint_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("Int64")

    # =========================================================
    # 2) Columnas enteras estándar
    # =========================================================
    # También se castea a 'Int64' para soportar NaN
    for c in int_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("Int64")

    # =========================================================
    # 3) Columnas decimales
    # =========================================================
    # Se castea a float64 (estándar numpy)
    for c in float_cols:
        if c in df.columns:
            df[c] = to_num_series(df[c]).astype("float64")

    return df


def normalize_dv_column(df: pd.DataFrame, dv_col: str) -> pd.DataFrame:
    """
    Normaliza una columna de Dígito Verificador (DV).

    Transformaciones aplicadas:
        - Convierte a tipo 'string' nullable de pandas.
        - Elimina espacios (strip).
        - Convierte a mayúsculas.
        - Conserva sólo el primer carácter (char(1)).
        - Valores inválidos o vacíos -> pd.NA.

    Pensado para:
        - DV de RUT chileno (0-9, K).
        - Datos que vienen con:
            * espacios
            * strings largos
            * valores nulos
            * mezclas de tipos

    Args:
        df:
            DataFrame de entrada.
        dv_col:
            Nombre de la columna que contiene el DV.

    Returns:
        pd.DataFrame:
            El mismo DataFrame, con la columna DV normalizada si existía.
    """

    if dv_col in df.columns:
        df[dv_col] = (
            df[dv_col]
            # Convertir a string nullable (soporta NA)
            .astype("string")
            # Limpiar espacios
            .str.strip()
            # Normalizar a mayúsculas (K)
            .str.upper()
            # Quedarse sólo con el primer carácter válido
            .map(
                lambda x: x[:1]
                if pd.notna(x) and len(x) > 0
                else pd.NA
            )
        )

    return df


def trim_string_columns(
    df: pd.DataFrame,
    limits: Mapping[str, int],
    *,
    strip: bool = True,
) -> pd.DataFrame:
    """
    Recorta columnas de texto a un largo máximo definido por columna.

    Para cada columna especificada en `limits`, si la columna existe en el DataFrame:
        - Convierte la columna a tipo 'string' nullable de pandas.
        - Opcionalmente elimina espacios en blanco (strip).
        - Recorta el texto al largo máximo permitido.

    Es especialmente útil para:
        - Ajustar columnas a límites de VARCHAR/CHAR en bases de datos.
        - Evitar errores de truncamiento al insertar en SQL Server.
        - Normalizar textos provenientes de Excel u otras fuentes.

    Args:
        df:
            DataFrame de entrada.
        limits:
            Mapping {nombre_columna: largo_maximo}.
            Ejemplo:
                {
                    "nombre_cliente": 100,
                    "direccion": 255,
                    "dv": 1,
                }
        strip:
            Si True (default), elimina espacios al inicio y final antes de recortar.

    Returns:
        pd.DataFrame:
            El mismo DataFrame (mutado), con las columnas de texto recortadas
            cuando existían en el DF.
    """

    # =========================================================
    # 1) Iterar sobre las columnas y sus largos máximos
    # =========================================================
    for col, max_len in limits.items():

        # Sólo aplicamos la lógica si la columna existe
        if col in df.columns:

            # =================================================
            # 2) Conversión a string nullable
            # =================================================
            # Usar dtype "string" de pandas:
            # - soporta pd.NA
            # - es más consistente que object
            s = df[col].astype("string")

            # =================================================
            # 3) Limpieza opcional de espacios
            # =================================================
            if strip:
                s = s.str.strip()

            # =================================================
            # 4) Recorte al largo máximo
            # =================================================
            # str.slice(0, max_len):
            # - corta desde el índice 0
            # - asegura que ningún valor exceda max_len caracteres
            df[col] = s.str.slice(0, int(max_len))

    return df


def report_nulls(df: pd.DataFrame, critical_cols: Sequence[str]) -> None:
    """
    Imprime por consola el conteo de valores nulos (NaN) en columnas críticas.

    Sólo considera las columnas que:
        - estén listadas en `critical_cols`
        - y que efectivamente existan en el DataFrame

    Pensado para:
        - validaciones rápidas de calidad de datos
        - checks previos a carga en base de datos
        - logging simple en ETL

    Args:
        df:
            DataFrame a evaluar.
        critical_cols:
            Secuencia de nombres de columnas consideradas críticas
            (ej: claves, fechas, montos obligatorios).

    Returns:
        None
    """

    # =========================================================
    # 1) Filtrar columnas críticas presentes en el DF
    # =========================================================
    present = [c for c in critical_cols if c in df.columns]

    # Si ninguna columna crítica está presente, no hay nada que reportar
    if not present:
        return

    # =========================================================
    # 2) Imprimir reporte simple de nulos
    # =========================================================
    print("\n🔎 Nulos en columnas críticas:")
    for c in present:
        # isna().sum() devuelve la cantidad de valores nulos
        print(f" - {c}: {int(df[c].isna().sum())} nulos")



def build_sql_frame(df: pd.DataFrame, cols_sql: Sequence[str]) -> pd.DataFrame:
    """
    Construye un DataFrame con las columnas esperadas por SQL,
    preservando el orden definido en `cols_sql`.

    Sólo se incluyen las columnas que:
        - estén listadas en cols_sql
        - y que existan realmente en el DataFrame

    Es una función defensiva:
        - ignora columnas faltantes
        - no falla si el DF tiene columnas extra

    Args:
        df:
            DataFrame de entrada.
        cols_sql:
            Secuencia de nombres de columnas en el orden esperado por SQL.

    Returns:
        pd.DataFrame:
            Nuevo DataFrame con sólo las columnas presentes,
            en el orden definido por cols_sql.
    """

    # =========================================================
    # 1) Filtrar columnas que existan en el DataFrame
    # =========================================================
    cols_present = [c for c in cols_sql if c in df.columns]

    # =========================================================
    # 2) Construir DataFrame final (copia defensiva)
    # =========================================================
    # .copy() evita mutaciones accidentales del DF original
    return df[cols_present].copy()
