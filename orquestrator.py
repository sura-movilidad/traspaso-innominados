import os
import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError
from tqdm import tqdm


# ---------------------------------
# CONFIGURACIÓN
# ---------------------------------
BASE_NOTEBOOKS_DIR = "new_source/new_notebooks"
BASE_DATA_DIR = "data"
CCLA_DIR = "CCLA"
EXCEL_EXTENSION = ".xlsx"

# Notebook especial (final)
FINAL_NOTEBOOK_NAME = "Monitoreo_CCLA.ipynb"


# ---------------------------------
# UTILIDADES
# ---------------------------------
def normalizar_nombre(nombre: str) -> str:
    return nombre.strip().upper()


def existe_excel_en_ruta(base_path: str, nombre_fuente: str) -> bool:
    """
    Busca archivos Excel en:
    base_path / <nombre_fuente> /
    """
    if not os.path.exists(base_path):
        return False

    for carpeta in os.listdir(base_path):
        if normalizar_nombre(carpeta) == normalizar_nombre(nombre_fuente):
            carpeta_data = os.path.join(base_path, carpeta)
            return any(
                archivo.endswith(EXCEL_EXTENSION)
                for archivo in os.listdir(carpeta_data)
            )
    return False


def ejecutar_notebook(path_notebook: str) -> bool:
    print(f"[RUN] Ejecutando notebook: {path_notebook}")

    # PROJECT_ROOT para notebooks
    project_root = os.path.abspath(os.getcwd())
    os.environ["PROJECT_ROOT"] = project_root

    with open(path_notebook, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    client = NotebookClient(
        nb,
        kernel_name="python3",
        timeout=600,
        allow_errors=False
    )

    try:
        client.execute()
        print("[OK] Notebook ejecutado correctamente")
        return True

    except CellExecutionError:
        print(f"[ERROR] Error ejecutando {path_notebook}")
        return False


# ---------------------------------
# ORQUESTADOR PRINCIPAL
# ---------------------------------
def main():

    tareas_run = []
    tareas_skip = []
    final_notebook_path = None

    # ---------------------------------
    # Descubrimiento de notebooks
    # ---------------------------------
    for item in os.listdir(BASE_NOTEBOOKS_DIR):
        item_path = os.path.join(BASE_NOTEBOOKS_DIR, item)

        # --- NOTEBOOK FINAL (especial) ---
        if item == FINAL_NOTEBOOK_NAME:
            final_notebook_path = item_path
            continue

        # --- Notebook en carpeta (NO CCLA) ---
        if os.path.isdir(item_path):
            notebooks = [f for f in os.listdir(item_path) if f.endswith(".ipynb")]
            if not notebooks:
                continue

            nombre_fuente = item
            path_nb = os.path.join(item_path, notebooks[0])
            base_data = BASE_DATA_DIR

        # --- Notebook suelto (CCLA) ---
        elif item.endswith(".ipynb"):
            nombre_fuente = item.replace(".ipynb", "")
            path_nb = item_path
            base_data = os.path.join(BASE_DATA_DIR, CCLA_DIR)

        else:
            continue

        if existe_excel_en_ruta(base_data, nombre_fuente):
            tareas_run.append({
                "nombre": nombre_fuente,
                "path": path_nb
            })
        else:
            tareas_skip.append(nombre_fuente)

    # ---------------------------------
    # OUTPUT INICIAL
    # ---------------------------------
    total = len(tareas_run) + len(tareas_skip) + (1 if final_notebook_path else 0)
    print(f"\n[INFO] Notebooks detectados: {total}\n")

    for nombre in tareas_skip:
        print(f"[SKIP] {nombre} (sin Excel)")

    # ---------------------------------
    # Ejecución PRINCIPAL
    # ---------------------------------
    errores = []
    ejecutados_ok = 0

    for tarea in tqdm(
        tareas_run,
        desc="Ejecutando notebooks",
        unit="nb",
        dynamic_ncols=True,
        leave=True
    ):
        ok = ejecutar_notebook(tarea["path"])
        if ok:
            ejecutados_ok += 1
        else:
            errores.append(tarea["nombre"])

    # ---------------------------------
    # NOTEBOOK FINAL CONDICIONADO
    # ---------------------------------
    if final_notebook_path:
        print("\n[INFO] Evaluando ejecucion del notebook final...")

        if ejecutados_ok > 0:
            print("[INFO] Hubo cambios → ejecutando notebook final")
            ejecutar_notebook(final_notebook_path)
        else:
            print("[SKIP] Notebook final no ejecutado (sin cambios previos)")

    # ---------------------------------
    # RESUMEN FINAL
    # ---------------------------------
    print("\n[INFO] ejecucion finalizada")

    if errores:
        print("[WARN] Notebooks con error:")
        for e in errores:
            print(f"  - {e}")
    else:
        print("[OK] Todos los notebooks principales ejecutados correctamente")


if __name__ == "__main__":
    main()