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

    for item in os.listdir(BASE_NOTEBOOKS_DIR):
        item_path = os.path.join(BASE_NOTEBOOKS_DIR, item)

        # ---------------------------------
        # CASO 1: NOTEBOOK EN CARPETA (NO CCLA)
        # data/<carpeta>/
        # ---------------------------------
        if os.path.isdir(item_path):
            notebooks = [
                f for f in os.listdir(item_path)
                if f.endswith(".ipynb")
            ]
            if not notebooks:
                continue

            nombre_fuente = item
            path_nb = os.path.join(item_path, notebooks[0])
            base_data = BASE_DATA_DIR  # data/<fuente>/

        # ---------------------------------
        # CASO 2: NOTEBOOK SUELTO (CCLA)
        # data/CCLA/<notebook>/
        # ---------------------------------
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
    # OUTPUT
    # ---------------------------------
    total = len(tareas_run) + len(tareas_skip)
    print(f"\n[INFO] Notebooks detectados: {total}\n")

    for nombre in tareas_skip:
        print(f"[SKIP] {nombre} (sin Excel)")

    errores = []

    for tarea in tqdm(
        tareas_run,
        desc="Ejecutando notebooks",
        unit="nb",
        dynamic_ncols=True,
        leave=True
    ):
        ok = ejecutar_notebook(tarea["path"])
        if not ok:
            errores.append(tarea["nombre"])

    print("\n[INFO] Ejecución finalizada")

    if errores:
        print("[WARN] Notebooks con error:")
        for e in errores:
            print(f"  - {e}")
    else:
        print("[OK] Todos los notebooks ejecutados correctamente")


if __name__ == "__main__":
    main()