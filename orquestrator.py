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

# Notebook especial (final, dentro de CCLA)
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

    # PROJECT_ROOT para los notebooks
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
    # 1️⃣ Descubrimiento de notebooks
    # ---------------------------------
    for item in os.listdir(BASE_NOTEBOOKS_DIR):
        item_path = os.path.join(BASE_NOTEBOOKS_DIR, item)

        # -------------------------------------------------
        # CASO A: carpeta CCLA (lógica especial)
        # -------------------------------------------------
        if item == CCLA_DIR and os.path.isdir(item_path):
            for nb_file in os.listdir(item_path):
                if not nb_file.endswith(".ipynb"):
                    continue

                nb_path = os.path.join(item_path, nb_file)

                # Notebook final
                if nb_file == FINAL_NOTEBOOK_NAME:
                    final_notebook_path = nb_path
                    continue

                # Notebooks CCLA normales
                nombre_fuente = nb_file.replace(".ipynb", "")
                base_data = os.path.join(BASE_DATA_DIR, CCLA_DIR)

                if existe_excel_en_ruta(base_data, nombre_fuente):
                    tareas_run.append({
                        "nombre": nombre_fuente,
                        "path": nb_path
                    })
                else:
                    tareas_skip.append(nombre_fuente)

        # -------------------------------------------------
        # CASO B: carpetas NO CCLA
        # -------------------------------------------------
        elif os.path.isdir(item_path):
            notebooks = [
                f for f in os.listdir(item_path)
                if f.endswith(".ipynb")
            ]
            if not notebooks:
                continue

            nombre_fuente = item
            path_nb = os.path.join(item_path, notebooks[0])
            base_data = BASE_DATA_DIR

            if existe_excel_en_ruta(base_data, nombre_fuente):
                tareas_run.append({
                    "nombre": nombre_fuente,
                    "path": path_nb
                })
            else:
                tareas_skip.append(nombre_fuente)

    # ---------------------------------
    # 2️⃣ OUTPUT INICIAL
    # ---------------------------------
    total = len(tareas_run) + len(tareas_skip) + (1 if final_notebook_path else 0)
    print(f"\n[INFO] Notebooks detectados: {total}\n")

    for nombre in tareas_skip:
        print(f"[SKIP] {nombre} (sin Excel)")

    # ---------------------------------
    # 3️⃣ EJECUCIÓN PRINCIPAL
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
    # 4️⃣ NOTEBOOK FINAL (CCLA)
    # ---------------------------------
    if final_notebook_path:
        print("\n[INFO] Evaluando ejecución del notebook final (CCLA)...")

        if ejecutados_ok > 0:
            print("[INFO] Hubo cambios → ejecutando notebook final")
            ejecutar_notebook(final_notebook_path)
        else:
            print("[SKIP] Notebook final no ejecutado (sin cambios previos)")

    # ---------------------------------
    # 5️⃣ RESUMEN FINAL
    # ---------------------------------
    print("\n[INFO] Ejecución finalizada")

    if errores:
        print("[WARN] Notebooks con error:")
        for e in errores:
            print(f"  - {e}")
    else:
        print("[OK] Todos los notebooks principales ejecutados correctamente")


if __name__ == "__main__":
    main()