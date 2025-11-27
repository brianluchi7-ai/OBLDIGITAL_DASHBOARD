from conexion_mysql import crear_conexion
import pandas as pd

def ver_columnas(tabla):
    conexion = crear_conexion()
    if conexion is None:
        print("❌ No hay conexión.")
        return
    try:
        df = pd.read_sql(f"SHOW COLUMNS FROM {tabla}", conexion)
        print(f"\n📋 Columnas en {tabla}:")
        print(df[["Field", "Type"]])
    except Exception as e:
        print(f"⚠️ Error al leer columnas de {tabla}: {e}")
    finally:
        conexion.close()

if __name__ == "__main__":
    tablas = ["dep_sep_rtn_2025", "dep_oct_rtn_2025", "dep_nov_rtn_2025"]
    for t in tablas:
        ver_columnas(t)
