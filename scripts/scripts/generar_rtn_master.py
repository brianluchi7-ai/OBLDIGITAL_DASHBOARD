import pandas as pd
from conexion_mysql import crear_conexion

# ======================================================
# === OBL DIGITAL — Generador RTN_MASTER (Tolerante) ===
# ======================================================

def limpiar_encabezados(df, tabla):
    if any(c.lower().startswith("col") or "num_" in c.lower() for c in df.columns):
        print(f"🔹 Encabezados detectados en {tabla}, aplicando primera fila como encabezado...")
        primera_fila = df.iloc[0].fillna("").astype(str)
        df.columns = primera_fila
        df = df.drop(df.index[0])
    return df


def estandarizar_columnas(df):
    rename_map = {
        "fecha": "date",
        "date_ftd": "date",
        "fechadep": "date",
        "equipo": "team",
        "team_name": "team",
        "leader_team": "team",
        "pais": "country",
        "country_name": "country",
        "agente": "agent",
        "agent_name": "agent",
        "afiliado": "affiliate",
        "affiliate_name": "affiliate",
        "usuario": "id",
        "id_user": "id",
        "id_usuario": "id",
        "monto": "usd",
        "usd_total": "usd",
        "amount_country": "usd",
    }

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df.rename(columns={old: new}, inplace=True)
    return df


def cargar_tabla(tabla, conexion):
    print(f"\n===> Leyendo tabla {tabla} ...")
    df = pd.read_sql(f"SELECT * FROM {tabla}", conexion)
    print(f"   🔸 Columnas detectadas: {list(df.columns)}")
    print(f"   🔸 Registros brutos: {len(df)}")

    # Limpieza básica
    if "nov" not in tabla:
        df = limpiar_encabezados(df, tabla)
    df = estandarizar_columnas(df)

    # 🔹 Modo tolerante total para noviembre
    if "nov" in tabla:
        print("   ⚙️ Modo tolerante: NOVIEMBRE se leerá y guardará tal cual.")
        df["month_name"] = "Nov"
    else:
        df = df.map(lambda x: str(x).strip() if isinstance(x, str) else x)
        df["month_name"] = tabla.replace("dep_", "").replace("_rtn_2025", "").capitalize()

    # 🔹 Asegurar índice limpio y columnas únicas
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.reset_index(drop=True)

    print(f"   ✅ Filas válidas: {len(df)}")
    return df


def obtener_datos():
    conexion = crear_conexion()
    if conexion is None:
        print("❌ No se pudo conectar a Railway.")
        return pd.DataFrame()

    tablas = ["dep_sep_rtn_2025", "dep_oct_rtn_2025", "dep_nov_rtn_2025"]
    dataframes = []

    for tabla in tablas:
        try:
            df = cargar_tabla(tabla, conexion)
            if not df.empty:
                dataframes.append(df)
        except Exception as e:
            print(f"⚠️ Error procesando {tabla}: {e}")

    conexion.close()

    if not dataframes:
        print("❌ No se generó RTN_MASTER (sin datos).")
        return pd.DataFrame()

    # 🔹 Concatenar sin conflictos
    for i in range(len(dataframes)):
        dataframes[i].columns = dataframes[i].columns.astype(str)
        dataframes[i] = dataframes[i].reset_index(drop=True)

    df_master = pd.concat(dataframes, ignore_index=True, sort=False)
    df_master.dropna(how="all", inplace=True)
    df_master = df_master.reset_index(drop=True)

    print(f"\n📊 RTN_MASTER generado correctamente con {len(df_master)} registros.")
    df_master.to_csv("RTN_MASTER_preview.csv", index=False, encoding="utf-8-sig")
    print("💾 Vista previa guardada: RTN_MASTER_preview.csv")

    # ======================================
    # Crear tabla física RTN_MASTER_CLEAN
    # ======================================
    try:
        conexion = crear_conexion()
        if conexion:
            cursor = conexion.cursor()
            cursor.execute("DROP TABLE IF EXISTS RTN_MASTER_CLEAN;")
            cursor.execute("""
                CREATE TABLE RTN_MASTER_CLEAN (
                    date TEXT,
                    id TEXT,
                    team TEXT,
                    agent TEXT,
                    country TEXT,
                    affiliate TEXT,
                    usd TEXT,
                    month_name TEXT
                );
            """)
            conexion.commit()

            columnas = ["date", "id", "team", "agent", "country", "affiliate", "usd", "month_name"]
            for _, row in df_master.iterrows():
                valores = [str(row[c]) if c in row else None for c in columnas]
                cursor.execute(
                    "INSERT INTO RTN_MASTER_CLEAN (date, id, team, agent, country, affiliate, usd, month_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    valores
                )

            conexion.commit()
            conexion.close()
            print("✅ RTN_MASTER_CLEAN creada y poblada correctamente en Railway.")
    except Exception as e:
        print(f"⚠️ Error al crear RTN_MASTER_CLEAN: {e}")

    return df_master


if __name__ == "__main__":
    df = obtener_datos()
    print("\nPrimeras filas de RTN_MASTER:")
    print(df.head())
