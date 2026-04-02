import streamlit as st
import pandas as pd
import io
import unicodedata

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG - Flujo Colab", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Este código ejecuta los pasos en el orden exacto de tu Notebook de Colab.")

# --- FUNCIONES DE APOYO ---
def normalizar_encabezado(texto):
    if not isinstance(texto, str): return str(texto)
    texto = texto.lower().strip()
    texto = "".join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    return texto

def limpiar_celda_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

# --- FLUJO PRINCIPAL REORDENADO ---
def ejecutar_proceso_colab(df_subido, db_valor):
    try:
        # ---------------------------------------------------------
        # PASO 1: LIMPIEZA ESTRUCTURAL (Bloque 1 del Colab)
        # ---------------------------------------------------------
        st.write("---")
        st.write("### 🛠️ Paso 1: Limpieza Estructural")
        
        # Copia para no afectar el original
        df = df_subido.copy()
        
        # Normalizar nombres de columnas para que Python las encuentre
        df.columns = [normalizar_encabezado(c) for c in df.columns]
        
        # Columnas a eliminar según tu notebook
        cols_sobrantes = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura', 'estado', 'periodo_prestacion'
        ]
        df = df.drop(columns=[c for c in cols_sobrantes if c in df.columns])
        
        # Eliminar filas vacías y duplicados por transacción_item (Garantiza los 8289 registros)
        df = df.dropna(how='all')
        if 'transaccion_item' in df.columns:
            df = df.drop_duplicates(subset=['transaccion_item'], keep='first')
            st.success(f"Registros únicos detectados: {len(df)}")
        else:
            st.warning("⚠️ No se encontró 'transaccion_item'. Se omitió la limpieza de duplicados.")

        # ---------------------------------------------------------
        # PASO 2: PREPARACIÓN DE LAS BASES (Nomenclador, Unidades, Fijos)
        # ---------------------------------------------------------
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        # Limpieza de las bases
        for d in [df_nom, df_uni, df_fijos]:
            d.columns = [str(c).strip() for c in d.columns]

        # ---------------------------------------------------------
        # PASO 3: NORMALIZACIÓN DE DATOS PARA CRUCE
        # ---------------------------------------------------------
        # Creamos las columnas limpias para matchear
        if 'prestacion' in df.columns:
            df['prest_limpia'] = df['prestacion'].apply(limpiar_celda_codigo)
        else:
            st.error(f"❌ Error: No se encontró la columna 'prestación'. Columnas: {list(df.columns)}")
            return None

        # Categoría (si no existe, avisamos)
        if 'categoria' in df.columns:
            df['cat_limpia'] = df['categoria'].astype(str).str.strip().str.upper()
        else:
            st.info("ℹ️ La columna 'categoría' no está presente. Se ignorará el filtro por categoría.")
            df['cat_limpia'] = ""

        # Fechas y Periodos
        if 'fecha_transaccion' in df.columns:
            df['periodo_aux'] = pd.to_datetime(df['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')
        df_fijos['cat_limpia'] = df_fijos['Arancel'].astype(str).str.strip().str.upper()
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_celda_codigo)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_celda_codigo)

        # ---------------------------------------------------------
        # PASO 4: REGLAS DE VALORIZACIÓN (Orden de prioridad)
        # ---------------------------------------------------------
        st.write("### 💰 Paso 2: Aplicando Reglas de Valorización")
        
        # REGLA 1: Nomenclador * Unidades
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        
        df = pd.merge(df, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # REGLA 2: Fijos Swiss Medical
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        
        # Match A: Con Categoría
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df = pd.merge(df, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
        # Match B: Sin Categoría
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df = pd.merge(df, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2B'))

        # REGLA 3: Fijos Global (Sin filtros)
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df = pd.merge(df, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # ---------------------------------------------------------
        # PASO 5: CONSOLIDACIÓN Y CÁLCULO FINAL
        # ---------------------------------------------------------
        def consolidar_final(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR"

        df['IMPORTE'] = df.apply(consolidar_final, axis=1)
        
        # Cálculo de columna Total
        if 'cantidad' in df.columns:
            df['Total'] = pd.to_numeric(df['IMPORTE'], errors='coerce') * pd.to_numeric(df['cantidad'], errors='coerce')
            df['Total'] = df['Total'].fillna(df['IMPORTE'])
        
        # Limpieza de rastro (columnas auxiliares)
        auxiliares = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df.columns if not any(a in c for a in auxiliares) or c in ['IMPORTE', 'Total']]
        
        return df[cols_finales]

    except Exception as e:
        st.error(f"❌ Ocurrió un error en el orden de ejecución: {e}")
        return None

# --- UI STREAMLIT ---
st.sidebar.header("📂 Subida de Archivos")
file_liq = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
file_db = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if file_liq and file_db:
    if st.button("🚀 Ejecutar Proceso Ordenado"):
        # Carga inicial
        df_subido = pd.read_csv(file_liq, encoding='latin1', sep=None, engine='python') if file_liq.name.endswith('.csv') else pd.read_excel(file_liq)
        db_valor = pd.read_excel(file_db, sheet_name=None)
        
        resultado = ejecutar_proceso_colab(df_subido, db_valor)
        
        if resultado is not None:
            st.success("✅ Proceso completado.")
            st.dataframe(resultado.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                resultado.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado.xlsx")
