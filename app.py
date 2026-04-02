import streamlit as st
import pandas as pd
import io

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Profesional", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Carga los archivos para ejecutar el flujo completo del Colab.")

# --- FUNCIONES DE NORMALIZACIÓN (Según tu Colab) ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_todo_el_flujo(df_liqui, db_valor):
    try:
        # ==========================================
        # PASO 1: LIMPIEZA INICIAL (REPLICANDO CELDA 1 COLAB)
        # ==========================================
        df_f = df_liqui.copy()
        
        # 1.1 Limpiar nombres de columnas (Quitar espacios y pasar a minúsculas)
        df_f.columns = [str(c).strip().lower() for c in df_f.columns]
        
        # 1.2 Eliminar columnas que no se usan (Según tu notebook)
        cols_a_borrar = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura', 'estado', 'periodo_prestacion'
        ]
        df_f = df_f.drop(columns=[c for c in cols_a_borrar if c in df_f.columns])
        
        # 1.3 Eliminar filas totalmente vacías
        df_f = df_f.dropna(how='all')
        
        # 1.4 Mantener integridad de registros (8289 originales)
        if 'transacción_item' in df_f.columns:
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

        # ==========================================
        # PASO 2: PREPARACIÓN DE BASES DE VALORIZACIÓN
        # ==========================================
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()
        
        # Normalizar encabezados de bases
        for d in [df_nom, df_uni, df_fijos]:
            d.columns = [str(c).strip() for c in d.columns]

        # Macheo de claves (prestación y categoría)
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        
        # BUSQUEDA SEGURA DE CATEGORIA
        col_cat = next((c for c in df_f.columns if 'categoria' in c or 'categoría' in c), None)
        if col_cat:
            df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
        else:
            st.error("❌ No se encontró la columna 'categoria' en el reporte.")
            return None

        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Normalizar Fechas
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # ==========================================
        # PASO 3: REGLAS DE VALORIZACIÓN
        # ==========================================
        
        # Regla 1: Nomenclador + Unidades
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # Regla 2: Valor Fijos (Swiss Medical)
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2B'))

        # Regla 3: Valor Fijos (Sin filtros)
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # ==========================================
        # PASO 4: CONSOLIDACIÓN Y TOTAL (ÚLTIMAS CELDAS)
        # ==========================================
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        
        df_f['Total'] = pd.to_numeric(df_f['IMPORTE'], errors='coerce') * pd.to_numeric(df_f['cantidad'], errors='coerce')
        df_f['Total'] = df_f['Total'].fillna(df_f['IMPORTE'])

        # Limpieza de basura técnica
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in aux) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error en el procesamiento: {e}")
        return None

# --- INTERFAZ ---
st.sidebar.header("📁 Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar Reporte"):
        if f1.name.endswith('.csv'):
            df_in = pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        else:
            df_in = pd.read_excel(f1)
        
        db_in = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Ejecutando limpieza inicial y valorización..."):
            res = procesar_todo_el_flujo(df_in, db_in)
            
        if res is not None:
            st.success(f"✅ ¡Completado! {len(res)} registros únicos.")
            st.dataframe(res.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado.xlsx")
