import streamlit as st
import pandas as pd
import io

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Final", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Réplica exacta de la lógica de limpieza y valorización de tu Notebook.")

# --- FUNCIONES DE LIMPIEZA DEL COLAB ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_todo(df_liqui, db_valor):
    try:
        # ==========================================
        # PASO 1: LIMPIEZA INICIAL (REPLICANDO COLAB)
        # ==========================================
        # 1.1 Normalizar nombres de columnas: quitar espacios, tildes y pasar a minúsculas
        df_liqui.columns = [
            str(c).strip().lower()
            .replace('í', 'i').replace('á', 'a').replace('é', 'e').replace('ó', 'o').replace('ú', 'u') 
            for c in df_liqui.columns
        ]
        
        # 1.2 Eliminar filas vacías
        df_f = df_liqui.dropna(how='all').copy()
        
        # 1.3 Eliminar columnas innecesarias (según Bloque 1 de tu Colab)
        cols_borrar = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura', 'estado', 'periodo_prestacion'
        ]
        df_f = df_f.drop(columns=[c for c in cols_borrar if c in df_f.columns])

        # 1.4 Limpieza de duplicados por transacción_item (Mantener 8289 registros)
        if 'transacción_item' in df_f.columns:
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        elif 'transaccion_item' in df_f.columns:
             df_f = df_f.drop_duplicates(subset=['transaccion_item'], keep='first')

        # ==========================================
        # PASO 2: PREPARACIÓN DE BASES DE DATOS
        # ==========================================
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()
        
        # Limpiar encabezados de las bases
        df_nom.columns = [str(c).strip() for c in df_nom.columns]
        df_uni.columns = [str(c).strip() for c in df_uni.columns]
        df_fijos.columns = [str(c).strip() for c in df_fijos.columns]

        # Normalización de claves (prestación y categoria)
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        
        # BUSQUEDA SEGURA DE CATEGORIA (Ya normalizada a 'categoria' sin acento)
        if 'categoria' in df_f.columns:
            df_f['cat_limpia'] = df_f['categoria'].apply(limpiar_texto)
        else:
            st.error(f"❌ No se encontró la columna 'categoria'. Columnas detectadas: {df_f.columns.tolist()}")
            return None

        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Periodos
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # ==========================================
        # PASO 3: REGLAS DE VALORIZACIÓN
        # ==========================================
        # R1: Nomenclador
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # R2: Fijos Swiss
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2B'))

        # R3: Fijos Global
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # ==========================================
        # PASO 4: CONSOLIDACIÓN
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

        # Limpieza final
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first', errors='ignore')
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in aux) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

# --- INTERFAZ ---
st.sidebar.header("📁 Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar Reporte"):
        df_in = pd.read_csv(f1, encoding='latin1', sep=None, engine='python') if f1.name.endswith('.csv') else pd.read_excel(f1)
        db_in = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Procesando limpieza y valorización..."):
            res = procesar_todo(df_in, db_in)
            
        if res is not None:
            st.success(f"✅ ¡Completado! Registros: {len(res)}")
            st.dataframe(res.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado.xlsx")
