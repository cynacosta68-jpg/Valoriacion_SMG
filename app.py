import streamlit as st
import pandas as pd
import io
import unicodedata

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Profesional", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Réplica exacta de la lógica de tu Colab con limpieza de columnas inteligente.")

# --- FUNCIONES DE NORMALIZACIÓN ---
def normalizar_texto(texto):
    """Elimina tildes, pasa a minúsculas y quita espacios extra."""
    if not isinstance(texto, str):
        return str(texto)
    texto = texto.lower().strip()
    # Eliminar acentos
    texto = "".join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    return texto

def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto_celda(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_todo_el_flujo(df_liqui, db_valor):
    try:
        # ==========================================
        # PASO 1: LIMPIEZA INICIAL (REPLICANDO COLAB)
        # ==========================================
        # 1.1 Normalización AGRESIVA de nombres de columnas
        df_liqui.columns = [normalizar_texto(c) for c in df_liqui.columns]
        
        # 1.2 Eliminar columnas que no se usan (nombres normalizados)
        cols_borrar = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura', 'estado', 'periodo_prestacion'
        ]
        df_f = df_liqui.drop(columns=[c for c in cols_borrar if c in df_liqui.columns]).copy()
        
        # 1.3 Eliminar filas vacías
        df_f = df_f.dropna(how='all')
        
        # 1.4 Mantener integridad (8289 registros) usando el nombre normalizado
        col_item = 'transaccion_item' if 'transaccion_item' in df_f.columns else None
        if col_item:
            df_f = df_f.drop_duplicates(subset=[col_item], keep='first')

        # ==========================================
        # PASO 2: PREPARACIÓN DE BASES DE VALORIZACIÓN
        # ==========================================
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()
        
        # Normalizar encabezados de bases (solo espacios)
        for d in [df_nom, df_uni, df_fijos]:
            d.columns = [str(c).strip() for c in d.columns]

        # Claves de cruce normalizadas
        # Buscamos 'prestacion' (ya normalizada sin tilde)
        if 'prestacion' in df_f.columns:
            df_f['prest_limpia'] = df_f['prestacion'].apply(limpiar_codigo)
        else:
            st.error(f"❌ No se encontró la columna de prestación. Columnas: {df_f.columns.tolist()}")
            return None

        if 'categoria' in df_f.columns:
            df_f['cat_limpia'] = df_f['categoria'].apply(limpiar_texto_celda)
        else:
            st.error(f"❌ No se encontró la columna de categoría. Columnas: {df_f.columns.tolist()}")
            return None

        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto_celda)

        # Periodos
        col_fecha = 'fecha_transaccion' if 'fecha_transaccion' in df_f.columns else None
        if col_fecha:
            df_f['periodo_aux'] = pd.to_datetime(df_f[col_fecha], dayfirst=True, errors='coerce').dt.to_period('M')
        
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # ==========================================
        # PASO 3: REGLAS DE VALORIZACIÓN
        # ==========================================
        # Regla 1: Nomenclador
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # Regla 2: Fijos Swiss
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2B'))

        # Regla 3: Fijos Global
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # Consolidación
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        
        # Cantidad y Total
        col_cant = 'cantidad' if 'cantidad' in df_f.columns else None
        if col_cant:
            df_f['Total'] = pd.to_numeric(df_f['IMPORTE'], errors='coerce') * pd.to_numeric(df_f[col_cant], errors='coerce')
            df_f['Total'] = df_f['Total'].fillna(df_f['IMPORTE'])

        # Limpieza final
        if col_item:
            df_f = df_f.drop_duplicates(subset=[col_item], keep='first')
        
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador', 'cod_limpio_r2a', 'cod_limpio_r2b', 'cod_limpio_r3']
        cols_finales = [c for c in df_f.columns if not any(a in c.lower() for a in aux) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error detallado: {e}")
        return None

# --- INTERFAZ ---
st.sidebar.header("📁 Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar Reporte"):
        # Lectura con detección automática de separador para CSV
        if f1.name.endswith('.csv'):
            df_in = pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        else:
            df_in = pd.read_excel(f1)
        
        db_in = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Limpiando columnas y aplicando valorización..."):
            res = procesar_todo_el_flujo(df_in, db_in)
            
        if res is not None:
            st.success(f"✅ ¡Completado! Registros: {len(res)}")
            st.dataframe(res.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado.xlsx")
