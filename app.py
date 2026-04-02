import streamlit as st
import pandas as pd
import io

# Configuración de la página
st.set_page_config(page_title="Valorizador de Prestaciones SMG", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Esta aplicación replica la lógica exacta de tu Google Colab para procesar y valorizar liquidaciones.")

# --- FUNCIONES DE LIMPIEZA Y NORMALIZACIÓN (Extraídas del Colab) ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_flujo_colab(df_original, db_valor):
    try:
        # 1. RÉPLICA DE 'limpiar_bloque_1'
        df = df_original.copy()
        
        # Eliminar columnas según la lógica de tu notebook
        columnas_a_eliminar = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura'
        ]
        df = df.drop(columns=[c for c in columnas_a_eliminar if c in df.columns])
        
        # Limpieza de nombres de columnas y eliminación de filas vacías
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how='all')
        
        # Mantenemos los 8289 registros originales mediante transacción_item
        if 'transacción_item' in df.columns:
            df = df.drop_duplicates(subset=['transacción_item'], keep='first')

        # 2. PREPARACIÓN DE BASES DE DATOS (Mismo formato que el reporte)
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        for d in [df_nom, df_uni, df_fijos]:
            d.columns = [str(c).strip() for c in d.columns]

        # 3. NORMALIZACIÓN DE CLAVES DE CRUCE
        df['prest_limpia'] = df['prestación'].apply(limpiar_codigo)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        
        # Categoría (Normalización de nombres y contenido)
        col_cat = next((c for c in df.columns if c.lower() in ['categoria', 'categoría']), 'categoria')
        df['cat_limpia'] = df[col_cat].apply(limpiar_texto)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Fechas a periodos (YYYY-MM)
        df['periodo_aux'] = pd.to_datetime(df['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # 4. APLICACIÓN DE REGLAS DE VALORIZACIÓN
        
        # REGLA 1: Nomenclador + Unidades (Cirujano * Valor)
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        
        df = pd.merge(df, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # REGLA 2: Valor Fijos (Con filtro SWISS MEDICAL)
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        
        # 2A: Match con Categoría
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df = pd.merge(df, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                      left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                      how='left', suffixes=('', '_R2A'))
        
        # 2B: Match sin Categoría
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df = pd.merge(df, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                      left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                      how='left', suffixes=('', '_R2B'))

        # REGLA 3: Valor Fijos (SIN FILTROS - Toda la tabla como pediste)
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df = pd.merge(df, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                      left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                      right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                      how='left', suffixes=('', '_R3'))

        # 5. CONSOLIDACIÓN FINAL
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR VALORES"

        df['IMPORTE'] = df.apply(consolidar, axis=1)

        # Cálculo de Columna TOTAL
        def calcular_total_final(row):
            try: return float(row['IMPORTE']) * float(row['cantidad'])
            except: return row['IMPORTE']
        df['Total'] = df.apply(calcular_total_final, axis=1)

        # 6. LIMPIEZA DE COLUMNAS AUXILIARES
        # Re-limpiamos duplicados finales para asegurar los 8289
        df = df.drop_duplicates(subset=['transacción_item'], keep='first')
        
        auxiliares = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df.columns if not any(a in c for a in auxiliares) or c in ['IMPORTE', 'Total']]
        
        return df[cols_finales]

    except Exception as e:
        st.error(f"Error detallado en la ejecución: {e}")
        return None

# --- INTERFAZ STREAMLIT ---
st.sidebar.header("📁 Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Iniciar Procesamiento"):
        # Lectura del reporte
        if f1.name.endswith('.csv'):
            df_in = pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        else:
            df_in = pd.read_excel(f1)
        
        # Lectura de la base
        db_in = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Procesando limpieza inicial y reglas de valorización..."):
            res = procesar_flujo_colab(df_in, db_in)
            
        if res is not None:
            st.success(f"✅ ¡Éxito! Registros procesados: {len(res)}")
            st.dataframe(res.head(50))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Descargar Reporte Final Valorizado",
                data=output.getvalue(),
                file_name="reporte_final_valorizado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
