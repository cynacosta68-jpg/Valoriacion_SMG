import streamlit as st
import pandas as pd
import io

# Configuración de página
st.set_page_config(page_title="Valorizador de Prestaciones SMG", layout="wide")

st.title("🏥 Sistema de Valorización Médica Profesional")
st.markdown("Este sistema replica la lógica completa de tu Colab para la liquidación y valorización.")

# --- FUNCIONES DE LIMPIEZA Y NORMALIZACIÓN ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_flujo_completo(df_liqui, db_valor):
    try:
        # --- PASO 1: LIMPIEZA SEGÚN TU COLAB ---
        # Eliminamos filas totalmente vacías y duplicados por transacción
        df_f = df_liqui.dropna(how='all').copy()
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        
        # Limpieza de nombres de columnas (quita espacios invisibles)
        df_f.columns = [str(c).strip() for c in df_f.columns]
        
        # --- PASO 2: PREPARACIÓN DE BASES ---
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()
        
        df_nom.columns = [str(c).strip() for c in df_nom.columns]
        df_uni.columns = [str(c).strip() for c in df_uni.columns]
        df_fijos.columns = [str(c).strip() for c in df_fijos.columns]

        # Normalización para el Macheo
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        
        # Categoría (Flexible a nombres de columna)
        col_cat = next((c for c in df_f.columns if c.lower() in ['categoria', 'categoría']), 'categoria')
        df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Fechas a periodos Mes-Año
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # --- PASO 3: REGLAS DE VALORIZACIÓN (Merges de alta velocidad) ---
        
        # REGLA 1: Nomenclador + Unidades
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']],
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

        # REGLA 2: Valor Fijos (Con filtro SWISS MEDICAL)
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        
        # 2A: Con Categoría
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
        # 2B: Sin Categoría
        f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2B'))

        # REGLA 3: Valor Fijos (SIN FILTROS - TODA LA TABLA)
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # --- PASO 4: CONSOLIDACIÓN DE RESULTADOS ---
        def consolidar(row):
            if pd.notna(row['IMPORTE_R1']): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR VALORES"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)

        # Cálculo de Columna TOTAL
        def calcular_total_final(row):
            try:
                return float(row['IMPORTE']) * float(row['cantidad'])
            except:
                return row['IMPORTE']
        
        df_f['Total'] = df_f.apply(calcular_total_final, axis=1)

        # --- PASO 5: LIMPIEZA FINAL DE COLUMNAS ---
        # Mantenemos las columnas originales + IMPORTE y Total
        # Borramos todas las creadas por el proceso de unión
        auxiliares = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in auxiliares) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error en la lógica de procesamiento: {e}")
        return None

# --- INTERFAZ STREAMLIT ---
st.sidebar.header("📥 Carga de Datos")
file_rep = st.sidebar.file_uploader("1. Reporte de Liquidación (Excel/CSV)", type=["xlsx", "csv"])
file_val = st.sidebar.file_uploader("2. Base de Valorización (Excel)", type=["xlsx"])

if file_rep and file_val:
    if st.button("🚀 Iniciar Valorización"):
        # Lectura robusta
        if file_rep.name.endswith('.csv'):
            df_in = pd.read_csv(file_rep, encoding='latin1', sep=None, engine='python')
        else:
            df_in = pd.read_excel(file_rep)
        
        db_in = pd.read_excel(file_val, sheet_name=None)
        
        with st.spinner("Ejecutando pasos del Colab..."):
            df_res = procesar_flujo_completo(df_in, db_in)
            
        if df_res is not None:
            st.success(f"✅ ¡Proceso Exitoso! {len(df_res)} registros procesados.")
            st.dataframe(df_res.head(100))
            
            # Preparar Excel para descargar
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_res.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Descargar Reporte Final",
                data=output.getvalue(),
                file_name="reporte_valorizado_smg.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
else:
    st.info("Sube los archivos para comenzar. Se aplicará la limpieza de duplicados y las 3 reglas de valorización.")
