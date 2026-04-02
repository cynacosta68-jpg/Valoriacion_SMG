import streamlit as st
import pandas as pd
import io

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Profesional", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.info("Este sistema replica la lógica completa de tu Google Colab.")

# --- FUNCIONES DE LIMPIEZA ORIGINALES DEL COLAB ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_valorizacion_completa(df_liqui, db_valor):
    try:
        # 1. Preparación del reporte (Igual a limpiar_bloque_1 de tu Colab)
        df_f = df_liqui.dropna(how='all').copy()
        df_f.columns = [str(c).strip() for c in df_f.columns]
        # Limpieza de duplicados por transacción para asegurar los 8289 registros
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        
        # 2. Carga de hojas de la base de valorización
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        # 3. Normalización de Datos para cruce
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        
        # Detección de Categoría
        col_cat = next((c for c in df_f.columns if c.lower() in ['categoria', 'categoría']), 'categoria')
        df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Fechas a periodos (YYYY-MM)
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # --- REGLA 1: NOMENCLADOR + UNIDADES ---
        # Unimos Nomenclador con Unidades (usando los nombres exactos del Excel)
        df_calc_uni = pd.merge(
            df_nom, df_uni, 
            left_on=['Tipo de nomenclador'], 
            right_on=['Tipo de Nomenclador'], 
            how='inner'
        )
        # Filtramos para que coincida el periodo del valor de la unidad
        df_calc_uni = df_calc_uni[df_calc_uni['periodo_aux_x'] == df_calc_uni['periodo_aux_y']] if 'periodo_aux_x' in df_calc_uni.columns else df_calc_uni
        
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux' if 'periodo_aux' in df_calc_uni.columns else 'periodo_aux_y'])
        
        # Cruzamos con el reporte principal
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], 
                        left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # --- REGLA 2: VALOR FIJOS (SWISS MEDICAL) ---
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

        # --- REGLA 3: VALOR FIJOS (SIN FILTROS) ---
        f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], 
                        right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R3'))

        # --- CONSOLIDACIÓN ---
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
            return "#REVISAR VALORES"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        
        # Columna Total
        def calcular_total(row):
            try: return float(row['IMPORTE']) * float(row['cantidad'])
            except: return row['IMPORTE']
        df_f['Total'] = df_f.apply(calcular_total, axis=1)

        # --- LIMPIEZA FINAL DE AUXILIARES Y DUPLICADOS ---
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in aux) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error detallado: {e}")
        return None

# --- INTERFAZ ---
st.sidebar.header("Archivos")
f1 = st.sidebar.file_uploader("Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar"):
        if f1.name.endswith('.csv'):
            df_l = pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        else:
            df_l = pd.read_excel(f1)
        
        db_v = pd.read_excel(f2, sheet_name=None)
        
        res = procesar_valorizacion_completa(df_l, db_v)
        
        if res is not None:
            st.success(f"Procesado: {len(res)} registros.")
            st.dataframe(res.head(50))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Excel", output.getvalue(), "reporte_valorizado.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
