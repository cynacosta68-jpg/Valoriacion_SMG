import streamlit as st
import pandas as pd
import io

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Completo", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Este sistema replica **todos** los pasos de tu Colab: Limpieza inicial + Reglas de Valorización.")

# --- FUNCIONES DE LIMPIEZA ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_todo(df_liqui, db_valor):
    try:
        # ==========================================
        # PASO 1: LÓGICA DE LIMPIEZA INICIAL (DEL COLAB)
        # ==========================================
        # Eliminamos filas totalmente vacías
        df_f = df_liqui.dropna(how='all').copy()
        
        # Limpieza de nombres de columnas
        df_f.columns = [str(c).strip() for c in df_f.columns]
        
        # ELIMINAR COLUMNAS (Basado en tu notebook: quitamos las que no sirven para el reporte final)
        # Aquí puedes agregar o quitar nombres según lo que borraba tu Colab específicamente
        cols_a_borrar_inicial = ['estado', 'periodo_prestacion', 'fecha_presentacion', 'nro_lote', 'nro_factura']
        df_f = df_f.drop(columns=[c for c in cols_a_borrar_inicial if c in df_f.columns])
        
        # LIMPIEZA DE DUPLICADOS (Crucial para mantener los 8289 registros)
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

        # ==========================================
        # PASO 2: PREPARACIÓN DE BASES DE DATOS
        # ==========================================
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()
        
        # Limpiar nombres de columnas en bases
        for d in [df_nom, df_uni, df_fijos]:
            d.columns = [str(c).strip() for c in d.columns]

        # Normalización para cruces
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        
        col_cat = next((c for c in df_f.columns if c.lower() in ['categoria', 'categoría']), 'categoria')
        df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Fechas a periodos
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # ==========================================
        # PASO 3: REGLAS DE VALORIZACIÓN
        # ==========================================
        
        # REGLA 1: Nomenclador + Unidades
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux' if 'periodo_aux' in df_calc_uni.columns else 'periodo_aux_y'])
        
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # REGLA 2: Valor Fijos (Swiss Medical)
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                        how='left', suffixes=('', '_R2A'))
        
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

        # CONSOLIDACIÓN
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

        # ==========================================
        # PASO 4: LIMPIEZA FINAL (COMO TU COLAB)
        # ==========================================
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in aux) or c in ['IMPORTE', 'Total']]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error en el flujo: {e}")
        return None

# --- INTERFAZ ---
st.sidebar.header("📂 Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar Reporte Completo"):
        if f1.name.endswith('.csv'):
            df_l = pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        else:
            df_l = pd.read_excel(f1)
        
        db_v = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Procesando limpieza y valorización..."):
            res = procesar_todo(df_l, db_v)
        
        if res is not None:
            st.success(f"✅ Finalizado: {len(res)} registros únicos.")
            st.dataframe(res.head(50))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado_final.xlsx")
