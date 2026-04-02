import streamlit as st
import pandas as pd
import io

# Configuración de la aplicación
st.set_page_config(page_title="Valorizador SMG", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("""
Esta herramienta cruza los reportes de liquidación con la base de datos de valorización 
aplicando reglas de Nomenclador y Valores Fijos.
""")

# --- FUNCIONES DE LIMPIEZA ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_valorizacion(df_f, db_val):
    # 0. Limpiar nombres de columnas (quita espacios y errores de carga)
    df_f.columns = [str(c).strip() for c in df_f.columns]
    
    # 1. Identificar columna de categoría (flexible a acentos/mayúsculas)
    col_cat = None
    for c in ['categoria', 'Categoría', 'CATEGORIA', 'CATEGORÍA']:
        if c in df_f.columns:
            col_cat = c
            break
    
    if not col_cat:
        st.error("❌ No se encontró la columna 'categoria' en el archivo de liquidación.")
        return None

    # 2. Limpieza de duplicados inicial
    df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first').copy()
    
    # 3. Carga y limpieza de hojas de la base de datos
    df_nom = db_val['Nomenclador'].copy()
    df_uni = db_val['unidades'].copy()
    df_fijos = db_val['Valor Fijos'].copy()
    
    df_nom.columns = [str(c).strip() for c in df_nom.columns]
    df_uni.columns = [str(c).strip() for c in df_uni.columns]
    df_fijos.columns = [str(c).strip() for c in df_fijos.columns]

    # 4. Normalización de Datos
    df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
    df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
    df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
    df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
    df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

    # Normalización de fechas a periodos (YYYY-MM)
    df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
    df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
    df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

    # --- REGLA 1: NOMENCLADOR + UNIDADES ---
    df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
    df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
    df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
    
    df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']],
                    left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

    # --- REGLA 2: VALOR FIJOS (SWISS MEDICAL) ---
    f_filt = df_fijos[df_fijos['Nomenclador'].str.contains('SWISS MEDICAL', na=True, case=False)].copy()
    
    f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
    df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                    left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                    how='left', suffixes=('', '_R2A'))
    
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
        if pd.notna(row['IMPORTE_R1']): return row['IMPORTE_R1']
        if pd.notna(row['Total prestación']): return row['Total prestación']
        if pd.notna(row['Total prestación_R2B']): return row['Total prestación_R2B']
        if pd.notna(row['Total prestación_R3']): return row['Total prestación_R3']
        return "#REVISAR VALORES"

    df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)

    # --- COLUMNA TOTAL ---
    def calc_total(row):
        try: return float(row['IMPORTE']) * float(row['cantidad'])
        except: return row['IMPORTE']
    df_f['Total'] = df_f.apply(calc_total, axis=1)

    # --- LIMPIEZA FINAL ---
    df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
    prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación']
    cols_a_borrar = [c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']]
    
    return df_f.drop(columns=cols_a_borrar)

# --- INTERFAZ DE USUARIO ---
c1, c2 = st.columns(2)
with c1:
    archivo_liqui = st.file_uploader("Subir Reporte de Liquidación (.xlsx)", type="xlsx")
with c2:
    archivo_base = st.file_uploader("Subir Base de Datos Valorización (.xlsx)", type="xlsx")

if archivo_liqui and archivo_base:
    if st.button("🚀 Iniciar Procesamiento"):
        df_liqui = pd.read_excel(archivo_liqui)
        db_valor = pd.read_excel(archivo_base, sheet_name=None)
        
        with st.spinner("Procesando reglas de valorización..."):
            df_final = procesar_valorizacion(df_liqui, db_valor)
        
        if df_final is not None:
            st.success(f"¡Listo! Se procesaron {len(df_final)} registros únicos.")
            st.dataframe(df_final.head(50))
            
            towrite = io.BytesIO()
            df_final.to_excel(towrite, index=False, engine='openpyxl')
            towrite.seek(0)
            
            st.download_button(
                label="📥 Descargar Reporte Valorizado",
                data=towrite,
                file_name="reporte_final_valorizado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
