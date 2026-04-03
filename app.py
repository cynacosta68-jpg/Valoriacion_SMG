import streamlit as st
import pandas as pd
import io
import unicodedata

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG - Flujo Completo", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Este sistema ejecuta la unión con la base de prestadores y las reglas de valorización.")

# --- FUNCIONES DE APOYO ---
def normalizar_encabezado(texto):
    if not isinstance(texto, str): return str(texto)
    texto = texto.lower().strip()
    texto = "".join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    return texto

def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

# --- LÓGICA DE IVA Y OS (Tus funciones de Colab) ---
def calcular_iva_template(row):
    if pd.notna(row.get('Responsabilidad Fiscal')) and (row['Responsabilidad Fiscal'] in ['Monotributo', 'Exento']):
        return '0'
    elif pd.notna(row.get('condición_iva')) and row['condición_iva'] == 'Exento':
        return '0'
    else:
        return '1'

def calcular_tipo_os(row):
    if pd.notna(row.get('Responsabilidad Fiscal')) and row['Responsabilidad Fiscal'] == 'Responsable Inscripto':
        return '11062'
    else:
        return '11060'

# --- PROCESO PRINCIPAL ---
def ejecutar_proceso_total(df_subido, db_valor):
    try:
        # ---------------------------------------------------------
        # PASO 1: LIMPIEZA INICIAL
        # ---------------------------------------------------------
        df_bloque_2 = df_subido.copy()
        df_bloque_2.columns = [str(c).strip().lower() for c in df_bloque_2.columns]
        df_bloque_2 = df_bloque_2.drop_duplicates(subset=['transacción_item'], keep='first')

        # ---------------------------------------------------------
        # PASO 2: UNIÓN CON EVWEB (El bloque que compartiste)
        # ---------------------------------------------------------
        # Asumimos que 'evweb' es una pestaña del Excel de valorización
        if 'evweb' not in db_valor:
            st.error("❌ El Excel de base de datos debe tener una pestaña llamada 'evweb' con la info de prestadores.")
            return None
        
        df_evweb = db_valor['evweb'].copy()
        
        # Merge por CUIT
        df_merged = df_bloque_2.merge(df_evweb, left_on='efector_cuit', right_on='CUIT', how='left')

        # Crear nuevas columnas (Réplica exacta de tu código)
        df_merged['cuenta_matricula'] = df_merged.get('Matricula')
        df_merged['especialidad_medica'] = df_merged.get('Especialidad')

        # Lógica de categoría
        if 'Categoria' in df_merged.columns:
            df_merged['categoria'] = df_merged['Categoria']
        elif 'Matricula Arancel' in df_merged.columns:
            df_merged['categoria'] = df_merged['Matricula Arancel']
        elif 'Arancel' in df_merged.columns:
            df_merged['categoria'] = df_merged['Arancel']
        else:
            df_merged['categoria'] = df_merged.get('Matricula')

        # Aplicar funciones de IVA y OS
        df_merged['IVA_Template'] = df_merged.apply(calcular_iva_template, axis=1)
        df_merged['Tipo_OS'] = df_merged.apply(calcular_tipo_os, axis=1)

        # Seleccionar columnas finales para esta etapa
        cols_finales_pre = df_bloque_2.columns.tolist() + ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']
        df_f = df_merged[cols_finales_pre].copy()

        # ---------------------------------------------------------
        # PASO 3: VALORIZACIÓN (Reglas Nomenclador y Fijos)
        # ---------------------------------------------------------
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        # Normalización para cruce
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        df_f['cat_limpia'] = df_f['categoria'].astype(str).str.strip().str.upper()
        
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].astype(str).str.strip().str.upper()

        # Fechas
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # Aplicar Reglas (R1, R2, R3)
        # ... (Aquí sigue la lógica de merge que ya teníamos)
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left', suffixes=('', '_R2'))

        # Consolidación Final
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            return "#REVISAR"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        df_f['Total'] = pd.to_numeric(df_f['IMPORTE'], errors='coerce') * pd.to_numeric(df_f['cantidad'], errors='coerce')

        # Limpiar rastro técnico
        aux = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación']
        return df_f[[c for c in df_f.columns if not any(a in c for a in aux)]]

    except Exception as e:
        st.error(f"Error: {e}")
        return None

# --- UI STREAMLIT ---
st.sidebar.header("📁 Archivos")
f1 = st.sidebar.file_uploader("1. Reporte", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base Valorización (con pestaña evweb)", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Procesar Todo"):
        df_l = pd.read_excel(f1) if f1.name.endswith('.xlsx') else pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        db_v = pd.read_excel(f2, sheet_name=None)
        res = ejecutar_proceso_total(df_l, db_v)
        if res is not None:
            st.dataframe(res.head(50))
            output = io.BytesIO()
            with pd.ExcelWriter(output) as w: res.to_excel(w, index=False)
            st.download_button("📥 Descargar", output.getvalue(), "final.xlsx")
