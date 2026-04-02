import streamlit as st
import pandas as pd
import io

# Configuración de página
st.set_page_config(page_title="Procesador de Valorización SMG", layout="wide")

st.title("🏥 Sistema de Valorización Médica")
st.markdown("Esta aplicación procesa reportes de liquidación basándose en tu lógica original de Colab.")

# --- FUNCIONES DE LIMPIEZA (Extraídas de tu Colab) ---
def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

def limpiar_texto(x):
    if pd.isna(x): return ""
    return str(x).strip().upper()

def procesar_datos(df_liqui, db_valor):
    try:
        # 1. Limpieza Inicial (Evitar duplicados en la transacción)
        df_f = df_liqui.drop_duplicates(subset=['transacción_item'], keep='first').copy()
        
        # 2. Cargar hojas de la base de valorización
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        # 3. Normalización (Basado en tu lógica de celdas de Colab)
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        
        # Detección flexible de columna categoría
        col_cat = next((c for c in df_f.columns if c.lower() in ['categoria', 'categoría']), None)
        if col_cat:
            df_f['cat_limpia'] = df_f[col_cat].apply(limpiar_texto)
        else:
            df_f['cat_limpia'] = ""

        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar_texto)

        # Periodos para el cruce
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

        # --- LÓGICA DE VALORIZACIÓN ---
        
        # Regla 1: Nomenclador + Unidades
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
        
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']],
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

        # Regla 2: Valor Fijos (Swiss Medical)
        f_filt = df_fijos[df_fijos['Nomenclador'].str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        
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

        # Consolidación de Importe
        def consolidar(row):
            if pd.notna(row['IMPORTE_R1']): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
            return "#REVISAR"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        
        # Cálculo de Total
        df_f['Total'] = pd.to_numeric(df_f['IMPORTE'], errors='coerce') * pd.to_numeric(df_f['cantidad'], errors='coerce')
        df_f['Total'] = df_f['Total'].fillna(df_f['IMPORTE'])

        # Limpieza de columnas técnicas antes de devolver
        cols_aux = ['prest_limpia', 'cat_limpia', 'periodo_aux', 'cod_limpio', 'IMPORTE_R1', 'Total prestación', 'Total prestación_R2B']
        df_final = df_f.drop(columns=[c for c in cols_aux if c in df_f.columns])
        
        return df_final

    except Exception as e:
        st.error(f"Error procesando los datos: {e}")
        return None

# --- INTERFAZ STREAMLIT ---
st.sidebar.header("Carga de Archivos")
file_rep = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
file_val = st.sidebar.file_uploader("2. Base de Valorización", type=["xlsx"])

if file_rep and file_val:
    if st.button("🚀 Ejecutar Procesamiento"):
        with st.spinner("Procesando como en Colab..."):
            # Leer Liquidación
            if file_rep.name.endswith('.csv'):
                df_l = pd.read_csv(file_rep, encoding='latin1', sep=None, engine='python')
            else:
                df_l = pd.read_excel(file_rep)
            
            # Leer Base de Valorización (todas las hojas)
            db_v = pd.read_excel(file_val, sheet_name=None)
            
            resultado = procesar_datos(df_l, db_v)
            
            if resultado is not None:
                st.success(f"Proceso finalizado. Registros procesados: {len(resultado)}")
                st.dataframe(resultado.head(100))
                
                # Descarga
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    resultado.to_excel(writer, index=False)
                
                st.download_button(
                    label="📥 Descargar Resultado Excel",
                    data=output.getvalue(),
                    file_name="reporte_valorizado_final.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
else:
    st.info("Por favor, sube ambos archivos en la barra lateral para comenzar.")
