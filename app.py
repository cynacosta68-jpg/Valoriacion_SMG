import streamlit as st
import pandas as pd
import io
import unicodedata

# Configuración de la página
st.set_page_config(page_title="Valorizador SMG Profesional", layout="wide")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Flujo completo: Limpieza → Unión con Base Evweb → Lógica IVA/OS → Valorización.")

# --- FUNCIONES DE LÓGICA FISCAL (Réplica de tu Colab) ---
def calcular_iva_template(row):
    resp_fiscal = str(row.get('Responsabilidad Fiscal', '')).strip()
    cond_iva = str(row.get('condición_iva', '')).strip()
    if resp_fiscal in ['Monotributo', 'Exento'] or cond_iva == 'Exento':
        return '0'
    else:
        return '1'

def calcular_tipo_os(row):
    resp_fiscal = str(row.get('Responsabilidad Fiscal', '')).strip()
    if resp_fiscal == 'Responsable Inscripto':
        return '11062'
    else:
        return '11060'

def limpiar_codigo(x):
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

# --- PROCESO PRINCIPAL ---
def ejecutar_proceso_total(df_subido, db_valor):
    try:
        # 1. PASO 1: LIMPIEZA INICIAL (BLOQUE 1 COLAB)
        df_bloque_2 = df_subido.copy()
        df_bloque_2.columns = [str(c).strip().lower() for c in df_bloque_2.columns]
        
        # Eliminar columnas sobrantes
        cols_borrar = [
            'nro_internacion', 'nro_beneficiario', 'nro_orden', 'fecha_desde', 
            'fecha_hasta', 'id_especialidad', 'id_prestacion', 'nro_factura_reemplazo',
            'id_entidad_intermedia', 'nro_lote', 'nro_factura', 'estado', 'periodo_prestacion'
        ]
        df_bloque_2 = df_bloque_2.drop(columns=[c for c in cols_borrar if c in df_bloque_2.columns])
        
        # Limpieza de duplicados por transacción_item
        if 'transacción_item' in df_bloque_2.columns:
            df_bloque_2 = df_bloque_2.drop_duplicates(subset=['transacción_item'], keep='first')

        # 2. PASO 2: UNIÓN CON BASE EVWEB (TU BLOQUE COMPARTIDO)
        nombre_hoja_evweb = 'Base Evweb'
        if nombre_hoja_evweb not in db_valor:
            st.error(f"❌ No se encontró la pestaña '{nombre_hoja_evweb}' en el archivo de base de datos.")
            return None
        
        df_evweb = db_valor[nombre_hoja_evweb].copy()
        
        # Asegurar que los CUIT sean strings para el merge
        df_bloque_2['efector_cuit'] = df_bloque_2['efector_cuit'].astype(str).str.strip()
        df_evweb['CUIT'] = df_evweb['CUIT'].astype(str).str.strip()
        
        # Merge (Union con prestadores)
        df_merged = df_bloque_2.merge(df_evweb, left_on='efector_cuit', right_on='CUIT', how='left')

        # Crear nuevas columnas según tu código
        df_merged['cuenta_matricula'] = df_merged.get('Matricula')
        df_merged['especialidad_medica'] = df_merged.get('Especialidad')

        # Lógica de categoría (Arancel)
        if 'Arancel' in df_merged.columns:
            df_merged['categoria'] = df_merged['Arancel']
        elif 'Categoria' in df_merged.columns:
            df_merged['categoria'] = df_merged['Categoria']
        elif 'Matricula Arancel' in df_merged.columns:
            df_merged['categoria'] = df_merged['Matricula Arancel']
        else:
            df_merged['categoria'] = df_merged.get('Matricula')

        # Aplicar lógica condicional para IVA y OS
        df_merged['IVA_Template'] = df_merged.apply(calcular_iva_template, axis=1)
        df_merged['Tipo_OS'] = df_merged.apply(calcular_tipo_os, axis=1)

        # Seleccionar columnas originales + las 5 nuevas
        cols_finales_bloque = df_bloque_2.columns.tolist() + \
                              ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']
        df_f = df_merged[cols_finales_bloque].copy()

        # 3. PASO 3: VALORIZACIÓN
        df_nom = db_valor['Nomenclador'].copy()
        df_uni = db_valor['unidades'].copy()
        df_fijos = db_valor['Valor Fijos'].copy()

        # Normalización para cruce de precios
        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo)
        df_f['cat_limpia'] = df_f['categoria'].astype(str).str.strip().str.upper()
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
        
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].astype(str).str.strip().str.upper()
        df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')
        df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')

        # Regla 1: Nomenclador
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux_y'])
        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'IMPORTE_R1']], left_on=['prest_limpia'], right_on=['cod_limpio'], how='left')

        # Regla 2: Fijos Swiss
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=True, case=False)].copy()
        f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
        df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                        left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left', suffixes=('', '_R2'))

        # Consolidación final de precios
        def consolidar(row):
            if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
            if pd.notna(row.get('Total prestación')): return row['Total prestación']
            return "#REVISAR"

        df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
        df_f['Total'] = pd.to_numeric(df_f['IMPORTE'], errors='coerce') * pd.to_numeric(df_f['cantidad'], errors='coerce')

        # Limpiar columnas auxiliares
        aux_cols = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación']
        cols_finales = [c for c in df_f.columns if not any(a in c for a in aux_cols)]
        
        return df_f[cols_finales]

    except Exception as e:
        st.error(f"Error en el proceso: {e}")
        return None

# --- INTERFAZ STREAMLIT ---
st.sidebar.header("Carga de Archivos")
f1 = st.sidebar.file_uploader("1. Reporte de Liquidación", type=["xlsx", "csv"])
f2 = st.sidebar.file_uploader("2. Base de Datos (con hoja Base Evweb)", type=["xlsx"])

if f1 and f2:
    if st.button("🚀 Ejecutar Proceso Completo"):
        df_l = pd.read_excel(f1) if f1.name.endswith('.xlsx') else pd.read_csv(f1, encoding='latin1', sep=None, engine='python')
        db_v = pd.read_excel(f2, sheet_name=None)
        
        with st.spinner("Uniendo prestadores y valorizando..."):
            res = ejecutar_proceso_total(df_l, db_v)
            
        if res is not None:
            st.success(f"✅ Finalizado. Registros: {len(res)}")
            st.dataframe(res.head(50))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                res.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte Final", output.getvalue(), "reporte_valorizado_final.xlsx")
