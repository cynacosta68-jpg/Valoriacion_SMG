import streamlit as st
import pandas as pd
import io

# Configuración inicial
st.set_page_config(layout="wide", page_title="Procesador SMG Final")

st.title("🏥 Procesador y Valorizador SMG")
st.markdown("Lógica de Colab con Limpieza de Tipos de Datos (Anti-Errores de Cruce)")

# --- FUNCIONES DE LIMPIEZA TÉCNICA ---
def forzar_texto_limpio(x):
    """Convierte a string, quita el .0 si es un número y limpia espacios."""
    if pd.isna(x): return ""
    s = str(x).strip()
    if s.endswith('.0'): s = s[:-2]
    return s.upper()

def limpiar_codigo_practica(x):
    """Limpia códigos de prestación como en tu Colab."""
    if pd.isna(x): return ""
    return str(x).split('.')[0].strip().upper()

# --- Bloque 1: Limpieza del Reporte ---
st.header("Bloque 1: Limpieza del Reporte")

uploaded_file_liquidacion = st.file_uploader("🚀 Paso 1: Sube el reporte de liquidación", type=['csv', 'xlsx'])

if uploaded_file_liquidacion is not None:
    try:
        if uploaded_file_liquidacion.name.endswith('.csv'):
            try:
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=';')
            except:
                uploaded_file_liquidacion.seek(0)
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=',')
        else:
            df = pd.read_excel(uploaded_file_liquidacion)

        columnas_a_borrar = [
            'prestador', 'Razón_Social', 'cuit', 'transacción_ticket',
            'fecha_prestacion', 'transacción_tipo', 'autorización',
            'efector', 'efector_matricula', 'prescriptor',
            'prescriptor_matricula', 'prescriptor_razón_social',
            'icd', 'terminal', 'terminal_domicilio'
        ]
        df_limpio = df.drop(columns=[c for c in columnas_a_borrar if c in df.columns], errors='ignore').dropna(how='all')
        
        st.session_state['df_bloque_2'] = df_limpio
        st.success(f"✅ Reporte cargado: {len(df_limpio)} filas.")
    except Exception as e:
        st.error(f"❌ Error en Bloque 1: {e}")

# --- Bloque 2: Integración con Base Evweb ---
st.header("Bloque 2: Integración con Base Evweb")

if 'df_bloque_2' in st.session_state:
    uploaded_file_valorizacion = st.file_uploader("🚀 Paso 2: Sube la 'Base de datos_valorizacion.xlsx'", type=['xlsx'])

    if uploaded_file_valorizacion is not None:
        try:
            xls = pd.ExcelFile(uploaded_file_valorizacion)
            nombre_hoja = next((s for s in xls.sheet_names if 'evweb' in s.lower()), xls.sheet_names[0])
            df_evweb = pd.read_excel(xls, sheet_name=nombre_hoja)
            
            # NORMALIZACIÓN DE CUIT (Forzar match perfecto)
            df_reporte = st.session_state['df_bloque_2'].copy()
            df_reporte['efector_cuit'] = df_reporte['efector_cuit'].apply(forzar_texto_limpio)
            df_evweb['CUIT'] = df_evweb['CUIT'].apply(forzar_texto_limpio)

            # Unión
            df_merged = df_reporte.merge(df_evweb, left_on='efector_cuit', right_on='CUIT', how='left')

            # Columnas del profesional
            df_merged['cuenta_matricula'] = df_merged.get('Matricula')
            df_merged['especialidad_medica'] = df_merged.get('Especialidad')
            
            # Determinación de categoría (Arancel)
            if 'Arancel' in df_merged.columns: df_merged['categoria'] = df_merged['Arancel']
            elif 'Categoria' in df_merged.columns: df_merged['categoria'] = df_merged['Categoria']
            else: df_merged['categoria'] = df_merged.get('Matricula')

            # IVA y OS (Lógica Colab)
            df_merged['IVA_Template'] = df_merged.apply(lambda r: '0' if (str(r.get('Responsabilidad Fiscal')) in ['Monotributo', 'Exento'] or r.get('condición_iva') == 'Exento') else '1', axis=1)
            df_merged['Tipo_OS'] = df_merged.apply(lambda r: '11062' if str(r.get('Responsabilidad Fiscal')) == 'Responsable Inscripto' else '11060', axis=1)

            cols_finales = df_reporte.columns.tolist() + ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']
            st.session_state['df_final'] = df_merged[cols_finales].copy()
            st.session_state['xls_path'] = uploaded_file_valorizacion
            
            st.success("✅ Datos de prestadores vinculados correctamente.")
        except Exception as e:
            st.error(f"❌ Error en Bloque 2: {e}")

# --- Bloque 3: Valorización (Lógica Estricta de Importe) ---
st.header("Bloque 3: Valorización")

if 'df_final' in st.session_state:
    if st.button("🚀 Ejecutar Valorización Definitiva"):
        try:
            df_f = st.session_state['df_final'].copy()
            # Mantener 8289 registros únicos
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

            xls_v = pd.ExcelFile(st.session_state['xls_path'])
            df_nom = pd.read_excel(xls_v, sheet_name='Nomenclador')
            df_uni = pd.read_excel(xls_v, sheet_name='unidades')
            df_fijos = pd.read_excel(xls_v, sheet_name='Valor Fijos')

            # Normalización de códigos y categorías
            df_f['prest_limpia'] = df_f['prestación'].apply(limpiar_codigo_practica)
            df_f['cat_limpia'] = df_f['categoria'].apply(forzar_texto_limpio)
            
            df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar_codigo_practica)
            df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar_codigo_practica)
            df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(forzar_texto_limpio)

            # Sincronización de Periodos
            df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
            df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
            df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

            # --- REGLA 1: NOMENCLADOR ---
            df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
            df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
            df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
            
            df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']], 
                            left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

            # --- REGLA 2: VALOR FIJOS SWISS ---
            f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=False, case=False)].copy()
            
            # Match A: Código + Categoría + Periodo
            f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
            df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left')

            # Match B: Código + Periodo (R2B)
            f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
            df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left', suffixes=('', '_R2B'))

            # --- REGLA 3: RESTO DE FIJOS ---
            f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
            df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left', suffixes=('', '_R3'))

            # --- CONSOLIDACIÓN FINAL (Idéntica a Colab) ---
            def consolidar_importe(row):
                if pd.notna(row.get('IMPORTE_R1')): return row['IMPORTE_R1']
                if pd.notna(row.get('Total prestación')): return row['Total prestación']
                if pd.notna(row.get('Total prestación_R2B')): return row['Total prestación_R2B']
                if pd.notna(row.get('Total prestación_R3')): return row['Total prestación_R3']
                return "#REVISAR VALORES"

            df_f['IMPORTE'] = df_f.apply(consolidar_importe, axis=1)
            
            # Cálculo de Total
            def calcular_total(row):
                try:
                    if row['IMPORTE'] == "#REVISAR VALORES": return "#REVISAR VALORES"
                    return float(row['IMPORTE']) * float(row['cantidad'])
                except: return row['IMPORTE']

            df_f['Total'] = df_f.apply(calcular_total, axis=1)

            # Limpiar columnas técnicas
            prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador']
            df_final_res = df_f.drop(columns=[c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']], errors='ignore')

            st.success("✅ Valorización terminada.")
            st.dataframe(df_final_res.head(100))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_final_res.to_excel(writer, index=False)
            st.download_button("📥 Descargar Reporte FINAL", output.getvalue(), "reporte_valorizado.xlsx")
            
        except Exception as e:
            st.error(f"❌ Error en Valorización: {e}")
