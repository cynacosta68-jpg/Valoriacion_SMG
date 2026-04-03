import streamlit as st
import pandas as pd
import io

# Configuración inicial de la página
st.set_page_config(layout="wide", page_title="Valorizador SMG - Versión Colab")

st.title("🏥 Sistema de Valorización Médica (SMG)")
st.markdown("Esta aplicación replica la lógica 1:1 de tu Notebook de Google Colab para procesar y valorizar liquidaciones.")

# --- Bloque 1: Limpieza del Reporte de Liquidación ---
st.header("Bloque 1: Limpieza del Reporte de Liquidación")

uploaded_file_liquidacion = st.file_uploader(
    "🚀 Paso 1: Sube el archivo 'Reporte de Liquidacion 1'",
    type=['csv', 'xlsx'],
    key="liquidacion_file"
)

if uploaded_file_liquidacion is not None:
    try:
        file_name = uploaded_file_liquidacion.name
        if file_name.endswith('.csv'):
            try:
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=';')
            except:
                uploaded_file_liquidacion.seek(0)
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=',')
        else:
            df = pd.read_excel(uploaded_file_liquidacion)

        # 1. Definir columnas a eliminar (Exacto al Colab)
        columnas_a_borrar = [
            'prestador', 'Razón_Social', 'cuit', 'transacción_ticket',
            'fecha_prestacion', 'transacción_tipo', 'autorización',
            'efector', 'efector_matricula', 'prescriptor',
            'prescriptor_matricula', 'prescriptor_razón_social',
            'icd', 'terminal', 'terminal_domicilio'
        ]
        columnas_presentes = [col for col in columnas_a_borrar if col in df.columns]
        df_limpio = df.drop(columns=columnas_presentes, errors='ignore').dropna(how='all')

        # NORMALIZACIÓN CRÍTICA DE CUIT: Elimina el .0 que Streamlit agrega a veces
        if 'efector_cuit' in df_limpio.columns:
            df_limpio['efector_cuit'] = df_limpio['efector_cuit'].astype(str).str.split('.').str[0].str.strip()

        st.session_state['df_bloque_2'] = df_limpio
        st.success(f"✅ Paso 1 completado. Reporte procesado con {len(df_limpio)} filas.")
    except Exception as e:
        st.error(f"❌ Error en Bloque 1: {e}")

# --- Bloque 2: Integración de Datos Adicionales (Evweb) ---
st.header("Bloque 2: Integración de Datos Adicionales")

if 'df_bloque_2' in st.session_state:
    uploaded_file_valorizacion = st.file_uploader(
        "🚀 Paso 2: Sube el archivo 'Base de datos_valorizacion.xlsx'",
        type=['xlsx'],
        key="valorizacion_file"
    )

    if uploaded_file_valorizacion is not None:
        try:
            xls = pd.ExcelFile(uploaded_file_valorizacion)
            sheet_names = xls.sheet_names

            # Cargar Base Evweb (priorizando nombre exacto o similar)
            nombre_ho_evweb = next((s for s in sheet_names if 'evweb' in s.lower()), sheet_names[0])
            df_evweb = pd.read_excel(xls, sheet_name=nombre_ho_evweb)

            # Normalizar CUIT en la base de datos
            if 'CUIT' in df_evweb.columns:
                df_evweb['CUIT'] = df_evweb['CUIT'].astype(str).str.split('.').str[0].str.strip()

            # Merge por CUIT
            df_merged = st.session_state['df_bloque_2'].merge(df_evweb, left_on='efector_cuit', right_on='CUIT', how='left')

            # Creación de nuevas columnas según lógica Colab
            df_merged['cuenta_matricula'] = df_merged.get('Matricula')
            df_merged['especialidad_medica'] = df_merged.get('Especialidad')

            # Lógica de Categoría (Prioridad: Arancel -> Categoria -> Matricula)
            if 'Arancel' in df_merged.columns:
                df_merged['categoria'] = df_merged['Arancel']
            elif 'Categoria' in df_merged.columns:
                df_merged['categoria'] = df_merged['Categoria']
            elif 'Matricula Arancel' in df_merged.columns:
                df_merged['categoria'] = df_merged['Matricula Arancel']
            else:
                df_merged['categoria'] = df_merged.get('Matricula')

            # Lógica IVA y OS
            df_merged['IVA_Template'] = df_merged.apply(lambda r: '0' if (str(r.get('Responsabilidad Fiscal')) in ['Monotributo', 'Exento'] or r.get('condición_iva') == 'Exento') else '1', axis=1)
            df_merged['Tipo_OS'] = df_merged.apply(lambda r: '11062' if str(r.get('Responsabilidad Fiscal')) == 'Responsable Inscripto' else '11060', axis=1)

            # Mantener columnas originales + las nuevas calculadas
            cols_finales = st.session_state['df_bloque_2'].columns.tolist() + ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']
            st.session_state['df_final'] = df_merged[cols_finales].copy()
            st.session_state['xls_valorizacion_raw'] = uploaded_file_valorizacion

            st.success("✅ Paso 2 completado: Integración de prestadores exitosa.")
        except Exception as e:
            st.error(f"❌ Error en Bloque 2: {e}")

# --- Bloque 3: Valorización Definitiva ---
st.header("Bloque 3: Valorización")

if 'df_final' in st.session_state:
    if st.button("🚀 Ejecutar Valorización Definitiva"):
        try:
            # 1. Carga de datos desde el estado de sesión
            df_f = st.session_state['df_final'].copy()
            
            # Limpieza de duplicados inicial (Clave para mantener 8289 registros si aplica)
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

            xls_v = pd.ExcelFile(st.session_state['xls_valorizacion_raw'])
            db_val = pd.read_excel(xls_v, sheet_name=None)
            df_nom = db_val['Nomenclador'].copy()
            df_uni = db_val['unidades'].copy()
            df_fijos = db_val['Valor Fijos'].copy()

            # 2. Normalización de códigos y periodos
            def limpiar(x):
                if pd.isna(x): return ""
                return str(x).split('.')[0].strip().upper()

            df_f['prest_limpia'] = df_f['prestación'].apply(limpiar)
            df_f['cat_limpia'] = df_f['categoria'].apply(limpiar)
            df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar)
            df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar)
            df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar)

            # Fechas a periodos (Formato YYYY-MM)
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

            # --- CONSOLIDACIÓN (Lógica exacta de cascada del Colab) ---
            def consolidar(row):
                if pd.notna(row['IMPORTE_R1']): return row['IMPORTE_R1']
                if pd.notna(row['Total prestación']): return row['Total prestación'] # R2A
                if pd.notna(row['Total prestación_R2B']): return row['Total prestación_R2B']
                if pd.notna(row['Total prestación_R3']): return row['Total prestación_R3']
                return "#REVISAR VALORES"

            df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)

            # Cálculo de columna Total
            def calcular_total(row):
                try: return float(row['IMPORTE']) * float(row['cantidad'])
                except: return row['IMPORTE']

            df_f['Total'] = df_f.apply(calcular_total, axis=1)

            # Limpieza final de columnas auxiliares
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
            prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador', 'Tipo de nomenclador', 'Código']
            cols_a_borrar = [c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']]
            df_final_res = df_f.drop(columns=cols_a_borrar, errors='ignore')

            # Resultado visual
            revisar_count = (df_final_res['IMPORTE'] == '#REVISAR VALORES').sum()
            if revisar_count > 0:
                st.warning(f"🔍 Atención: Se encontraron {revisar_count} registros que requieren revisión manual (#REVISAR VALORES).")
            else:
                st.success(f"✅ ¡Éxito! Valorización completada al 100%. {len(df_final_res)} registros procesados.")
            
            st.dataframe(df_final_res.head(100))

            # Preparar descarga
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_final_res.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Descargar Reporte FINAL VALORIZADO",
                data=output.getvalue(),
                file_name="reporte_valorizado_final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Error en Valorización: {e}")
