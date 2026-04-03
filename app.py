import streamlit as st
import pandas as pd
import io

st.set_page_config(layout="wide")
st.title("Aplicación de Procesamiento y Valorización de Reportes")

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

        columnas_a_borrar = [
            'prestador', 'Razón_Social', 'cuit', 'transacción_ticket',
            'fecha_prestacion', 'transacción_tipo', 'autorización',
            'efector', 'efector_matricula', 'prescriptor',
            'prescriptor_matricula', 'prescriptor_razón_social',
            'icd', 'terminal', 'terminal_domicilio'
        ]
        columnas_presentes = [col for col in columnas_a_borrar if col in df.columns]
        df_limpio = df.drop(columns=columnas_presentes, errors='ignore').dropna(how='all')
        df_limpio['cantidad'] = pd.to_numeric(df_limpio['cantidad'], errors='coerce').fillna(0)

        st.session_state['df_bloque_2'] = df_limpio
        st.success("✅ Paso 1 completado: Reporte de liquidación procesado.")
    except Exception as e:
        st.error(f"❌ Error en Bloque 1: {e}")

# --- Bloque 2: Integración de Datos Adicionales ---
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
            
            # Cargar Base Evweb
            nombre_hoja_evweb = next((s for s in sheet_names if 'evweb' in s.lower()), sheet_names[0])
            df_evweb = pd.read_excel(xls, sheet_name=nombre_hoja_evweb)

            df_merged = st.session_state['df_bloque_2'].merge(df_evweb, left_on='efector_cuit', right_on='CUIT', how='left')
            
            df_merged['cuenta_matricula'] = df_merged.get('Matricula')
            df_merged['especialidad_medica'] = df_merged.get('Especialidad')
            
            # Lógica Categoría
            if 'Categoria' in df_merged.columns: df_merged['categoria'] = df_merged['Categoria']
            elif 'Arancel' in df_merged.columns: df_merged['categoria'] = df_merged['Arancel']
            else: df_merged['categoria'] = df_merged.get('Matricula')

            df_merged['IVA_Template'] = df_merged.apply(lambda r: '0' if (str(r.get('Responsabilidad Fiscal')) in ['Monotributo', 'Exento'] or r.get('condición_iva') == 'Exento') else '1', axis=1)
            df_merged['Tipo_OS'] = df_merged.apply(lambda r: '11062' if r.get('Responsabilidad Fiscal') == 'Responsable Inscripto' else '11060', axis=1)

            cols_finales = st.session_state['df_bloque_2'].columns.tolist() + ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']
            st.session_state['df_final'] = df_merged[cols_finales].copy()
            st.session_state['xls_valorizacion'] = uploaded_file_valorizacion
            
            st.success("✅ Paso 2 completado: Datos integrados.")
        except Exception as e:
            st.error(f"❌ Error en Bloque 2: {e}")

# --- Bloque 3: Valorización ---
st.header("Bloque 3: Valorización")

if 'df_final' in st.session_state:
    if st.button("🚀 Ejecutar Valorización Definitiva"):
        try:
            df_f = st.session_state['df_final'].copy()
            xls_v = pd.ExcelFile(st.session_state['xls_valorizacion'])
            
            df_nom = pd.read_excel(xls_v, sheet_name='Nomenclador')
            df_uni = pd.read_excel(xls_v, sheet_name='unidades')
            df_fijos = pd.read_excel(xls_v, sheet_name='Valor Fijos')

            def limpiar(x): return str(x).split('.')[0].strip().upper() if pd.notna(x) else ""

            df_f['prest_limpia'] = df_f['prestación'].apply(limpiar)
            df_f['cat_limpia'] = df_f['categoria'].apply(limpiar)
            df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar)
            df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar)
            df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar)

            # Conversión de periodos idéntica al Bloque que funcionó
            df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
            df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
            df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

            # REGLA 1: NOMENCLADOR + UNIDADES
            df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
            df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
            df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
            df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']], left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

            # REGLA 2 & 3: VALOR FIJOS
            f_filt_sm = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=False, case=False)].copy()
            df_f = pd.merge(df_f, f_filt_sm.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left')
            df_f = pd.merge(df_f, f_filt_sm.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])[['cod_limpio', 'periodo_aux', 'Total prestación']], on=['cod_limpio', 'periodo_aux'], how='left', suffixes=('', '_R2B'))
            df_f = pd.merge(df_f, df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], on=['cod_limpio', 'cat_limpia', 'periodo_aux'], how='left', suffixes=('', '_R3'))

            def consolidar(r):
                if pd.notna(r.get('IMPORTE_R1')): return r['IMPORTE_R1']
                if pd.notna(r.get('Total prestación')): return r['Total prestación']
                if pd.notna(r.get('Total prestación_R2B')): return r['Total prestación_R2B']
                if pd.notna(r.get('Total prestación_R3')): return r['Total prestación_R3']
                return "#REVISAR VALORES"

            df_f['IMPORTE'] = df_f.apply(consolidar, axis=1)
            df_f['Total'] = df_f.apply(lambda r: float(r['IMPORTE'])*float(r['cantidad']) if isinstance(r['IMPORTE'], (int, float)) else r['IMPORTE'], axis=1)

            prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador', 'Tipo de nomenclador', 'Código']
            df_final_res = df_f.drop(columns=[c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']], errors='ignore')

            st.success(f"✅ Valorización completada. {len(df_final_res)} registros.")
            st.dataframe(df_final_res.head())

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_final_res.to_excel(writer, index=False)
            st.download_button("Descargar Reporte FINAL", output.getvalue(), "reporte_FINAL_VALORIZADO.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"❌ Error en Valorización: {e}")
