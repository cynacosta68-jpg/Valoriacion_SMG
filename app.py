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

df_bloque_2 = None
if uploaded_file_liquidacion is not None:
    st.info(f"Archivo subido: {uploaded_file_liquidacion.name}")

    try:
        # Leer el archivo (soporta CSV y Excel)
        file_name = uploaded_file_liquidacion.name
        if file_name.endswith('.csv'):
            # Probamos con separador coma y punto y coma por las dudas
            try:
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=';')
            except Exception:
                uploaded_file_liquidacion.seek(0) # Reset stream position
                df = pd.read_csv(uploaded_file_liquidacion, encoding='latin1', sep=',')
        else:
            df = pd.read_excel(uploaded_file_liquidacion)

        st.success("Archivo leído correctamente.")

        # Definir columnas a eliminar según tu pedido
        columnas_a_borrar = [
            'prestador', 'Razón_Social', 'cuit', 'transacción_ticket',
            'fecha_prestacion', 'transacción_tipo', 'autorización',
            'efector', 'efector_matricula', 'prescriptor',
            'prescriptor_matricula', 'prescriptor_razón_social',
            'icd', 'terminal', 'terminal_domicilio'
        ]

        # Identificar cuáles de esas columnas están realmente presentes
        columnas_presentes = [col for col in columnas_a_borrar if col in df.columns]

        # Ejecutar la limpieza
        df_limpio = df.drop(columns=columnas_presentes, errors='ignore')

        # Limpieza extra: quitar filas totalmente vacías si existieran
        df_limpio = df_limpio.dropna(how='all')

        # Convert 'cantidad' to numeric and fill NaNs with 0 to prevent calculation errors
        df_limpio['cantidad'] = pd.to_numeric(df_limpio['cantidad'], errors='coerce')
        df_limpio['cantidad'] = df_limpio['cantidad'].fillna(0)

        df_bloque_2 = df_limpio # Assign to df_bloque_2 for the next step

        st.subheader("✅ REPORTE DE LIMPIEZA")
        st.write(f"Archivo procesado: {file_name}")
        st.write(f"Columnas eliminadas: {len(columnas_presentes)}")
        st.write(f"Columnas restantes: {len(df_limpio.columns)}")
        st.write(f"Filas totales: {len(df_limpio)}")
        st.write("--- COLUMNAS QUE QUEDARON ---")
        st.write(list(df_limpio.columns))
        st.write("--- VISTA PREVIA (PRIMERAS 5 FILAS) ---")
        st.dataframe(df_limpio.head())

        # Store df_bloque_2 in session_state for subsequent steps
        st.session_state['df_bloque_2'] = df_bloque_2

    except Exception as e:
        st.error(f"❌ Error al procesar el archivo de liquidación: {e}")

# --- Bloque 2: Integración de Datos Adicionales ---
st.header("Bloque 2: Integración de Datos Adicionales")

if 'df_bloque_2' in st.session_state and st.session_state['df_bloque_2'] is not None:
    st.markdown("Ahora, buscamos los datos que nos faltan para avanzar al bloque 3: cuenta, especialidad, categoria y el IVA del template con el tipo de OS.")

    uploaded_file_valorizacion = st.file_uploader(
        "🚀 Paso 2: Sube el archivo 'Base de datos_valorizacion.xlsx'",
        type=['xlsx'],
        key="valorizacion_file"
    )

    df_final = None
    if uploaded_file_valorizacion is not None:
        st.info(f"Archivo subido: {uploaded_file_valorizacion.name}")
        try:
            # Store the uploaded file object itself in session_state for later use
            st.session_state['uploaded_file_valorizacion_object'] = uploaded_file_valorizacion

            xls = pd.ExcelFile(uploaded_file_valorizacion)
            sheet_names = xls.sheet_names

            st.write("Hojas disponibles en 'Base de datos_valorizacion.xlsx':")
            for sheet_name in sheet_names:
                st.write(f"- {sheet_name}")

            if 'Base evweb' in sheet_names:
                df_evweb = pd.read_excel(xls, sheet_name='Base evweb')
                st.success("Se cargó la hoja 'Base evweb'.")
            elif sheet_names:
                df_evweb = pd.read_excel(xls, sheet_name=sheet_names[0])
                st.warning(f"La hoja 'Base evweb' no fue encontrada. Se cargó la primera hoja disponible: '{sheet_names[0]}'. Por favor, verifica si esta es la hoja correcta.")
            else:
                st.error("No se encontraron hojas en el archivo Excel de valorización.")
                df_evweb = pd.DataFrame()

            if not df_evweb.empty:
                st.write("Columnas en 'df_evweb':")
                st.write(df_evweb.columns.tolist())
                st.write("Primeras 5 filas de 'df_evweb':")
                st.dataframe(df_evweb.head())

                # 2. Unir df_bloque_2 con df_evweb usando 'efector_cuit' y 'CUIT'
                df_merged = st.session_state['df_bloque_2'].merge(df_evweb,
                                                                  left_on='efector_cuit',
                                                                  right_on='CUIT',
                                                                  how='left')

                # 3. Crear las nuevas columnas
                df_merged['cuenta_matricula'] = df_merged['Matricula']
                df_merged['especialidad_medica'] = df_merged['Especialidad']

                if 'Categoria' in df_merged.columns:
                    df_merged['categoria'] = df_merged['Categoria']
                elif 'Matricula Arancel' in df_merged.columns:
                    df_merged['categoria'] = df_merged['Matricula Arancel']
                elif 'Arancel' in df_merged.columns:
                    df_merged['categoria'] = df_merged['Arancel']
                else:
                    df_merged['categoria'] = df_merged['Matricula']
                    st.warning("Advertencia: No se encontró una columna explícita para 'categoria' (e.g., 'Categoria', 'Matricula Arancel', 'Arancel'). Se ha utilizado la columna 'Matricula' para 'categoria'.")

                def calcular_iva_template(row):
                    if pd.notna(row['Responsabilidad Fiscal']) and (row['Responsabilidad Fiscal'] == 'Monotributo' or row['Responsabilidad Fiscal'] == 'Exento'):
                        return '0'
                    elif pd.notna(row['condición_iva']) and row['condición_iva'] == 'Exento':
                        return '0'
                    else:
                        return '1'

                df_merged['IVA_Template'] = df_merged.apply(calcular_iva_template, axis=1)

                def calcular_tipo_os(row):
                    if pd.notna(row['Responsabilidad Fiscal']) and row['Responsabilidad Fiscal'] == 'Responsable Inscripto':
                        return '11062'
                    else:
                        return '11060'

                df_merged['Tipo_OS'] = df_merged.apply(calcular_tipo_os, axis=1)

                # Select columns
                columns_to_include_in_final_df = st.session_state['df_bloque_2'].columns.tolist() + \
                                                  ['cuenta_matricula', 'especialidad_medica', 'categoria', 'IVA_Template', 'Tipo_OS']

                df_final = df_merged[columns_to_include_in_final_df].copy()
                st.session_state['df_final'] = df_final # Store df_final in session_state here

                # Removed download button for df_final as per user request

        except Exception as e:
            st.error(f"❌ Error al procesar el archivo de valorización o al unir datos: {e}")
    else:
        st.info("Sube el archivo 'Base de datos_valorizacion.xlsx' para continuar.")
else:
    st.info("Primero, procesa el 'Reporte de Liquidacion 1' en el Bloque 1.")


# --- Bloque 3: Valorización ---
st.header("Bloque 3: Valorización")

if 'df_final' in st.session_state and st.session_state['df_final'] is not None and \
   'uploaded_file_valorizacion_object' in st.session_state and st.session_state['uploaded_file_valorizacion_object'] is not None:

    st.markdown("Comenzamos a valorizar los datos.")

    # Re-define the function to take DataFrames directly and return the result DataFrame
    def aplicar_valorizacion_definitiva_streamlit(df_f, uploaded_valorizacion_file_obj):
        st.write("🚀 Iniciando proceso de valorización...")

        # --- INSTANCIA PREVIA: LIMPIEZA DE DUPLICADOS EN ORIGEN ---
        conteo_inicial = len(df_f)
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
        st.write(f"🔹 Registros originales: {conteo_inicial} -> Después de limpiar duplicados: {len(df_f)}")

        # Re-read the base excel file for specific sheets using the uploaded file object
        xls_base = pd.ExcelFile(uploaded_valorizacion_file_obj)
        df_nom = pd.read_excel(xls_base, sheet_name='Nomenclador').copy()
        df_uni = pd.read_excel(xls_base, sheet_name='unidades').copy()
        df_fijos = pd.read_excel(xls_base, sheet_name='Valor Fijos').copy()

        # 2. NORMALIZACIÓN
        def limpiar(x):
            if pd.isna(x): return ""
            return str(x).split('.')[0].strip().upper()

        df_f['prest_limpia'] = df_f['prestación'].apply(limpiar)
        df_f['cat_limpia'] = df_f['categoria'].apply(limpiar)
        df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar)
        df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar)
        df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar)

        # Fechas a periodos
        df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')

        # Handle potential missing 'Mes' or 'Periodo' columns more robustly
        if 'Mes' in df_uni.columns and df_uni['Mes'].notna().any(): # Check if there are any non-NA dates
            df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
        else:
            df_uni['periodo_aux'] = pd.NaT # If no valid dates, all periods will be NaT
            st.warning("La columna 'Mes' no se encontró o no contiene valores de fecha válidos en la hoja 'unidades'. La valorización de la Regla 1 podría verse afectada.")

        if 'Periodo' in df_fijos.columns and df_fijos['Periodo'].notna().any(): # Check if there are any non-NA dates
            df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')
        else:
            df_fijos['periodo_aux'] = pd.NaT # If no valid dates, all periods will be NaT
            st.warning("La columna 'Periodo' no se encontró o no contiene valores de fecha válidos en la hoja 'Valor Fijos'. La valorización de las Reglas 2 y 3 podría verse afectada.")


        # --- REGLA 1: NOMENCLADOR + UNIDADES ---
        df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
        df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')

        # Drop duplicates en la tabla de referencia para evitar duplicar el archivo principal
        df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux']) # Changed to not modify original

        df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']],
                        left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

        # --- REGLA 2: VALOR FIJOS (SWISS MEDICAL) ---
        f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=False, case=False)].copy()

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

        # --- NUEVA COLUMNA: TOTAL ---
        def calcular_total(row):
            try:
                importe_val = row['IMPORTE']
                # Already handled '#REVISAR VALORES' from consolidar
                if isinstance(importe_val, str): # Catch '#REVISAR VALORES' or any other string
                    return importe_val
                return float(importe_val) * float(row['cantidad'])
            except Exception:
                return "#ERROR CALCULO TOTAL"

        df_f['Total'] = df_f.apply(calcular_total, axis=1)

        # --- LIMPIEZA FINAL DE COLUMNAS AUXILIARES Y DUPLICADOS POST-MERGE ---
        df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

        prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1',
                      'Total prestación', 'Tipo de Nomenclador', 'Tipo de nomenclador', 'Código']

        cols_a_borrar = [c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']]
        df_final_resultado = df_f.drop(columns=cols_a_borrar, errors='ignore')

        st.success(f"✅ Finalizado. Registros finales: {len(df_final_resultado)}")
        return df_final_resultado

    # Now, call the modified function
    df_resultado = aplicar_valorizacion_definitiva_streamlit(
        st.session_state['df_final'].copy(), # Pass a copy to avoid modifying original df_final
        st.session_state['uploaded_file_valorizacion_object'] # Pass the uploaded file object
    )

    if df_resultado is not None:
        st.subheader("Reporte Final Valorizado:")
        st.dataframe(df_resultado.head())
        st.write(f"Columnas del DataFrame resultado: {df_resultado.columns.tolist()}")

        excel_buffer_resultado = io.BytesIO()
        df_resultado.to_excel(excel_buffer_resultado, index=False)
        st.download_button(
            label="Descargar Reporte FINAL LIMPIO (Excel)",
            data=excel_buffer_resultado.getvalue(),
            file_name="reporte_FINAL_LIMPIO.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

else:
    st.info("Completa los Bloques anteriores para realizar la valorización.")
