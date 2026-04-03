# --- Bloque 3: Valorización ---
st.header("Bloque 3: Valorización")

if 'df_final' in st.session_state:
    if st.button("🚀 Ejecutar Valorización Definitiva"):
        try:
            # 1. CARGA DESDE SESSION STATE
            df_f = st.session_state['df_final'].copy()
            
            # Limpieza inicial de duplicados para asegurar integridad
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')

            xls_v = pd.ExcelFile(st.session_state['xls_valorizacion'])
            df_nom = pd.read_excel(xls_v, sheet_name='Nomenclador')
            df_uni = pd.read_excel(xls_v, sheet_name='unidades')
            df_fijos = pd.read_excel(xls_v, sheet_name='Valor Fijos')

            # 2. NORMALIZACIÓN DE CLAVES (Igual que en Colab)
            def limpiar(x): return str(x).split('.')[0].strip().upper() if pd.notna(x) else ""

            df_f['prest_limpia'] = df_f['prestación'].apply(limpiar)
            df_f['cat_limpia'] = df_f['categoria'].apply(limpiar)
            df_nom['cod_limpio'] = df_nom['Código'].apply(limpiar)
            df_fijos['cod_limpio'] = df_fijos['Cod'].apply(limpiar)
            df_fijos['cat_limpia'] = df_fijos['Arancel'].apply(limpiar)

            # Normalización de fechas a periodos Mensuales
            df_f['periodo_aux'] = pd.to_datetime(df_f['fecha_transaccion'], dayfirst=True, errors='coerce').dt.to_period('M')
            df_uni['periodo_aux'] = pd.to_datetime(df_uni['Mes'], errors='coerce').dt.to_period('M')
            df_fijos['periodo_aux'] = pd.to_datetime(df_fijos['Periodo'], errors='coerce').dt.to_period('M')

            # --- REGLA 1: NOMENCLADOR + UNIDADES ---
            df_calc_uni = pd.merge(df_nom, df_uni, left_on=['Tipo de nomenclador'], right_on=['Tipo de Nomenclador'], how='inner')
            df_calc_uni['IMPORTE_R1'] = pd.to_numeric(df_calc_uni['Cirujano'], errors='coerce') * pd.to_numeric(df_calc_uni['Valor'], errors='coerce')
            df_calc_uni = df_calc_uni.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
            
            df_f = pd.merge(df_f, df_calc_uni[['cod_limpio', 'periodo_aux', 'IMPORTE_R1']], 
                            left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], how='left')

            # --- REGLA 2: VALOR FIJOS (FILTRO SWISS MEDICAL) ---
            f_filt = df_fijos[df_fijos['Nomenclador'].astype(str).str.contains('SWISS MEDICAL', na=False, case=False)].copy()
            
            # 2A: Match Código + Categoría + Periodo
            f_2a = f_filt.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
            df_f = pd.merge(df_f, f_2a[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                            how='left')

            # 2B: Match Código + Periodo (Sin Categoría)
            f_2b = f_filt.drop_duplicates(subset=['cod_limpio', 'periodo_aux'])
            df_f = pd.merge(df_f, f_2b[['cod_limpio', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'periodo_aux'], right_on=['cod_limpio', 'periodo_aux'], 
                            how='left', suffixes=('', '_R2B'))

            # --- REGLA 3: VALOR FIJOS (TODOS LOS REGISTROS) ---
            f_3 = df_fijos.drop_duplicates(subset=['cod_limpio', 'cat_limpia', 'periodo_aux'])
            df_f = pd.merge(df_f, f_3[['cod_limpio', 'cat_limpia', 'periodo_aux', 'Total prestación']], 
                            left_on=['prest_limpia', 'cat_limpia', 'periodo_aux'], right_on=['cod_limpio', 'cat_limpia', 'periodo_aux'], 
                            how='left', suffixes=('', '_R3'))

            # --- PASO CRUCIAL: CONSOLIDACIÓN DE IMPORTE (Lógica Colab) ---
            def consolidar_importe(row):
                # 1. Prioridad: Regla 1 (Nomenclador)
                if pd.notna(row.get('IMPORTE_R1')):
                    return row['IMPORTE_R1']
                
                # 2. Prioridad: Regla 2A (Swiss Medical con Categoría)
                if pd.notna(row.get('Total prestación')):
                    return row['Total prestación']
                
                # 3. Prioridad: Regla 2B (Swiss Medical sin Categoría)
                if pd.notna(row.get('Total prestación_R2B')):
                    return row['Total prestación_R2B']
                
                # 4. Prioridad: Regla 3 (Cualquier valor fijo restante)
                if pd.notna(row.get('Total prestación_R3')):
                    return row['Total prestación_R3']
                
                return "#REVISAR VALORES"

            df_f['IMPORTE'] = df_f.apply(consolidar_importe, axis=1)
            
            # Cálculo de Total Final
            def calcular_total(row):
                try:
                    val_imp = row['IMPORTE']
                    if val_imp == "#REVISAR VALORES": return "#REVISAR VALORES"
                    return float(val_imp) * float(row['cantidad'])
                except: return row['IMPORTE']

            df_f['Total'] = df_f.apply(calcular_total, axis=1)

            # Limpieza de columnas auxiliares para el archivo final
            df_f = df_f.drop_duplicates(subset=['transacción_item'], keep='first')
            prohibidas = ['_limpia', 'periodo_aux', 'cod_limpio', 'cat_limpia', 'IMPORTE_R1', 'Total prestación', 'Tipo de Nomenclador', 'Tipo de nomenclador', 'Código']
            cols_a_borrar = [c for c in df_f.columns if any(p in c for p in prohibidas) and c not in ['IMPORTE', 'Total']]
            df_final_res = df_f.drop(columns=cols_a_borrar, errors='ignore')

            st.success(f"✅ Valorización completada exitosamente.")
            st.dataframe(df_final_res.head(50))
            
            # Preparación de descarga
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_final_res.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Descargar Reporte FINAL VALORIZADO",
                data=output.getvalue(),
                file_name="reporte_FINAL_VALORIZADO.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        except Exception as e:
            st.error(f"❌ Error en Valorización: {e}")
