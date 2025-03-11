import openpyxl
import pandas as pd
import sqlite3
import re
import os
import streamlit as st
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, filename='procesamiento.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Mapeos predeterminados basados en el nombre de la pestaña
default_mappings = {
    "SIMPATIC": {
        'ICCID': 'iccid',
        'TELEFONO': 'msisdn',
        'ESTADO DEL SIM': 'status',
        'EN SESION': 'status',
        'ConsumoMb': 'consumo en Mb'  
    },
    "TELCEL ALEJANDRO": {
        'ICCID': 'ICCID',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'ESTADO SIM',
        'EN SESION': 'SESIÓN',
        'ConsumoMb': 'LÍMITE DE USO DE DATOS' 
    },
    "-1": {
        'ICCID': 'ICCID',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'Estado de SIM',
        'EN SESION': 'En sesión',
        'ConsumoMb': 'Uso de ciclo hasta la fecha (MB)'  
    },
    "-2": {
        'ICCID': 'ICCID',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'Estado de SIM',
        'EN SESION': 'En sesión',
        'ConsumoMb': 'Uso de ciclo hasta la fecha (MB)'  
    },
    "TELCEL": {
        'ICCID': 'ICCID',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'ESTADO SIM',
        'EN SESION': 'SESIÓN',
        'ConsumoMb': 'LÍMITE DE USO DE DATOS' 
    },
    "MOVISTAR": {
        'ICCID': 'ICC',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'Estado',
        'EN SESION': 'Estado GPRS',
        'ConsumoMb': 'Consumo Datos Mensual' 
    },
    "NANTI": {
        'ICCID': 'ICCID',
        'TELEFONO': 'MSISDN',
        'ESTADO DEL SIM': 'STATUS',
        'EN SESION': 'STATUS',
        'ConsumoMb': 'Plan Original'  
    },
    "LEGACY": {
        'ICCID': 'ICCID',
        'TELEFONO': 'TELEFONO',
        'ESTADO DEL SIM': 'Estatus',
        'EN SESION': 'Estatus',
        'ConsumoMb': 'BSP Nacional'  
    }
}


def create_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS sims ( 
            ICCID TEXT, 
            TELEFONO TEXT, 
            ESTADO_DEL_SIM TEXT, 
            EN_SESION TEXT, 
            ConsumoMb TEXT,  -- Añadido
            Compania TEXT,
            UNIQUE(ICCID, TELEFONO)
        ) 
    ''')
    conn.commit()
    conn.close()

# Función para insertar datos en la base de datos con manejo de duplicados
def insert_data(db_path, data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    records_before = cursor.execute("SELECT COUNT(*) FROM sims").fetchone()[0]
    try:
        cursor.executemany(
            "INSERT OR IGNORE INTO sims (ICCID, TELEFONO, ESTADO_DEL_SIM, EN_SESION, ConsumoMb, Compania) VALUES (?, ?, ?, ?, ?, ?)",
            data
        )
        conn.commit()
        records_after = cursor.execute("SELECT COUNT(*) FROM sims").fetchone()[0]
        records_inserted = records_after - records_before
        logging.info(f"Insertados {records_inserted} registros en la base de datos.")
        return len(data), records_inserted
    finally:
        conn.close()

# Función para limpiar ICCID, TELEFONO y ConsumoMb manteniendo ceros a la izquierda
def clean_iccid_telefono_consumo(data):
    cleaned_data = []
    for row in data:
        cleaned_row = list(row)
        original_iccid = cleaned_row[0]
        original_telefono = cleaned_row[1]
        original_consumo_mb = cleaned_row[4]  # Índice para ConsumoMb
        
        # Limpieza de ICCID
        iccid_value = row[0]
        if isinstance(iccid_value, float) and iccid_value.is_integer():
            cleaned_iccid = str(int(iccid_value))
        else:
            cleaned_iccid = str(iccid_value)
        cleaned_row[0] = ''.join(filter(str.isdigit, cleaned_iccid)) if cleaned_iccid else ""
        
        # Limpieza de TELEFONO
        telefono_value = row[1]
        if isinstance(telefono_value, float) and telefono_value.is_integer():
            cleaned_telefono = str(int(telefono_value))
        else:
            cleaned_telefono = str(telefono_value)
        cleaned_row[1] = ''.join(filter(str.isdigit, cleaned_telefono)) if cleaned_telefono else ""
        
        # Limpieza de ConsumoMb (si es necesario)
        consumo_mb_value = row[4]
        cleaned_consumo_mb = ''.join(filter(str.isdigit, consumo_mb_value)) if consumo_mb_value else ""
        cleaned_row[4] = cleaned_consumo_mb
        
        # Normalizar otros campos
        cleaned_row[2] = cleaned_row[2].strip().lower() if cleaned_row[2] else ""
        cleaned_row[3] = cleaned_row[3].strip().lower() if cleaned_row[3] else ""
        
        cleaned_data.append(tuple(cleaned_row))
        logging.info(f"Limpieza Registro: ICCID '{original_iccid}' a '{cleaned_row[0]}', TELEFONO '{original_telefono}' a '{cleaned_row[1]}', ConsumoMb '{original_consumo_mb}' a '{cleaned_row[4]}'")
    return cleaned_data

# Función para normalizar otros campos
def normalize_data(data):
    normalized_data = []
    for row in data:
        cleaned_row = list(row)
        # Normalizar otros campos a minúsculas y eliminar espacios
        cleaned_row[2] = cleaned_row[2].strip().lower() if cleaned_row[2] else ""
        cleaned_row[3] = cleaned_row[3].strip().lower() if cleaned_row[3] else ""
        # Puedes añadir normalización para ConsumoMb si es necesario
        normalized_data.append(tuple(cleaned_row))
    return normalized_data

# Función para procesar archivos Excel
def process_excel(excel_path, column_mapping, sheet_name):
    workbook = openpyxl.load_workbook(excel_path, data_only=True)
    sheet = workbook[sheet_name]
    all_data = []
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    for row in sheet.iter_rows(min_row=2, values_only=True):
        try:
            row_data = []
            for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:  # Añadido
                col_index = column_mapping[key]
                if col_index is None or col_index == -1:
                    cell_value = ""
                elif col_index >= len(row):
                    cell_value = ""
                else:
                    cell = row[col_index]
                    if isinstance(cell, float) and cell.is_integer():
                        cell_value = str(int(cell))
                    elif isinstance(cell, (int, str)):
                        cell_value = str(cell)
                    else:
                        cell_value = str(cell) if cell is not None else ""
                row_data.append(cell_value)
            row_data.append(sheet_name)  # Añadir el nombre de la pestaña como 'Compania'
            all_data.append(row_data)
        except IndexError:
            st.warning(f"Error procesando fila en la pestaña '{sheet_name}' del archivo '{os.path.basename(excel_path)}'. Fila omitida.")
    return all_data

# Función para procesar archivos CSV
def process_csv(csv_path, column_mapping):
    try:
        df = pd.read_csv(csv_path, dtype=str)  # Leer todas las columnas como cadenas
    except Exception as e:
        logging.error(f"Error leyendo CSV '{csv_path}': {e}")
        return []
    all_data = []
    company_name = os.path.splitext(os.path.basename(csv_path))[0]  # Obtener el nombre del archivo sin extensión
    for index, row in df.iterrows():
        try:
            row_data = []
            for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:  # Añadido
                cell = row.get(column_mapping[key], "")
                if pd.notnull(cell):
                    cell = cell.strip()
                    # Manejar floats representando enteros
                    if re.match(r'^\d+\.\0+$', cell):
                        cell_value = str(int(float(cell)))
                    else:
                        cell_value = re.sub(r'[^\d]', '', cell)  # Eliminar todo excepto dígitos
                else:
                    cell_value = ""
                row_data.append(cell_value)
            row_data.append(company_name)  # Añadir el nombre del archivo como 'Compania'
            all_data.append(row_data)
        except KeyError:
            st.warning(f"Error procesando fila {index + 1} en el archivo CSV '{os.path.basename(csv_path)}'. Fila omitida.")
    return all_data

# Función auxiliar para permitir la selección manual de columnas
def get_column_selection(columns, label, key):
    selection = st.selectbox(
        label,
        options=columns,
        index=0,  # Por defecto, seleccionar la primera columna
        key=key
    )
    return selection

# Interfaz de usuario con Streamlit
st.title("Carga de Excel y CSV y Homologación de Base de Datos")

# Ruta de la carpeta con archivos Excel y CSV con valor por defecto
default_folder_path = r"C:\Users\capac\OneDrive\Escritorio\Actividades de Sims\bd_sims"
folder_path = st.text_input(
    "Ingresa la ruta de la carpeta con archivos Excel y CSV:",
    value=default_folder_path
)

# Ruta para almacenar la base de datos con valor por defecto
default_db_path = os.path.join(default_folder_path, 'sims_hoy.db')
db_path = st.text_input(
    "Ingresa la ruta para almacenar la base de datos (ej. 'sims_database.db'):",
    value=default_db_path
)

if folder_path:
    if os.path.isdir(folder_path):
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') or f.endswith('.csv')]
        st.write(f"Archivos encontrados: {files}")  # Mostrar archivos encontrados

        if files:
            selected_files = st.multiselect("Selecciona los archivos Excel o CSV:", files)

            if selected_files:
                # Selección de columnas para cada archivo
                column_mapping = {}
                for file in selected_files:
                    sheet_data = {}
                    file_path = os.path.join(folder_path, file)
                    if file.endswith('.xlsx'):
                        workbook = openpyxl.load_workbook(file_path, data_only=True)
                        for sheet_name in workbook.sheetnames:
                            st.header(f"Archivo: {file} | Pestaña: {sheet_name}")
                            if sheet_name in default_mappings:
                                # Usar mapeo predeterminado
                                mapping = default_mappings[sheet_name]
                                header_row = next(workbook[sheet_name].iter_rows(min_row=1, max_row=1, values_only=True))
                                
                                # Inicializar mapeo basado en nombres de columnas
                                mapping_indices = {}
                                mapping_valid = True
                                for key_field, column_name in mapping.items():
                                    if column_name in header_row:
                                        mapping_indices[key_field] = header_row.index(column_name)
                                    else:
                                        st.warning(f"La columna '{column_name}' no se encontró en la pestaña '{sheet_name}' del archivo '{file}'. Se requiere selección manual.")
                                        mapping_valid = False
                                        break  # Salir del mapeo automático si falta alguna columna
                                
                                if mapping_valid:
                                    # Manejar ConsumoMb
                                    if 'ConsumoMb' in mapping:
                                        consumo_mb_col = mapping['ConsumoMb']
                                        if consumo_mb_col in header_row:
                                            mapping_indices['ConsumoMb'] = header_row.index(consumo_mb_col)
                                        else:
                                            mapping_indices['ConsumoMb'] = -1  # Indica que no está mapeado
                                    sheet_data[sheet_name] = mapping_indices
                                    st.info(f"Usando mapeo predeterminado para la pestaña '{sheet_name}' del archivo '{file}'.")
                                    logging.info(f"Archivo: {file} | Pestaña: {sheet_name} | Mapeo: {mapping_indices}")
                                else:
                                    # Permitir selección manual de columnas
                                    columns = [cell if cell is not None else "" for cell in header_row]
                                    st.write("Selecciona las columnas correspondientes para cada campo requerido:")

                                    iccid_col = get_column_selection(
                                        columns,
                                        label="Selecciona columna para ICCID:",
                                        key=f"{file}_{sheet_name}_iccid"
                                    )
                                    telefono_col = get_column_selection(
                                        columns,
                                        label="Selecciona columna para TELEFONO:",
                                        key=f"{file}_{sheet_name}_telefono"
                                    )
                                    estado_sim_col = get_column_selection(
                                        columns,
                                        label="Selecciona columna para ESTADO DEL SIM:",
                                        key=f"{file}_{sheet_name}_estado_sim"
                                    )
                                    en_sesion_col = get_column_selection(
                                        columns,
                                        label="Selecciona columna para EN SESION:",
                                        key=f"{file}_{sheet_name}_en_sesion"
                                    )
                                    consumo_mb_col = get_column_selection(
                                        columns,
                                        label="Selecciona columna para ConsumoMb:",
                                        key=f"{file}_{sheet_name}_consumo_mb"
                                    )

                                    # Mapear las selecciones de columnas a sus índices (0-based)
                                    sheet_data[sheet_name] = {
                                        'ICCID': columns.index(iccid_col),
                                        'TELEFONO': columns.index(telefono_col),
                                        'ESTADO DEL SIM': columns.index(estado_sim_col),
                                        'EN SESION': columns.index(en_sesion_col),
                                        'ConsumoMb': columns.index(consumo_mb_col) if consumo_mb_col in columns else -1  # Añadido
                                    }
                                    logging.info(f"Archivo: {file} | Pestaña: {sheet_name} | Mapeo Manual: {sheet_data[sheet_name]}")
                            else:
                                # Permitir selección manual de columnas para pestañas no predefinidas
                                # Corrección: Asegurarse de que se obtiene la primera fila para encabezados
                                columns = [cell if cell is not None else "" for cell in next(workbook[sheet_name].iter_rows(min_row=1, max_row=1, values_only=True))]
                                st.write("Selecciona las columnas correspondientes para cada campo requerido:")

                                iccid_col = get_column_selection(
                                    columns,
                                    label="Selecciona columna para ICCID:",
                                    key=f"{file}_{sheet_name}_iccid"
                                )
                                telefono_col = get_column_selection(
                                    columns,
                                    label="Selecciona columna para TELEFONO:",
                                    key=f"{file}_{sheet_name}_telefono"
                                )
                                estado_sim_col = get_column_selection(
                                    columns,
                                    label="Selecciona columna para ESTADO DEL SIM:",
                                    key=f"{file}_{sheet_name}_estado_sim"
                                )
                                en_sesion_col = get_column_selection(
                                    columns,
                                    label="Selecciona columna para EN SESION:",
                                    key=f"{file}_{sheet_name}_en_sesion"
                                )
                                consumo_mb_col = get_column_selection(
                                    columns,
                                    label="Selecciona columna para ConsumoMb:",
                                    key=f"{file}_{sheet_name}_consumo_mb"
                                )

                                # Mapear las selecciones de columnas a sus índices (0-based)
                                sheet_data[sheet_name] = {
                                    'ICCID': columns.index(iccid_col),
                                    'TELEFONO': columns.index(telefono_col),
                                    'ESTADO DEL SIM': columns.index(estado_sim_col),
                                    'EN SESION': columns.index(en_sesion_col),
                                    'ConsumoMb': columns.index(consumo_mb_col) if consumo_mb_col in columns else -1  # Añadido
                                }
                                logging.info(f"Archivo: {file} | Pestaña: {sheet_name} | Mapeo Manual: {sheet_data[sheet_name]}")
                    elif file.endswith('.csv'):
                        df = pd.read_csv(file_path, dtype=str)  # Leer todas las columnas como cadenas
                        columns = df.columns.tolist()  # Obtener nombres de columnas
                        st.header(f"Archivo: {file}")
                        st.write("Selecciona las columnas correspondientes para cada campo requerido:")

                        # Permitir selección de columnas manualmente
                        iccid_col = get_column_selection(
                            columns,
                            label="Selecciona columna para ICCID:",
                            key=f"{file}_iccid"
                        )
                        telefono_col = get_column_selection(
                            columns,
                            label="Selecciona columna para TELEFONO:",
                            key=f"{file}_telefono"
                        )
                        estado_sim_col = get_column_selection(
                            columns,
                            label="Selecciona columna para ESTADO DEL SIM:",
                            key=f"{file}_estado_sim"
                        )
                        en_sesion_col = get_column_selection(
                            columns,
                            label="Selecciona columna para EN SESION:",
                            key=f"{file}_en_sesion"
                        )
                        consumo_mb_col = get_column_selection(
                            columns,
                            label="Selecciona columna para ConsumoMb:",
                            key=f"{file}_consumo_mb"
                        )

                        # Mapear las selecciones de columnas a sus índices (0-based)
                        sheet_data[file] = {
                            'ICCID': df.columns.get_loc(iccid_col),
                            'TELEFONO': df.columns.get_loc(telefono_col),
                            'ESTADO DEL SIM': df.columns.get_loc(estado_sim_col),
                            'EN SESION': df.columns.get_loc(en_sesion_col),
                            'ConsumoMb': df.columns.get_loc(consumo_mb_col) if consumo_mb_col in df.columns else -1  # Añadido
                        }
                        logging.info(f"Archivo: {file} | CSV | Mapeo Manual: {sheet_data[file]}")
                    
                    column_mapping[file] = sheet_data

                # Añadir una vista previa antes de procesar
                st.subheader("Vista Previa de Mapeo de Columnas")
                for file in selected_files:
                    if file.endswith('.xlsx'):
                        workbook = openpyxl.load_workbook(os.path.join(folder_path, file), data_only=True)
                        for sheet, mapping in column_mapping[file].items():
                            st.write(f"**Archivo:** {file} | **Pestaña:** {sheet}")
                            header_row = next(workbook[sheet].iter_rows(min_row=1, max_row=1, values_only=True))
                            st.write(f" - ICCID: {header_row[mapping['ICCID']]}")
                            st.write(f" - TELEFONO: {header_row[mapping['TELEFONO']]}")
                            st.write(f" - ESTADO DEL SIM: {header_row[mapping['ESTADO DEL SIM']]}")
                            st.write(f" - EN SESION: {header_row[mapping['EN SESION']]}")
                            if mapping.get('ConsumoMb', -1) != -1:
                                st.write(f" - ConsumoMb: {header_row[mapping['ConsumoMb']]}")
                            else:
                                st.write(" - ConsumoMb: No mapeado")
                    elif file.endswith('.csv'):
                        df = pd.read_csv(os.path.join(folder_path, file), dtype=str)
                        mapping = column_mapping[file]
                        st.write(f"**Archivo CSV:** {file}")
                        st.write(f" - ICCID: {df.columns[mapping['ICCID']]}")
                        st.write(f" - TELEFONO: {df.columns[mapping['TELEFONO']]}")
                        st.write(f" - ESTADO DEL SIM: {df.columns[mapping['ESTADO DEL SIM']]}")
                        st.write(f" - EN SESION: {df.columns[mapping['EN SESION']]}")
                        if mapping.get('ConsumoMb', -1) != -1:
                            st.write(f" - ConsumoMb: {df.columns[mapping['ConsumoMb']]}")
                        else:
                            st.write(" - ConsumoMb: No mapeado")
                
                # Botón para procesar todos los archivos seleccionados
                if st.button("Procesar Todos los Archivos"):
                    # Validación: Asegurarse de que todas las columnas hayan sido mapeadas correctamente
                    all_mappings_valid = True
                    for file in selected_files:
                        if file.endswith('.xlsx'):
                            workbook = openpyxl.load_workbook(os.path.join(folder_path, file), data_only=True)
                            for sheet_name in workbook.sheetnames:
                                num_columns = workbook[sheet_name].max_column
                                for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:  # Añadido
                                    if key == 'ConsumoMb' and column_mapping[file][sheet_name][key] == -1:
                                        st.warning(f"La columna 'ConsumoMb' no está mapeada para la pestaña '{sheet_name}' del archivo '{file}'. Se establecerá como NULL.")
                                    elif key != 'ConsumoMb' and not (0 <= column_mapping[file][sheet_name][key] < num_columns):
                                        st.error(f"Mapeo inválido para '{key}' en la pestaña '{sheet_name}' del archivo '{file}'.")
                                        all_mappings_valid = False
                        elif file.endswith('.csv'):
                            df = pd.read_csv(os.path.join(folder_path, file), dtype=str)
                            mapping = column_mapping[file]
                            num_columns = len(df.columns)
                            for key in ['ICCID', 'TELEFONO', 'ESTADO DEL SIM', 'EN SESION', 'ConsumoMb']:  # Añadido
                                if key == 'ConsumoMb' and mapping[key] == -1:
                                    st.warning(f"La columna 'ConsumoMb' no está mapeada para el archivo CSV '{file}'. Se establecerá como NULL.")
                                elif key != 'ConsumoMb' and not (0 <= mapping[key] < num_columns):
                                    st.error(f"Mapeo inválido para '{key}' en el archivo CSV '{file}'.")
                                    all_mappings_valid = False

                    if not all_mappings_valid:
                        st.error("Por favor, revisa los mapeos de columnas y corrige los errores antes de proceder.")
                    else:
                        create_database(db_path)
                        logging.info(f"Base de datos creada o existente: {db_path}")

                        all_data = []
                        total_records = 0
                        total_inserted = 0
                        stats_by_file = {}

                        for file in selected_files:
                            file_path = os.path.join(folder_path, file)
                            if file.endswith('.xlsx'):
                                workbook = openpyxl.load_workbook(file_path, data_only=True)
                                stats_by_file[file] = {'sheets': {}}
                                
                                for sheet_name in workbook.sheetnames:
                                    data = process_excel(file_path, column_mapping[file][sheet_name], sheet_name)
                                    if data:
                                        processed, inserted = insert_data(db_path, data)
                                        stats_by_file[file]['sheets'][sheet_name] = {
                                            'processed': processed,
                                            'inserted': inserted
                                        }
                                        total_records += processed
                                        total_inserted += inserted
                                        
                            elif file.endswith('.csv'):
                                data = process_csv(file_path, column_mapping[file])
                                if data:
                                    processed, inserted = insert_data(db_path, data)
                                    stats_by_file[file] = {
                                        'processed': processed,
                                        'inserted': inserted
                                    }
                                    total_records += processed
                                    total_inserted += inserted

                        # Crear pestañas para la información
                        tab1, tab2 = st.tabs(["Proceso", "Estadísticas"])
                        
                        with tab1:
                            st.success("¡Procesamiento completado!")
                            st.write(f"Total de registros procesados: {total_records}")
                            st.write(f"Total de registros insertados: {total_inserted}")
                            
                        with tab2:
                            st.header("Estadísticas de Procesamiento")
                            st.write(f"Tasa de inserción total: {(total_inserted/total_records*100):.2f}%")
                            
                            for file, stats in stats_by_file.items():
                                st.subheader(f"Archivo: {file}")
                                if 'sheets' in stats:  # Excel file
                                    for sheet, sheet_stats in stats['sheets'].items():
                                        processed = sheet_stats['processed']
                                        inserted = sheet_stats['inserted']
                                        insertion_rate = (inserted/processed*100) if processed > 0 else 0
                                        st.write(f"Pestaña: {sheet}")
                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            st.metric("Registros Procesados", processed)
                                        with col2:
                                            st.metric("Registros Insertados", inserted)
                                        with col3:
                                            st.metric("Tasa de Inserción", f"{insertion_rate:.2f}%")
                                else:  # CSV file
                                    processed = stats['processed']
                                    inserted = stats['inserted']
                                    insertion_rate = (inserted/processed*100) if processed > 0 else 0
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Registros Procesados", processed)
                                    with col2:
                                        st.metric("Registros Insertados", inserted)
                                    with col3:
                                        st.metric("Tasa de Inserción", f"{insertion_rate:.2f}%")
    else:
        st.warning("No se encontraron archivos Excel o CSV en la carpeta seleccionada.")
else:
    st.error("La ruta ingresada no es válida. Por favor, verifica la ruta de la carpeta.")
