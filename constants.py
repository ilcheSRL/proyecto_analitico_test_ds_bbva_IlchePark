# project/constants.py

import os

# Directorio base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Rutas a los archivos CSV de entrada
RESOURCES_DIR = os.path.join(BASE_DIR, 'resources')
CUSTOMERS_PATH = os.path.join(RESOURCES_DIR, 'customers_large.csv')
ACCOUNTS_PATH = os.path.join(RESOURCES_DIR, 'accounts_large.csv')
TRANSACTIONS_PATH = os.path.join(RESOURCES_DIR, 'transactions_large.csv')

# Ruta para los datos procesados de salida
#OUTPUT_PATH = os.path.join(RESOURCES_DIR, 'feature_table.csv')
# constants.py
FEATURES_PATH = os.path.join(RESOURCES_DIR, 'feature_table.csv')
FEATURES_NUMERIC_PATH = os.path.join(RESOURCES_DIR, 'feature_table_numeric.csv')
