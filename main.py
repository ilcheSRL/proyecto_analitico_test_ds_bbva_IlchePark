# project/main.py

import pandas as pd
from constants import (
    CUSTOMERS_PATH, 
    ACCOUNTS_PATH, 
    TRANSACTIONS_PATH,
    #OUTPUT_PATH
    FEATURES_PATH
    FEATURES_NUMERIC_PATH
)
from utils.utils import Utils
from business_logic.integration import IntegrationTransformer
from business_logic.cleaning import CleaningTransformer
from business_logic.feature_engineering import FeatureEngineeringTransformer

def main():
    """
    Función principal que ejecuta el pipeline de procesamiento de datos.
    """
    print("===== INICIO DEL PIPELINE DE PROCESAMIENTO DE DATOS BANCARIOS =====")

    # Paso 1: Lectura de Datos
    # TODO: Carga las tablas de 'accounts' y 'transactions' usando la función Utils.load_table()
    # y las constantes correspondientes.
    customers = Utils.load_table(CUSTOMERS_PATH)
    accounts = Utils.load_table(ACCOUNTS_PATH)
    transactions = Utils.load_table(TRANSACTIONS_PATH)

    # Instanciar los transformadores
    integrator = IntegrationTransformer()
    cleaner = CleaningTransformer()
    feature_creator = FeatureEngineeringTransformer()

    # Paso 2: Unión de Tablas
    integrated_data = integrator.integrate_data(transactions, customers, accounts)
    
    # Paso 3: Limpieza de Datos
    cleaned_data = cleaner.clean_data(integrated_data)

    # Paso 4: Ingeniería de Features
    #final_feature_table = feature_creator.create_features(cleaned_data)
    final_feature_table, final_feature_table_numeric = feature_creator.create_features(cleaned_data)

    # Paso 5: Guardar el resultado
    # TODO: Llama a la función Utils.save_dataframe() para guardar 'final_feature_table'.
    Utils.save_dataframe(final_feature_table, FEATURES_PATH)
    Utils.save_dataframe(final_feature_table_numeric, FEATURES_NUMERIC_PATH)
    # Pasa la ruta de salida (OUTPUT_PATH) y un nombre de archivo, por ejemplo 'feature_table.csv'.
    

    print("\n===== PIPELINE COMPLETADO CON ÉXITO =====")
    print("\n📊 Muestra del Tablón de Features Final:")
    print(final_feature_table.head())
    print(final_feature_table_numeric.head())

    print("\n📋 Información del Tablón Final:")
    final_feature_table.info()
    final_feature_table_numeric.info()  

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    main()
