# project/business_logic/cleaning.py

import pandas as pd
import numpy as np

class CleaningTransformer:
    """
    Transformador para la limpieza y preprocesamiento de datos.
    """
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza la limpieza de datos en el DataFrame integrado.

        Args:
            df (pd.DataFrame): El DataFrame para limpiar.

        Returns:
            pd.DataFrame: El DataFrame limpio y preprocesado.
        """
        print("🚀 Iniciando la limpieza de datos...")
        
        # Se recomienda trabajar sobre una copia para evitar advertencias
        df_cleaned = df.copy()
        print("🚀 PARTE #1 OK")
        # 1. Convertir columnas de fecha a formato datetime
        # TODO: Convierte las columnas 'transaction_date' y 'dob' a formato datetime.
        df_cleaned['transaction_date'] = pd.to_datetime(df_cleaned['transaction_date'], errors='coerce')
        print(df_cleaned.sample(5, random_state=42))

        print("🚀 PARTE #2 OK")
        df_cleaned['dob'] = pd.to_datetime(df_cleaned['dob'], errors='coerce')
        print("🚀 PARTE #3 OK")
        # 2. Eliminar valores nulos críticos y montos iguales a 0
        # TODO: Primero, elimina las filas que tengan valores nulos en 'customer_id', 'account_id' o 'amount'.
        # TODO: Luego, filtra el dataframe para quedarte solo con las transacciones cuyo monto ('amount') sea diferente de 0.
        df_cleaned = df_cleaned.dropna(subset=['customer_id', 'account_id', 'amount'])
        df_cleaned = df_cleaned[df_cleaned['amount'] != 0]
        
        # 3. Calcular la edad del cliente
        # Aquí sólo vamos a hacer un cambio ya que por cada ejecución debería de tomar el Today.
        #current_date = pd.to_datetime('2025-07-10') --> DESFASADO, NO DEBERÍA DE HABER UNA FECHA DE ESTA MANERA

        current_date = pd.Timestamp.today()

        # TODO: Calcula la edad del cliente en años (como un número entero) y guárdala en una nueva columna 'age'.
        # Aquí estamos calculando la edad con 365.25 (Bueba práctica)
        df_cleaned['age'] = ((current_date - df_cleaned['dob']).dt.days // 365.25).astype('Int64')
        df_cleaned.drop(columns=['dob'], inplace=True)
        
        # Este paso ya está hecho para ti
        df_cleaned.drop(columns=['dob'], inplace=True)

        # 4. Manejar la columna 'amount'
        # TODO: Crea la columna 'amount_abs' con el valor absoluto de la columna 'amount'.
        df_cleaned['amount_abs'] = df_cleaned['amount'].abs()

        # 5. Mapear columna binaria 'active'
        # TODO: Convierte la columna 'active' de strings ('Yes'/'No') a números (1/0).
        df_cleaned['active'] = df_cleaned['active'].map({'Yes': 1, 'No': 0}).astype('Int64')
        
        # 6. Codificar variables categóricas
        categorical_cols = [
            'gender', 'region', 'risk_profile', # de customers
            'type',                           # de transactions
            'account_type'                    # de accounts
        ]
        # TODO: Aplica one-hot encoding a las columnas listadas en 'categorical_cols'.
        df_cleaned = pd.get_dummies(df_cleaned, columns=categorical_cols, dummy_na=True, prefix=categorical_cols)
        
        # 7. Eliminar columnas que no aportan al modelo
        # TODO: Elimina las columnas de texto o identificadores que ya no necesites.
        # Por ejemplo: 'name', 'description', 'currency'.
        cols_to_drop = ['name', 'description', 'currency']
        df_cleaned.drop(columns=[c for c in cols_to_drop if c in df_cleaned.columns], inplace=True, errors='ignore')       
        
        print("✅ Limpieza de datos completada.")
        return df_cleaned
