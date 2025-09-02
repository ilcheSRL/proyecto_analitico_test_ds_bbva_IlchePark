# project/business_logic/feature_engineering.py

import pandas as pd

class FeatureEngineeringTransformer:
    """
    Transformador para la creación de nuevas características (features).
    """
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera nuevas variables para el modelo de predicción.

        Args:
            df (pd.DataFrame): El DataFrame limpio.

        Returns:
            pd.DataFrame: El DataFrame con las nuevas características.
        """
        print("🚀 Iniciando la ingeniería de características...")
  

        # TODO: Ordena el DataFrame por 'customer_id' y 'transaction_date'.
        # Esto es crucial para que los cálculos de ventana (móviles) funcionen correctamente.
        df = df.copy()
        df = df.sort_values(by=['customer_id', 'transaction_date'], kind='mergesort')
        print("🚀 PARTE #1 OK")
        # 1. Total de transacciones por cliente.
        # TODO: Crea la columna 'transactions_count'. Debe contener el número total de transacciones
        # realizadas por cada cliente.
        df['transactions_count'] = df.groupby('customer_id')['transaction_id'].transform('count')
        print("🚀 PARTE #2 OK")
        # 2. Promedio móvil del monto (absoluto) de las últimas 3 transacciones por cliente.
        # TODO: Crea la columna 'amount_abs_moving_avg_3t'. Debe ser el promedio móvil
        # de las últimas 3 transacciones de 'amount_abs' para cada cliente.
        df['amount_abs_moving_avg_3t'] = (
            df.groupby('customer_id')['amount_abs']
              .rolling(window=3, min_periods=1)
              .mean()
              .reset_index(level=0, drop=True)
        )

        print("🚀 PARTE #3 OK")
        # 3. Suma total de dinero movido (absoluto) por el cliente.
        # TODO: Crea la columna 'total_amount_moved'. Debe contener la suma de todos los
        # montos en 'amount_abs' para cada cliente.
        df['total_amount_moved'] = df.groupby('customer_id')['amount_abs'].transform('sum')
        print("🚀 PARTE #4 OK")
        # 4. Día de la semana y mes de la transacción.
        # TODO: Extrae el día de la semana y el mes de 'transaction_date'.
        df['day_of_week'] = df['transaction_date'].dt.dayofweek   # Lunes=0, Domingo=6
        df['month_of_transaction'] = df['transaction_date'].dt.month

        #5. Agregado adicional

        print("🚀 AGREGADO ADICIONAL: (Experimental para el entrenamiento)")
        df['mean_amount'] = df.groupby('customer_id')['amount_abs'].transform('mean')
        df['max_amount'] = df.groupby('customer_id')['amount_abs'].transform('max')
        df['min_amount'] = df.groupby('customer_id')['amount_abs'].transform('min')
        df['std_amount'] = df.groupby('customer_id')['amount_abs'].transform('std')
        df['days_since_last_tx'] = df.groupby('customer_id')['transaction_date'].diff().dt.days
        df['avg_days_between_tx'] = df.groupby('customer_id')['days_since_last_tx'].transform('mean')
        df['transactions_last_30d'] = (
            df.groupby('customer_id')['transaction_date']
              .transform(lambda x: (x.max() - x).dt.days.le(30).sum())
        )

        cols_to_impute = [
        'mean_amount',
        'max_amount',
        'min_amount',
        'std_amount',
        'days_since_last_tx',
        'avg_days_between_tx',
        'transactions_last_30d'
        ]

        df[cols_to_impute] = df[cols_to_impute].fillna(0)

        print("Reporte de Nulos, aseguramiento de calidad")

        # Calcular % de nulos por columna
        null_report = (
            df.isnull().mean().reset_index()
            .rename(columns={'index': 'column', 0: 'null_percentage'})
        )
        
        # Convertir a %
        null_report['null_percentage'] = null_report['null_percentage'] * 100
 
        
        print("📊 Porcentaje de valores nulos por columna:")
        print(null_report)


       #USABLE PARA MACHINE LEARNING
       # CONVIRTIENDO A NUMÉRICO PARA QUE SEA USÁBLE
       # ========= construir df_numérico =========
        df_numeric = df.copy()

        # 1) Convertir booleanos a enteros
        bool_cols = df_numeric.select_dtypes(include=['bool']).columns
        if len(bool_cols) > 0:
            df_numeric[bool_cols] = df_numeric[bool_cols].astype('int8')

        # 2) Convertir datetimes a epoch (segundos) y eliminar la original
        if 'transaction_date' in df_numeric.columns and pd.api.types.is_datetime64_any_dtype(df_numeric['transaction_date']):
            df_numeric['transaction_ts'] = (df_numeric['transaction_date'].view('int64') // 10**9).astype('Int64')
            df_numeric.drop(columns=['transaction_date'], inplace=True)

        # 3) Quitar IDs de texto que no son numéricos (opcional pero recomendado para modelos)
        drop_ids = ['transaction_id','customer_id','account_id','name','description','currency']
        df_numeric.drop(columns=[c for c in drop_ids if c in df_numeric.columns], inplace=True, errors='ignore')

        # 4) Forzar lo restante a numérico (lo no convertible queda NaN)
        for col in df_numeric.columns:
            if not pd.api.types.is_numeric_dtype(df_numeric[col]):
                df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')

 



        
        print("✅ Ingeniería de características completada.")
        return df, df_numeric
