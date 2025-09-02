# project/business_logic/integration.py

import pandas as pd

class IntegrationTransformer:
    """
    Transformador para la integración de datos de diferentes fuentes.
    """
    def integrate_data(self, transactions_df: pd.DataFrame, customers_df: pd.DataFrame, accounts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Une los DataFrames de transacciones, clientes y cuentas según el nuevo esquema.

        Args:
            transactions_df (pd.DataFrame): DataFrame de transacciones.
            customers_df (pd.DataFrame): DataFrame de clientes.
            accounts_df (pd.DataFrame): DataFrame de cuentas.

        Returns:
            pd.DataFrame: Un único DataFrame con toda la información integrada.
        """
        print("🚀 Iniciando la integración de tablas...")
        
        # 1. Unir transacciones con clientes.
        # TODO: Realiza la unión entre 'transactions_df' y 'customers_df'. 
        # Deberás inferir la columna común a partir del diccionario de datos.
        merged_df = pd.merge(transactions_df, customers_df, on="customer_id", how="left")

 
        
        # 2. Unir el resultado con cuentas.
        # TODO: Ahora, une 'merged_df' con 'accounts_df'.
        # Infiere la columna de unión y el tipo de join más adecuado para no perder transacciones.
        full_df = pd.merge(merged_df, accounts_df, on="account_id", how="left")
 
        
        print("✅ Integración completada.")
        return full_df
