# project/utils/utils.py

import pandas as pd
import os

class Utils:
    """
    Clase con funciones de utilidad para leer y escribir datos en el proyecto.
    """
    @staticmethod
    def load_table(path: str) -> pd.DataFrame:
        """
        Carga una tabla desde una ruta de archivo CSV y la retorna como un DataFrame.
        """
        print(f"🔄 Cargando tabla desde: {path}")
        df = pd.read_csv(path)
        return df

    @staticmethod
    def save_dataframe(df: pd.DataFrame, path: str):
        path = os.path.abspath(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        print(f"💾 DataFrame guardado exitosamente en: {path}")
