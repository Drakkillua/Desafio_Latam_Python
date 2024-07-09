# mi_modulo.py

import pandas as pd

def filtrar_por_fechas(df, columna_fecha, fecha_inicio, fecha_fin):
    """
    Filtra un DataFrame por un rango de fechas.

    Parameters:
    df (pd.DataFrame): El DataFrame a filtrar.
    columna_fecha (str): El nombre de la columna que contiene las fechas.
    fecha_inicio (str): La fecha de inicio del rango (inclusive) en formato 'YYYY-MM-DD'.
    fecha_fin (str): La fecha de fin del rango (inclusive) en formato 'YYYY-MM-DD'.

    Returns:
    pd.DataFrame: Un DataFrame filtrado por el rango de fechas especificado.
    """
    # Asegurarse de que la columna de fecha sea del tipo datetime
    df[columna_fecha] = pd.to_datetime(df[columna_fecha])

    # Convertir las fechas de inicio y fin a objetos datetime
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)

    # Filtrar el DataFrame por el rango de fechas
    df_filtrado = df[(df[columna_fecha] >= fecha_inicio) & (df[columna_fecha] <= fecha_fin)]

    return df_filtrado

def generar_reporte(df, filas, columnas, valores, funcion_agrupadora, fill_value=0):
    """
    Genera un reporte pivotado a partir de un DataFrame.

    Parameters:
    df (pd.DataFrame): El DataFrame de entrada.
    filas (list): Lista de columnas para usar como índices en el pivote.
    columnas (list): Lista de columnas para usar como columnas en el pivote.
    valores (list): Lista de columnas cuyos valores se desean agregar.
    funcion_agrupadora (str or func): Función de agregación, como 'sum', 'mean', etc.
    fill_value (scalar, default 0): Valor para reemplazar las entradas faltantes en el DataFrame pivotado.

    Returns:
    pd.DataFrame: Un DataFrame pivotado según los parámetros especificados.
    """
    # Crear la tabla dinámica
    tabla_pivotada = pd.pivot_table(
        df,
        index=filas,
        columns=columnas,
        values=valores,
        aggfunc=funcion_agrupadora,
        fill_value=fill_value
    )
    
    return tabla_pivotada

from sqlalchemy import create_engine

def guardar_en_bd(df, nombre_tabla, engine, if_exists='fail'):
    """
    Guarda un DataFrame en una base de datos.

    Parameters:
    df (pd.DataFrame): El DataFrame a guardar.
    nombre_tabla (str): El nombre de la tabla en la base de datos.
    engine: El objeto engine de SQLAlchemy.
    if_exists (str, default 'fail'): Comportamiento si la tabla ya existe.
                                     'fail': Lanza un error.
                                     'replace': Reemplaza la tabla existente.
                                     'append': Agrega los datos a la tabla existente.

    Returns:
    None
    """
    df.to_sql(nombre_tabla, engine, if_exists=if_exists, index=False)