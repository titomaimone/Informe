import pyodbc
import pandas as pd
def conexion():
# Conexión a la base de datos
    server = '192.168.0.10'
    database = 'SIGCAS'
    username = 'sa'
    password = 'Poseidon1'
    driver = 'SQL Server'
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    # Establecer la conexión
    cnxn = pyodbc.connect(connection_string)
    
    # Ejecutar la consulta SQL
    query = """
    Select convert(bigint,(select count(*) from Agencia_SubAg A where A.Anulado = 0 and A.estado ='A') + 
    (select count(*) from Agencia_SubAg A where A.Anulado = 0 and A.estado ='A' and Tipo=1)),'Cantidad'
    UNION
    Select (select top 1 convert(varchar,fechacrea,112)+replace(convert(varchar, fechacrea,108),':','') from CtaCte_Genera_Resumen order by 1 desc), 'Fecha'
    order by 2
    """
    cursor = cnxn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    df.columns = [[0],[1]]
    cursor.close()
    cnxn.close()  # cerrar la conexión
    return df

df = conexion()
print(df.to_html(index=False))