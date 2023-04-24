import os
import csv
import pyodbc
import pandas as pd
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import smtplib
import ftplib
import requests
from ftplib import FTP
import time
from colorama import Fore
import weasyprint
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML, CSS
import datetime
import configparser
now = datetime.datetime.now()
fecha_actual = now.strftime("%d-%m-%Y %H-%M-%S")
fecha_actual2 = now.strftime("%d/%m/%Y %H:%M:%S")
os.environ['TZ'] = 'America/Buenos_Aires'
def consultas_bdd():
    config = configparser.ConfigParser()
    config.read('config.ini')
    # Parámetros de la conexión
    server = '192.168.0.10'
    database = 'master'
    username = config.get('database','username')
    password = config.get('database','password')
    driver = 'SQL Server'
    # Definir y abrir la conexión
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(connection_string)
    #Constructor de estado
    class DatabaseStatus:
        def __init__(self, name, state_desc):
            self.name = name
            self.state_desc = state_desc

        def __iter__(self):
            return iter((self.name, self.state_desc))

        def __repr__(self):
            return f"DatabaseStatus(name='{self.name}', state_desc='{self.state_desc}')"


    # Consulta a ejecutar
    query = "SELECT name, state_desc FROM sys.databases"

    # Guardar datos de la consulta
    results = []
    with conn.cursor() as cursor:
        cursor.execute(query)
        result_set = cursor.fetchall()
    for row in result_set:
        db_status = DatabaseStatus(row[0], row[1])
        results.append(db_status)

    # Crear el dataframe para las bases de datos y una columna con la descripción.
    descripciones = ['Sistema SQL', 'Sistema SQL','Sistema SQL','Sistema SQL','Sistema SQL','Sistema SQL','Vortec Gestion','Gestión de Préstamos','Vortec Gestion','Vortec Gestion','Testing','Sistema Integral de Gestión','Concentrado de apuestas','Backup Bigdata 2023']
    df_db = pd.DataFrame(results, columns=['Nombre','Estado'])
    df_db['Ubicación'] = descripciones # Agregar esta línea para crear la columna Descripción en df_db
    print(df_db.head(14))

    # Leer los datos de los servidores
    with open('servidores.csv', 'r') as f:
        reader = csv.reader(f)
        servers = list(reader)

    # Obtener el estado de conexión de cada servidor
    status = []
    for server in servers:
        hostname = server[1]
        response = os.system("ping -n 1 " + hostname)
        if response == 0:
            status.append('OK')
        else:
            status.append('Error')

    # Crear el dataframe para servidores
    df_servers = pd.DataFrame({'Servidor': [server[0] for server in servers], 'IP': [server[1] for server in servers], 'Estado': status})
    print(df_servers.head(14))
    ftps = ftplib.FTP("pandora.cas.gob.ar")
    ftps.login(config.get('ftps','ftpsUser'), config.get('ftps','ftpsPass'))
    ftps.cwd("backup/bases/Backup SQL")

    # Obtener la lista de archivos en el directorio actual
    file_list = ftps.nlst()

    expected_backups = ['CAS_BigData.bak', 'CAS_Prestamos.bak', 'SIGCAS.bak', 'Vortec Asistencia.bak', 'Vortec Legajos.bak', 'vortecgestion.bak']

    backup_databases = []
    backup_res = ''
    backup_res2 = ''
    backup_res3 = ''

    for filename in file_list:
        if filename in expected_backups and filename.endswith('.bak'):
            # Obtener la fecha de modificación del archivo
            timestamp_str = ftps.sendcmd("MDTM " + filename)[4:].strip()

            print (timestamp_str)
            try:
                date_modified = datetime.datetime.strptime(timestamp_str, "%Y%m%d%H%M%S") - datetime.timedelta(hours=3)
                backup_databases.append({'Nombre del Backup': filename, 'Fecha': date_modified})
            except ValueError:
                print(f"No se pudo convertir la cadena de fecha {timestamp_str} para el archivo {filename}.")
    if backup_databases:
        backup_df = pd.DataFrame(backup_databases)
        expected_backups_df = pd.DataFrame({'Nombre del Backup': expected_backups})
        missing_backups = expected_backups_df[~expected_backups_df['Nombre del Backup'].isin(backup_df['Nombre del Backup'])]

        if not missing_backups.empty:
            backup_res = 'Faltan backups en la carpeta:'
            backup_res += '\n' + '\n'.join([f"- {backup}" for backup in missing_backups['Nombre del Backup']])
            print(backup_res)
        else:
            backup_res2 = 'No faltan backups en la carpeta'
            print("No faltan backups en la carpeta.")
    else:
        backup_res3 = 'No se encontraron backups en la carpeta'
        print("No se encontraron backups en la carpeta.")

    ftps.quit()
    #Crear tabla html
    html_table = backup_df.to_html(index=False)
    # Definir los estilos a utilizar
    css_files = ['estilo.css']
    css_strings = []
    for css_file in css_files:
        with open(css_file, 'r') as f:
            css_strings.append(f.read())
    css_string = '\n'.join(css_strings)
    #Definir clase según el estado
    def format_estado(status):
        if status == "online":
            return f'<span class="online">{status}</span>'
        elif status == "offline":
            return f'<span class="offline">{status}</span>'
        else:
            return status
    format_estado(status)
    # Primera API
    response1 = requests.get(config.get('reactor','url1'))
    json_data1 = response1.json()

    # Segunda API
    response2 = requests.get(config.get('reactor','url2'))
    json_data2 = response2.json()

    # Copiar el diccionario para poder iterar sobre él sin problemas
    json_data_copy = json_data2.copy()
    for k in json_data_copy.keys():
        if 'channel' in k:
            json_data2[k.replace('channel', 'channel2')] = json_data2.pop(k)

    # Fusionar los JSONs en uno solo
    merged_data = {}
    merged_data.update(json_data1)
    merged_data.update(json_data2)
    merged_data.pop('respuesta', None)
    # Crear un dataframe a partir del diccionario merged_data
    df = pd.DataFrame.from_dict(merged_data)

    # Generar la tabla HTML a partir del dataframe
    racks_table = df.to_html(index=False)
     #Generamos tablas html para las bases de datos y los servidores
    tabla_db = df_db.to_html(formatters={"Estado": format_estado}, escape=False, index=False, columns=['Nombre', 'Estado', 'Ubicación'])
    tabla_servers = df_servers.to_html(formatters={"Estado": format_estado}, escape=False,index=False)
    #Construimos el html
    html_string = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Informe {fecha_actual2}</title>
        <style>{css_string}</style>
    </head>
    <body>
    <div class="cabecera">
        <img src="banner.png" alt="hola">
        </div>
        <div class="container">
        <h1>Informe Dpto. Informática {fecha_actual2}</h1>
        <h1></h1>
        <h2>Estado de bases de datos locales</h2>
        {tabla_db}
        <h2>Estado de servidores</h2>
        {tabla_servers}
        <div class="cabecera">
        <img src="banner.png" alt="hola">
        </div>
        <h3>AWS Backups</h3>
        {html_table}
        {backup_res}
        {backup_res2}
        {backup_res3}
        <h4> Reactor Racks Temperatura<h4>
        {racks_table}
        </div>
        <p><strong>Santa Fe 10 (E) - Capital - 5400 - San Juan - cas.gov.ar</strong> </p>
    </body>
    </html>
    '''
    report_html = HTML(string=html_string, base_url='.')
    report_pdf = report_html.write_pdf(stylesheets=[CSS(filename='estilo.css')])
    nombre_archivo = "informe_" + fecha_actual + ".pdf"

    # Guardar el PDF con el nombre del archivo generado
    with open(nombre_archivo, 'wb') as f:
        f.write(report_pdf)
        # Definimos quien envia y recibe
        sender = config.get('email','sender')
        recipient = config.get('email','recipient')

        # Creamos el mensaje
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = 'Informe de estado de servidores y bases de datos'

        # Añadimos el archivo al correo
        with open(nombre_archivo, 'rb') as f:
            attach_file = MIMEApplication(f.read(), _subtype='pdf')
            attach_file.add_header('Content-Disposition', 'attachment', filename=nombre_archivo)
            msg.attach(attach_file)

        # Enviamos el correo
        with smtplib.SMTP('mail.cas.gob.ar', 25) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(sender, config.get('email','emailPass'))
            smtp.sendmail(sender, recipient, msg.as_string())
#Ejecutamos la función
consultas_bdd()