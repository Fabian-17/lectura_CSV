import csv 
import MySQLdb as mysql

try:
    db = mysql.connect('localhost', 'root', '', 'tp')
except mysql.Error as e:
    print(f"Error al conectar a MySQL: {e}")
    exit(1)


with open('localidades.csv', newline='') as archivo_csv:
    lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
    for fila in lector_csv:
        print(fila)


try:
    cursor = db.cursor()

    # Crear la tabla 'localidades' si no existe
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS localidades (
            id INT,
            provincia VARCHAR(255) NOT NULL,
            localidad VARCHAR(255) NOT NULL,
            cp VARCHAR(10) NOT NULL,
            id_prov_mstr INT
        )
    """)

except mysql.Error as e:
    print(f"Error al crear la tabla 'localidades' en MySQL: {e}")
    exit(1)


try:
    cursor = db.cursor()
    cursor.execute("SELECT  DISTINCT provincia FROM localidades;")
    provincias =cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0], ))
        localidades = cursor.fetchall()
        with open(f"csv/{provincia[0]}.csv", "w", newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(localidades)
except mysql.Error as e:
    db.rollback()
    print(f"Error al obtener los datos de MySQL: {e}")
    exit(1)
