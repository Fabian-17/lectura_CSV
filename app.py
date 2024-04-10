import csv 
import MySQLdb as mysql

with open('localidades.csv', newline='') as archivo_csv:
    lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
    for fila in lector_csv:
        print(fila)


# se exporta los datos a una base de datos mysql 

try:
    db = mysql.connect('localhost', 'root', '', 'localidades')
except mysql.Error as e:
    print(f"Error al conectar a MySQL: {e}")
    exit(1)

cursor = db.cursor()
sql = "INSERT INTO localidades (id, nombre, provincia) VALUES (%s, %s, %s)" 
try:
    cursor.executemany(sql, lector_csv)
    db.commit()
except:
    db.rollback()
db.close()