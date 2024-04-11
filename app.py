import csv 
import MySQLdb as mysql

try:
    db = mysql.connect('localhost', 'root', '', 'localidades')
except mysql.Error as e:
    print(f"Error al conectar a MySQL: {e}")
    exit(1)


try:
    cursor = db.cursor()

    # Verificar si la tabla 'localidades' ya existe
    cursor.execute("SHOW TABLES LIKE 'localidades'")
    table_exists = cursor.fetchone()

    # Si la tabla existe, eliminarla
    if table_exists:
        cursor.execute("DROP TABLE localidades")
        print("Tabla 'localidades' eliminada.")

    # Crear la tabla 'localidades'
    cursor.execute("""
        CREATE TABLE localidades (
                provincia VARCHAR(255), 
                `id` INT(11),
                localidad VARCHAR(255),
                cp INT(11), 
                id_prov_mstr INT(11)
        )
    """)
    print("Tabla 'localidades' creada.")

# Abrir y leer el archivo CSV
    with open('localidades.csv', newline='') as archivo_csv:
        lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
        next(lector_csv)  # Saltar la primera fila si contiene encabezados
        for fila in lector_csv:
            provincia, id, localidad, cp, id_prov_mstr = fila
            cursor.execute("INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)", (provincia, id, localidad, cp, id_prov_mstr))
    print("Datos insertados correctamente.")

# Confirmar la transacción
    db.commit()

except mysql.Error as e:
    db.rollback()
    print(f"Error al crear o eliminar la tabla 'localidades' en MySQL: {e}")
    exit(1)


try:
    cursor = db.cursor()
    cursor.execute("SELECT  DISTINCT provincia FROM localidades;")
    provincias =cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0], ))
        localidades = cursor.fetchall()
        with open(f"agrupacion/{provincia[0]}.csv", "w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow(localidades)
except mysql.Error as e:
    db.rollback()
    print(f"Error al obtener los datos de MySQL: {e}")
    exit(1)