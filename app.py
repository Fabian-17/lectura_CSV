import csv 
import MySQLdb as mysql

# conexión a la base de datos
try:
    db = mysql.connect('localhost', 'root', '', 'localidades')
except mysql.Error as e:
    print(f"Error al conectar a MySQL: {e}")
    exit(1)


try:
    # Crear un cursor para ejecutar consultas
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
        data = list(lector_csv) # Convertir el lector en una lista
        # Insertar los datos en la tabla 'localidades'
        cursor.executemany("""
        INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) 
        VALUES (%s, %s, %s, %s, %s)
        """, data)
        print("Datos insertados correctamente.")

# Confirmar la transacción
    db.commit()

# Manejo de errores
except mysql.Error as e:
    db.rollback() # Revertir la transacción si hay errores  
    print(f"Error al crear o eliminar la tabla 'localidades' en MySQL: {e}")
    exit(1)


try:
        # Función para crear el archivo de conteo
    def crear_archivo_conteo(conexion, cursor):
        cursor.execute("SELECT provincia, COUNT(*) AS total_localidades FROM localidades GROUP BY provincia;")
        conteo_por_provincia = cursor.fetchall()

        with open('conteo.csv', 'w', newline='') as csvfile:
            fieldnames = ['Provincia', 'Total_Localidades']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for provincia, total_localidades in conteo_por_provincia:
                writer.writerow({'Provincia': provincia, 'Total_Localidades': total_localidades})
        print("Archivo 'conteo.csv' creado exitosamente.")

    # Llamar a la función para crear el archivo de conteo
    crear_archivo_conteo(db, cursor)
    cursor = db.cursor() # Crear un cursor para ejecutar consultas
    # Obtener las provincias
    cursor.execute("SELECT  DISTINCT provincia FROM localidades;")
    provincias =cursor.fetchall()
    # Crear un archivo CSV por cada provincia
    for provincia in provincias:
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0], ))
        localidades = cursor.fetchall()
        with open(f"agrupacion/{provincia[0]}.csv", "w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow(localidades) # Escribir las filas de las localidades
# Manejo de errores
except mysql.Error as e:
    db.rollback()
    print(f"Error al obtener los datos de MySQL: {e}")
    exit(1)