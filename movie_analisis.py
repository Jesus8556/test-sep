from flask import Flask, jsonify
from pyspark.sql import SparkSession
import os

app = Flask(__name__)

# Ruta para obtener todas las películas
@app.route('/movies', methods=['GET'])
def get_movies():
    # Inicializar la sesión de Spark dentro de la función
    spark = SparkSession.builder \
        .appName("Movie Ratings Analysis") \
        .getOrCreate()

    # Verificar si el archivo CSV ya ha sido procesado
    if not os.path.exists("movies_processed.json"):
        # Leer el archivo movies.csv
        movies_df = spark.read.csv("movies.csv", header=True, inferSchema=True)

        # Convertir a JSON y guardar
        movies_json = movies_df.toJSON().collect()
        with open("movies_processed.json", "w") as f:
            f.write("[\n" + ",\n".join(movies_json) + "\n]")

    # Leer el archivo JSON procesado
    with open("movies_processed.json", "r") as f:
        movies_data = f.read()

    # Cerrar la sesión de Spark después de procesar
    spark.stop()

    # Devolver el JSON de películas
    return jsonify(eval(movies_data))

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
