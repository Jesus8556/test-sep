from flask import Flask, jsonify
from pyspark.sql import SparkSession
import os

app = Flask(__name__)

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

# Ruta para obtener todas las películas
@app.route('/movies', methods=['GET'])
def get_movies():
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

    # Devolver el JSON de películas
    return jsonify(eval(movies_data))

# Cerrar la sesión de Spark al terminar la aplicación
@app.teardown_appcontext
def shutdown_session(exception=None):
    spark.stop()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
