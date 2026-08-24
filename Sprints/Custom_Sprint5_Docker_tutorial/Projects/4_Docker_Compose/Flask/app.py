from flask import Flask
import pymysql

app = Flask(__name__)


def db_connection():
    # Connect to the MySQL database
    connection = pymysql.connect(
        host="mysql_container",  # Use the service name defined in docker-compose.yml
        user="demo_user",
        password="demopassword",
        database="demo_db",
    )
    return connection


@app.route("/")
def home():
    return "Hello,Flask from container!"


@app.route("/insert_data")
def insert_data():

    # Connect to the MySQL database
    connection = db_connection()
    cursor = connection.cursor()

    # Insert data into the demo_table
    cursor.execute(
        "INSERT INTO users (city,temperature) VALUES (%s, %s)", ("New York", 25)
    )
    connection.commit()

    # Close the database connection
    cursor.close()
    connection.close()

    return "Data inserted successfully!"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
