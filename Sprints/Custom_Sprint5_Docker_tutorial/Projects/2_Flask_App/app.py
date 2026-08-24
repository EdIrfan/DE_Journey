from flask import Flask


app = Flask(__name__)


@app.route("/")
def home():
    return "Hello, Flask running inside a Docker container!"


@app.route("/about")
def about():
    return "This is a simple Flask application inside a Docker container."


@app.route("/contact")
def contact():
    return "Contact us at: abc"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
