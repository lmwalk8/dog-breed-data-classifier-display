from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def home():
    return render_template('index.html', title='Home Page')

@app.route("/dog/<breed>")
def dog(breed):
    return render_template('dog.html', breed=breed)

if __name__ == '__main__':
    app.run(debug=True)
