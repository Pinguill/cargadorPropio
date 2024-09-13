from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/cargue')
def cargue():
    return render_template('cargue.html')

@app.route('/consulta')
def consulta():
    return render_template('consulta.html')

if __name__ == '__main__':
    app.run(debug=True)
