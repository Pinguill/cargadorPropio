from flask import Flask, render_template
from cargue import cargue_bp  # Importa el blueprint de cargue
from consulta import consulta_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mysecretkey'
app.register_blueprint(cargue_bp)
app.register_blueprint(consulta_bp)

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
