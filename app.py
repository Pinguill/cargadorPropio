from flask import Flask, render_template
from flask_wtf import CSRFProtect
from cargue import cargue_bp  # Importa el blueprint de cargue
from consulta import consulta_bp
from configuracion import config_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mysecretkey'
csrf = CSRFProtect(app)
app.register_blueprint(cargue_bp)
app.register_blueprint(consulta_bp)
app.register_blueprint(config_bp)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/cargue')
def cargue():
    return render_template('cargue.html')

@app.route('/consulta')
def consulta():
    return render_template('consulta.html')

@app.route('/configuracion')
def configuracion():
    return render_template('configuracion.html')

if __name__ == '__main__':
    app.run(debug=True)
