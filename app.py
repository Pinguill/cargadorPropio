from flask import Flask, render_template
from flask_wtf import CSRFProtect
from cargue import cargue_bp  # Importa el blueprint de cargue
from consulta import consulta_bp
from configuracion import config_bp
from cargueTR import cargueTR_bp

app = Flask(__name__)
csrf = CSRFProtect(app)  # Inicializa la protección CSRF

app.config['SECRET_KEY'] = 'mysecretkey'

app.register_blueprint(cargue_bp)
app.register_blueprint(consulta_bp)
app.register_blueprint(config_bp)
app.register_blueprint(cargueTR_bp)

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
