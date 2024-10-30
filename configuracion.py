from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import SubmitField
import os

config_bp = Blueprint('configuracion', __name__)

# Define el formulario con CSRF y un botón de submit
class ConfigForm(FlaskForm):
    submit = SubmitField('Guardar Configuración')

# Función para guardar los datos de conexión
def save_connection_data(db_type, host, database, user, password):
    with open('config/db_config.txt', 'w') as f:
        f.write(f'db_type={db_type}\n')
        f.write(f'host={host}\ndatabase={database}\nuser={user}\npassword={password}\n')

@config_bp.route('/configuracion', methods=['GET', 'POST'])
def configuracion():
    form = ConfigForm()  # Instancia el formulario para el token CSRF
    if form.validate_on_submit():  # Valida el CSRF y la solicitud POST
        db_type = request.form.get('db_type')
        if not db_type:
            flash('Debe seleccionar una base de datos', 'error')
            return redirect(url_for('configuracion.configuracion'))
        
        # Obtener los datos de conexión
        host = request.form.get('host')
        database = request.form.get('database')
        user = request.form.get('user')
        password = request.form.get('password')

        # Guarda los datos de conexión
        save_connection_data(db_type, host, database, user, password)

        # Manejo de la carga del logo
        logo = request.files.get('logo')
        if logo:
            logo_path = os.path.join('static/images', logo.filename)
            logo.save(logo_path)
            flash('Logo actualizado exitosamente', 'success')

        return redirect(url_for('configuracion.configuracion'))

    return render_template('configuracion.html', form=form)