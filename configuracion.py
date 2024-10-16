from flask import Blueprint, render_template, request, redirect, url_for, flash
import os

config_bp = Blueprint('configuracion', __name__)

# Ruta para manejar la carga del logo y los datos de conexión
@config_bp.route('/configuracion', methods=['GET', 'POST'])
def configuracion():
    if request.method == 'POST':
        # Obtener la base de datos seleccionada
        db_type = request.form.get('db_type')  # Asegúrate que el name del select en HTML sea "db_type"
        if not db_type:
            flash('Debe seleccionar una base de datos', 'error')
            return redirect(url_for('configuracion.configuracion'))
        
        # Manejo de los datos de conexión
        host = request.form.get('host')
        database = request.form.get('database')
        user = request.form.get('user')
        password = request.form.get('password')

        # Guardar los datos de conexión en un archivo o variables de entorno
        save_connection_data(db_type, host, database, user, password)

        # Manejo de la carga del logo
        logo = request.files.get('logo')
        if logo:
            logo_path = os.path.join('static/images', logo.filename)
            logo.save(logo_path)
            flash('Logo actualizado exitosamente', 'success')

        return redirect(url_for('configuracion.configuracion'))

    return render_template('configuracion.html')

# Función para guardar los datos de conexión
def save_connection_data(db_type, host, database, user, password):
    with open('config/db_config.txt', 'w') as f:
        f.write(f'db_type={db_type}\n')
        f.write(f'host={host}\ndatabase={database}\nuser={user}\npassword={password}\n')
