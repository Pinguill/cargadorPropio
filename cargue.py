import os
from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

# Configura el blueprint para el módulo de cargue
cargue_bp = Blueprint('cargue', __name__)

# Directorio donde se guardarán los archivos subidos
UPLOAD_FOLDER = '../../../../Lake/data/'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@cargue_bp.route('/cargue', methods=['GET', 'POST'])
def cargue():
    if request.method == 'POST':
        new_filename = request.form.get('new_filename')
        if 'file' not in request.files:
            
            flash('No se ha seleccionado ningún archivo')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('No se ha seleccionado ningún archivo')
            return redirect(request.url)

        if file:
            original_filename = secure_filename(file.filename)
            file_extension = os.path.splitext(original_filename)[1]

            if new_filename:
                filename = secure_filename(new_filename) + file_extension
            else:
                filename = original_filename

            file.save(os.path.join(UPLOAD_FOLDER, filename))
            flash(f'Archivo {filename} cargado exitosamente')
            return redirect(url_for('cargue.cargue'))

    return render_template('cargue.html')
