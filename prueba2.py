datos = {0: 
        {0: 'SEDE_CODIGO', 1: 'PERIODO_ID', 2: 'PERIODO_ANIO', 3: 'SEDEPRINC_CODIGO', 4: 'SEDEPRINC_NOMBRE', 5: 'SEDEPRINC_DIRECCION', 6: 'ID_CLASIFICACION_MUNICIPIO', 7: 'CODIGOINTERNODEPTO', 8: 'DEPTO', 9: 'CODIGOINTERNOMUNI', 10: 'MUNI', 11: 'AREA_ID', 12: 'AREA_CODIGO', 13: 'AREA_NOMBRE', 14: 'SEDEPRINC_TIPO', 15: 'TIPOPRINCIPAL'}, 
        'count': {0: 35066, 1: 35066, 2: 35066, 3: 35066, 4: 35066, 5: 35066, 6: 35066, 7: 35066, 8: 35066, 9: 35066, 10: 35066, 11: 35066, 12: 35066, 13: 35066, 14: 35066, 15: 35066}, 
        'unique': {0: 35066, 1: 1, 2: 1, 3: 6639, 4: 5982, 5: 7340, 6: 1113, 7: 31, 8: 31, 9: 1113, 10: 1031, 11: 2, 12: 2, 13: 2, 14: 2, 15: 2}, 
        'top': {0: '268229000261', 1: '9', 2: '2022', 3: '276275001461', 4: 'INSTITUCIÓN EDUCATIVA JORGE ELIECER GAITÁN', 5: 'COM INDIGENA LOMAGORDA FINC EL CAJÓN', 6: '13', 7: '05', 8: 'Antioquia', 9: '44847', 10: 'Uribia', 11: '2', 12: '2', 13: 'Rural', 14: 'E', 15: 'Existente'}, 
        'freq': {0: 1, 1: 35066, 2: 35066, 3: 65, 4: 113, 5: 63, 6: 474, 7: 4116, 8: 4116, 9: 474, 10: 474, 11: 24693, 12: 24693, 13: 24693, 14: 34905, 15: 34905}}

for key, val1 in datos.items():
    print(key)

    for key2, val2 in val1.items():
        print(val2)
    print()