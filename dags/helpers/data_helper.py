import pandas as pd

def process_milk_data():
    precio_leche = pd.read_csv('/opt/airflow/data/precio_leche.csv')
    precio_leche.rename(columns = {'Anio': 'ano', 'Mes': 'mes_pal'}, inplace = True) # precio = nominal, sin iva en clp/litro
    months_dict = {'Ene':'Jan', 'Feb':'Feb', 'Mar':'Mar', 'Abr':'Apr', 'May':'May', 'Jun':'Jun', 'Jul':'Jul', 'Ago':'Aug', 'Sep':'Sep', 'Oct':'Oct', 'Nov':'Nov', 'Dic':'Dec'}
    precio_leche['mes_pal'] = precio_leche['mes_pal'].apply(lambda x: months_dict[x])
    precio_leche['mes'] = pd.to_datetime(precio_leche['mes_pal'], format = '%b')
    precio_leche['mes'] = precio_leche['mes'].apply(lambda x: x.month)
    precio_leche['mes-ano'] = precio_leche.apply(lambda x: f'{x.mes}-{x.ano}', axis = 1)
    precio_leche.to_csv('/opt/airflow/data/preprocessed_milk_data.csv', index=False)

def process_rain_data():
    precipitaciones = pd.read_csv('/opt/airflow/data/precipitaciones.csv')
    precipitaciones['date'] = pd.to_datetime(precipitaciones['date'], format = '%Y-%m-%d')
    precipitaciones = precipitaciones.sort_values(by='date', ascending=True).reset_index(drop=True)
    precipitaciones['mes'] = precipitaciones.date.apply(lambda x: x.month)
    precipitaciones['ano'] = precipitaciones.date.apply(lambda x: x.year)
    precipitaciones.to_csv('/opt/airflow/data/preprocessed_rain_data.csv', index=False)

def process_bank_data():
    banco_central = pd.read_csv('/opt/airflow/data/banco_central.csv')
    banco_central['Periodo'] = banco_central['Periodo'].apply(lambda x: x[0:10])
    banco_central['Periodo'] = pd.to_datetime(banco_central['Periodo'], format = '%Y-%m-%d', errors = 'coerce')
    banco_central.drop_duplicates(subset='Periodo', inplace=True)
    banco_central = banco_central[~banco_central.Periodo.isna()]
    
    def convert_int(x):
        return int(x.replace('.', ''))

    cols_pib = [x for x in list(banco_central.columns) if 'PIB' in x]
    cols_pib.extend(['Periodo'])
    banco_central_pib = banco_central[cols_pib]
    banco_central_pib = banco_central_pib.dropna(how='any', axis=0)

    for col in cols_pib:
        if col == 'Periodo':
            continue
        else:
            banco_central_pib[col] = banco_central_pib[col].apply(lambda x: convert_int(x))

    def to_100(x):
        x = x.split('.')
        if x[0].startswith('1'):
            if len(x[0]) >2:
                return float(x[0] + '.' + x[1])
            else:
                x = x[0]+x[1]
                return float(x[0:3] + '.' + x[3:])
        else:
            if len(x[0])>2:
                return float(x[0][0:2] + '.' + x[0][-1])
            else:
                x = x[0] + x[1]
                return float(x[0:2] + '.' + x[2:])
            
    cols_imacec = [x for x in list(banco_central.columns) if 'Imacec' in x]
    cols_imacec.extend(['Periodo'])
    banco_central_imacec = banco_central[cols_imacec]
    banco_central_imacec = banco_central_imacec.dropna(how = 'any', axis = 0)

    for col in cols_imacec:
        if col == 'Periodo':
            continue
        else:
            banco_central_imacec[col] = banco_central_imacec[col].apply(lambda x: to_100(x))
            assert(banco_central_imacec[col].max()>100)
            assert(banco_central_imacec[col].min()>30)

    banco_central_iv = banco_central[['Indice_de_ventas_comercio_real_no_durables_IVCM', 'Periodo']]
    banco_central_iv = banco_central_iv.dropna() # -unidades? #parte 
    banco_central_iv = banco_central_iv.sort_values(by = 'Periodo', ascending = True)
    banco_central_iv['num'] = banco_central_iv.Indice_de_ventas_comercio_real_no_durables_IVCM.apply(lambda x: to_100(x))
    
    banco_central_num = pd.merge(banco_central_pib, banco_central_imacec, on = 'Periodo', how = 'inner')
    banco_central_num = pd.merge(banco_central_num, banco_central_iv, on = 'Periodo', how = 'inner')
    banco_central_num['mes'] = banco_central_num['Periodo'].apply(lambda x: x.month)
    banco_central_num['ano'] = banco_central_num['Periodo'].apply(lambda x: x.year)
    banco_central_num.to_csv('/opt/airflow/data/preprocessed_bank_data.csv', index=False)

def feature_engineering():
    precio_leche = pd.read_csv('/opt/airflow/data/preprocessed_milk_data.csv')
    precipitaciones = pd.read_csv('/opt/airflow/data/preprocessed_rain_data.csv')
    banco_central_num = pd.read_csv('/opt/airflow/data/preprocessed_bank_data.csv')

    precio_leche_pp = pd.merge(precio_leche, precipitaciones, on=['mes', 'ano'], how='inner')
    precio_leche_pp.drop('date', axis=1, inplace=True)

    precio_leche_pp_pib = pd.merge(precio_leche_pp, banco_central_num, on=['mes', 'ano'], how='inner')
    precio_leche_pp_pib.drop(['Periodo', 'Indice_de_ventas_comercio_real_no_durables_IVCM', 'mes-ano', 'mes_pal'], axis=1, inplace=True)

    precio_leche_pp_pib.to_csv('/opt/airflow/data/augmented_milk_data.csv', index=False)
