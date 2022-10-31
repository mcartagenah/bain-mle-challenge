from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pickle
import logging
import numpy as np

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

app = FastAPI()

model = {'predictor': None}

try:
    with open('/app/models/trained_model.pkl', 'rb') as f:
        model['predictor'] = pickle.load(f)
except Exception as err:
    logger.exception(err)

class ModelInput(BaseModel):
    ano: float
    mes: float
    coquimbo: float
    valparaiso: float
    metropolitana_de_santiago: float
    libertador_gral__bernardo_o_higgins: float
    maule: float
    biobio: float
    la_araucania: float
    los_rios: float
    pib_agropecuario_silvicola: float
    pib_pesca: float
    pib_mineria: float
    pib_mineria_del_cobre: float
    pib_otras_actividades_mineras: float
    pib_industria_manufacturera: float
    pib_alimentos: float
    pib_bebidas_y_tabaco: float
    pib_textil: float
    pib_maderas_y_muebles: float
    pib_celulosa: float
    pib_refinacion_de_petroleo: float
    pib_quimica: float
    pib_minerales_no_metalicos_y_metalica_basica: float
    pib_productos_metalicos: float
    pib_electricidad: float
    pib_construccion: float
    pib_comercio: float
    pib_restaurantes_y_hoteles: float
    pib_transporte: float
    pib_comunicaciones: float
    pib_servicios_financieros: float
    pib_servicios_empresariales: float
    pib_servicios_de_vivienda: float
    pib_servicios_personales: float
    pib_administracion_publica: float
    pib_a_costo_de_factores: float
    pib: float
    imacec_empalmado: float
    imacec_produccion_de_bienes: float
    imacec_minero: float
    imacec_industria: float
    imacec_resto_de_bienes: float
    imacec_comercio: float
    imacec_servicios: float
    imacec_a_costo_de_factores: float
    imacec_no_minero: float
    num: float


@app.get("/reload_model")
def reload_model():
    try:
        with open('/app/models/trained_model.pkl', 'rb') as f:
            model['predictor'] = pickle.load(f)
        return {"model": "Model loaded"}
    except Exception as err:
        logger.exception(err)
        return {"model": None}

@app.post("/predict")
def predict(x: ModelInput):
    if model['predictor'] != None:
        input_array = []
        for _, value in x:
            input_array.append(value)
        input_array = np.array(input_array).reshape(1, len(x.__fields__))
        prediction = model['predictor'].predict(input_array)[0]
        return {'prediction': prediction}
    else:
        raise HTTPException(status_code=503, detail="Model not loaded.")
