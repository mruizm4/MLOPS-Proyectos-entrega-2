from fastapi import FastAPI, Query
from typing import List, Annotated
import pandas as pd
from enum import Enum

from predict import load_model_from_minio, predict_new_data, safe_load


app = FastAPI(
    title="CoverType Prediction API",
    version="1.0"
)


# ------------------------------------------------------------------------------
# Cargar modelos al iniciar
# ------------------------------------------------------------------------------

class model_class(str, Enum):
    TREE = "TREE"
    KNN = "KNN"
    SVM = "SVM"




@app.post("/predict")
async def predict(

    models: Annotated[
        List[model_class],
        Query(
            ...,
            description="Lista de modelos a utilizar para la inferencia. Permite comparar resultados entre múltiples modelos o ejecutar inferencia tipo ensemble."
        )
    ],

    Wilderness_Area:str =Query(
            "Rawah",
            description="Área silvestre a la que pertenece la celda de terreno. Corresponde a una variable categórica one-hot del dataset Covertype."
        )
    ,

    Soil_Type: str = Query(
            "C2702",
            description="Tipo de suelo dominante en la celda. Variable categórica one-hot entre los 40 tipos ecológicos definidos en el dataset."
        )
    ,

    bucket: str = Query(
        "models-bucket",
        description="Elevación del terreno en metros sobre el nivel del mar."
    ),
    Elevation: float = Query(
        2500,
        description="Elevación del terreno en metros sobre el nivel del mar."
    ),
    

    Aspect: float = Query(
        120,
        description="Orientación azimutal de la pendiente en grados (0–360). Influye en la exposición solar."
    ),

    Slope: float = Query(
        10,
        description="Pendiente del terreno en grados."
    ),

    Horizontal_Distance_To_Hydrology: float = Query(
        100,
        description="Distancia horizontal en metros hasta la fuente de agua más cercana."
    ),

    Vertical_Distance_To_Hydrology: float = Query(
        20,
        description="Distancia vertical en metros respecto a la fuente de agua más cercana. Puede ser positiva o negativa."
    ),

    Horizontal_Distance_To_Roadways: float = Query(
        300,
        description="Distancia horizontal en metros hasta la carretera o vía más cercana."
    ),

    Hillshade_9am: float = Query(
        220,
        description="Índice de sombreado del terreno a las 9 AM calculado a partir de la topografía."
    ),

    Hillshade_Noon: float = Query(
        230,
        description="Índice de sombreado del terreno al mediodía."
    ),

    Hillshade_3pm: float = Query(
        200,
        description="Índice de sombreado del terreno a las 3 PM."
    ),

    Horizontal_Distance_To_Fire_Points: float = Query(
        500,
        description="Distancia horizontal en metros hasta el punto histórico de incendio forestal más cercano."
    ),

    ):
    """
    Realiza inferencia del tipo de cobertura forestal (Cover Type) usando uno o múltiples modelos previamente registrados.

    Este endpoint recibe variables cartográficas y ambientales correspondientes a una celda de terreno de 30x30 metros
    y retorna la predicción del tipo de bosque dominante.

    Parámetros
    ----------
    models : List[model_class]
        Lista de modelos a utilizar para la inferencia. Permite realizar inferencia comparativa o ensemble.
        El selector se construye automáticamente desde los modelos disponibles.

    Wilderness_Area : WildernessEnum
        Área silvestre a la que pertenece la celda de terreno. Representa una codificación categórica one-hot
        correspondiente a una de las cuatro zonas ecológicas del dataset.

    Soil_Type : SoilEnum
        Tipo de suelo dominante en la celda. Representa una codificación categórica one-hot de los 40 tipos de suelo
        definidos en el dataset Covertype.

    Elevation : float, default=2500
        Elevación del terreno en metros sobre el nivel del mar.

    Aspect : float, default=120
        Orientación de la pendiente en grados azimutales (0–360). Determina la exposición solar del terreno.

    Slope : float, default=10
        Inclinación del terreno en grados.

    Horizontal_Distance_To_Hydrology : float, default=100
        Distancia horizontal en metros hasta la fuente de agua más cercana (río, lago o drenaje).

    Vertical_Distance_To_Hydrology : float, default=20
        Distancia vertical en metros respecto a la fuente de agua más cercana. Puede ser positiva o negativa.

    Horizontal_Distance_To_Roadways : float, default=300
        Distancia horizontal en metros hasta la vía o carretera más cercana.

    Hillshade_9am : float, default=220
        Índice de iluminación del terreno a las 9:00 AM. Representa sombreado simulado según topografía.

    Hillshade_Noon : float, default=230
        Índice de iluminación del terreno al mediodía.

    Hillshade_3pm : float, default=200
        Índice de iluminación del terreno a las 3:00 PM.

    Horizontal_Distance_To_Fire_Points : float, default=500
        Distancia horizontal en metros hasta el punto histórico de incendio forestal más cercano.

    Retorna
    -------
    dict
        Predicción del tipo de cobertura forestal para cada modelo solicitado, junto con posibles probabilidades
        o métricas adicionales definidas por el servicio.
    """

    df = pd.DataFrame([{
        "Elevation": Elevation,
        "Aspect": Aspect,
        "Slope": Slope,
        "Horizontal_Distance_To_Hydrology": Horizontal_Distance_To_Hydrology,
        "Vertical_Distance_To_Hydrology": Vertical_Distance_To_Hydrology,
        "Horizontal_Distance_To_Roadways": Horizontal_Distance_To_Roadways,
        "Hillshade_9am": Hillshade_9am,
        "Hillshade_Noon": Hillshade_Noon,
        "Hillshade_3pm": Hillshade_3pm,
        "Horizontal_Distance_To_Fire_Points": Horizontal_Distance_To_Fire_Points,
        "Wilderness_Area": Wilderness_Area,
        "Soil_Type": Soil_Type
    }])

    tree_model, tree_scaler = safe_load("models/decision_tree.pkl", bucket=bucket)
    knn_model, knn_scaler = safe_load("models/knn.pkl", bucket=bucket)
    svm_model, svm_scaler = safe_load("models/svm.pkl", bucket=bucket)



    models_dict = {
        "TREE": {"model": tree_model, "scaler": tree_scaler},
        "KNN": {"model": knn_model, "scaler": knn_scaler},
        "SVM": {"model": svm_model, "scaler": svm_scaler},
    }




    response = {}

    if tree_model is None or knn_model is None:#or svm_model is None:
        return {"error": f'Modelos no disponibles. Revise que esten subidos al bucket "{bucket}". Si "{bucket}" no existe, créelo, entrene los modelos e intente nuevamente.'}
    else:

        for m in models:

            model_name = m.value

            prediction = predict_new_data(
                df,
                models_dict[model_name]["model"],
                models_dict[model_name]["scaler"]
            )

            response[model_name] = prediction.tolist()

        return response