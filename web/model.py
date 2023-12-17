from catboost import CatBoostClassifier
import json
import pandas as pd

loaded_model = CatBoostClassifier()
loaded_model.load_model('web/catboost_model.bin')


def predict(data):
    new_data_dict = json.loads(data)
    new_data_df = pd.DataFrame([new_data_dict])
    prediction = loaded_model.predict(new_data_df)
    return prediction[0]
