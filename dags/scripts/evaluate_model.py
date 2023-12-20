import os
import mlflow
import numpy as np
import pandas as pd

from airflow.models import Variable
from scripts.supp_functions import read_mysql_table
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report

def evaluate_model():

    os.environ['AWS_ACCESS_KEY_ID'] = Variable.get('AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get('AWS_SECRET_ACCESS_KEY')
    os.environ['AWS_DEFAULT_REGION'] = Variable.get('AWS_DEFAULT_REGION')
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = Variable.get('MLFLOW_S3_ENDPOINT_URL')

    run_name = reg_model_name = "CatBoostClassifier"

    mlflow.set_tracking_uri(Variable.get("MLFLOW_TRACKING_URI"))

    try:
        print(f"Попытка создать эксперимент {reg_model_name}")
        mlflow.create_experiment(reg_model_name)
    except:
        mlflow.set_experiment(reg_model_name)
    

    with mlflow.start_run(run_name=run_name) as run:

        dataframe = read_mysql_table("SELECT * FROM dataframe")

        df5k = dataframe[dataframe["state_of_selected_competition"] == 0]
        df5k = (
            pd.concat([
                df5k.sample(5000, random_state=42),
                dataframe[dataframe["state_of_selected_competition"] == 1]
            ])
        )

        target = np.ravel(df5k[['state_of_selected_competition']])
        df = df5k.drop(columns=["final_consent", "state_of_selected_competition"])

        cols = df.columns
        cat_columns = [col for col in cols if df[col].dtypes == object]
        num_columns = [col for col in cols if df[col].dtypes != object]

        print('categorical columns:\t ',cat_columns, '\n len = ',len(cat_columns))

        print('numerical columns:\t ',  num_columns, '\n len = ',len(num_columns))

        from sklearn.model_selection import train_test_split

        X_train, X_test, y_train, y_test = train_test_split(
            df,
            target,
            test_size=0.1,
            random_state=42
        )

        from catboost import Pool

        feature_names = list(df.columns)

        print(feature_names)

        train_data = Pool(
            data=X_train,
            label=y_train,
            cat_features = cat_columns,
            feature_names = feature_names
        )

        eval_data = Pool(
            data=X_test,
            label=y_test,
            cat_features = cat_columns,
            feature_names=feature_names
        )

        model = CatBoostClassifier(
            iterations = 2000, #кол-во итераций
            max_depth=8, #глубина деревьев
            verbose = 100, #красноречивость модели (влияет на кол-во логов, не на точность)
            cat_features = cat_columns,
            eval_metric= 'BalancedAccuracy'
        )

        model.fit(X=train_data, eval_set=eval_data)

        y_predict=model.predict(eval_data)

        print(classification_report(y_test, y_predict, target_names=["0", "1"]))

        mlflow.catboost.log_model(
            model, 
            run_name, 
            registered_model_name=reg_model_name
        )