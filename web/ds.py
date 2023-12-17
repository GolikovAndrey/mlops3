import pandas as pd

df = pd.read_csv("web/train.csv", sep=";")
kladr = pd.read_csv("web/KLADR.csv", sep=";", encoding='utf-8').rename(
    columns={"NAME": "Населённый пункт", "CODE": "Код насел. пункта"})[["Населённый пункт", "Код насел. пункта"]]


def normalize_sum_ball(ball):
    ball = (ball - df['Сумма баллов'].min()) / (
            df['Сумма баллов'].max() - df['Сумма баллов'].min())
    return ball


def mormalize_achieve_ball(ball):
    ball = (ball - df['Сумма баллов за индивидуальные достижения'].min()) / (
            df['Сумма баллов за индивидуальные достижения'].max() -
            df['Сумма баллов за индивидуальные достижения'].min())
    return ball


def normalize_Age(age):
    age = (age - df['Возраст'].min()) / (df['Возраст'].max() - df['Возраст'].min())


def get_cites():
    return df['Населённый пункт'].unique()


def get_op():
    return df['Набор ОП'].unique()


def get_edu():
    return df['Полученное образование'].unique()


def get_faculty():
    return df['Формирующее подр.'].unique()


def get_payment():
    return df['Вид возмещения затрат'].unique()


def get_form():
    return df['Форма обучения'].unique()


def get_enroll_type():
    return df['Вид приема'].unique()


# TODO
# Перенести все этапы обработки данных перед предсказанием в единый пайплайн (читать "функцию"),
# через которую будет прогоняться сырой датасет (выгрузка из ИС приемки).
def prepare_df(new_df):
    pass
    # return dataframe
