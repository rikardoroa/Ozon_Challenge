import pandas as pd
import json
import re
import os
import numpy as np
import bson
from datetime import datetime
from bson.json_util import dumps
from pymongo import MongoClient, errors


class ozon_challenge:
    #declaramos la ruta para leer los archivos
    path_to_xlsx_file = os.path.abspath("BD Motos.xlsx")
    path_to_csv_file = os.path.abspath("brands.csv")

    #inicializamos variables
    def __init__(self, df1=pd.read_excel(path_to_xlsx_file, skiprows=1),
                 df2=pd.read_csv(path_to_csv_file)):
        self.df1 = df1
        self.df2 = df2
        self.path = os.path.abspath("bikes_data.json")
        self.client = MongoClient('localhost', 27017)

    def transforming_data(self):
        """ funcion que genera todas las transformaciones en pandas
            para la creación de un dataframe para generar el archivo
            json final"""
        try:
            #generamos un nuevo dataframe y un campo date
            df3 = pd.DataFrame()
            today = datetime.today()
            df = {today}
            df = pd.DataFrame(df).reset_index(drop=True).rename(columns={0: "createdAt"})
            cols = []
            #se realiza un data cleaning a las columnas del dataframe
            #para remover espacios en blanco
            self.df1.columns = self.df1.columns.str.replace(" ", "")
            self.df2 = self.df2.rename(columns={"name": "marca"})
            #se realiza un join entre dataframes(brands.csv y DB Motos) y se captura el id del archivo csv
            self.df2 = self.df2[["_id", "marca"]]
            self.df1 = pd.merge(left=self.df1, right=self.df2, how="left", on=["marca"])
            for col in self.df1.columns:
                cols.append(col)
            #se realiza limpieza del dataframe limpiando las filas vacias
            df2 = self.df1.loc[:, cols].dropna(thresh=2)
            cols = cols[0]
            df2 = df2.dropna(subset=[cols])
            #se realizan las transformaciones en el dataframe para la creacion
            #de las columnas para generar el archivo json
            df3["_id"] = df2["count"].apply(lambda x: bson.objectid.ObjectId())
            df3["salePrice"] = np.where(df2["Precioventadescuento"].isnull(), 0, df2["cuotasemanaldescuento"])
            df3["year"] = df2["año"].astype(str)
            df3["year"] = df3["year"].apply(lambda x: x.replace(".0", ""))
            df3["milage"] = df2["kilometraje_aprox"]
            df3["oldPrice"] = np.where(df2["Precioventadescuento"].isnull(), 0, df2["cuota"])
            df3["internalId"] = df2["id_ozon"]
            df3["vehicleSN"] = df2["serie_vehicular_o_num_chasis"]
            df3["engineSN"] = df2["num_motor"]
            df3["purchaseCost"] = df2["gasto_compra"]
            #se implementa un regex para filtrar el color(se toma el primer color de la columna)
            df3["Color"] = df2["Color"].apply(lambda x: re.sub(r"([?<=\W][A-Za-z]*)", r"", str(x)))
            df3["cylindersCapacity"] = df2["cilindraje"]
            df3["brand"] = df2["_id"]
            df3["brand"] = df3["brand"].apply(lambda x: bson.objectid.ObjectId())
            for value in df.createdAt:
                df3["createdAt"] = value
                df3["updatedAt"] = value
            df3["createdAt"] = df3["createdAt"].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
            df3["updatedAt"] = df3["updatedAt"].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
            df3["country"] = df2["pais"]
            df3["plate"] = df2["placa"]
            df3["registrationCard"] = df2["num_tarjeta_circ"]
            #se crea un dataframe adicional
            #para generar las claves "year" y "milage" en un diccionario
            df4 = df3[["year", "milage"]]
            df3 = df3.drop(columns=["year", "milage"])
            json_df3_data = df3.to_dict('records')
            #se genera un diccionario con todas las claves
            #para el archivo json inicial
            df5 = pd.DataFrame()
            for row in df4.iterrows():
                df5['details'] = df4.apply(lambda x: json.loads(json.dumps(x.to_dict())), axis=1)
            json_df5_data = df5.to_dict('records')
            #se hace una union de ambos diccionarios
            #para generar un solo dataframe que posteriormente generara un json
            doc_3 = []
            for item_x, item_y in zip(json_df3_data, json_df5_data):
                doc_1 = item_x
                doc_2 = item_y
                doc_1.update(doc_2)
                doc_3.append(doc_1)
            self.df1 = pd.DataFrame(doc_3)
            return self.df1
        except FileNotFoundError:
            print("error file not found in path!")


    def filtering_data(self):
        """Funcion que filtra todas las motos con placa menor a 1000
        y las motocicletas cuyo pais es mexico y aplica valores nulos a los valores "NA,na y nan" """
        try:
            #se implementa un regex para crear el campo numerico correspondiente
            #al valor de la placa
            new_cols = []
            self.df1["numeric_internal_ind"] = self.df1["internalId"].apply(
                lambda x: re.sub(r"[A-Za-z](?<=[\d])*", r"", x)).astype(int)
            #aplicando valores nulos si los valores son "nan","NA" o "na"
            for new_col in self.df1.columns:
                new_cols.append(new_col)
                self.df1[new_col] = np.where(self.df1[new_col].isnull(), np.nan, self.df1[new_col])
                self.df1[new_col] = np.where((self.df1[new_col] == "nan") | (self.df1[new_col] == "NA") | (self.df1[new_col] == "na"), np.nan, self.df1[new_col])
            #se realiza el filtrado de pais y valores de placa
            self.df1 = self.df1.loc[(self.df1.country == "mexico") & (self.df1["numeric_internal_ind"] < 1000), new_cols]
            self.df1 = self.df1.drop(columns=["numeric_internal_ind"])
            #se genera el json final que se insertara en mongodb
            self.df1 = self.df1.to_dict("records")
            with open(self.path, "w") as my_file:
                json_object = bson.json_util.dumps(self.df1, indent=4)
                my_file.write(json_object)
        except FileNotFoundError:
            print("file not found in path!")


    def data_insert(self):
        """ Funcion que valida la existencia de la base de datos y la coleccion
         en mongodb para la inserción del archivo json"""
        try:
            #se carga el archivo json para ser insertado en la bd de mongo
            with open(self.path, "r") as load_file:
                json_file = json.dumps(json.load(load_file))
                formatted_file = json.loads(json_file, object_hook=bson.json_util.object_hook)
            #se realizan las validaciones para crear la bd y la coleccion
            #para insertar el archivo json en la coleccion
            #se crea el indice(para evitar duplicados en el documento una vez se inserte) y la coleccion
            dblist = self.client.list_database_names()
            #crea la base de datos si no existe y crea la coleccion para insertar los datos
            if "bikes_data" not in dblist:
                db = self.client["bikes_data"]
                db.create_collection("bikes")
                Collection = db["bikes"]
                Collection.insert_many(formatted_file)
                Collection.create_index([("internalId", 1)], unique=True)
            #si la coleccion no existe dentro de la base de datos, crea la coleccion e inserta los datos
            if "bikes_data" in dblist:
                db = self.client["bikes_data"]
                all_collections = db.list_collection_names()
                if "bikes" not in all_collections:
                    db.create_collection("bikes")
                    Collection = db["bikes"]
                    Collection.insert_many(formatted_file)
                    Collection.create_index([("internalId", 1)], unique=True)
        except errors.CollectionInvalid:
            print("collection already exist!")




if __name__ == "__main__":
    mongo_etl = ozon_challenge()
    mongo_etl.transforming_data()
    mongo_etl.filtering_data()
    mongo_etl.data_insert()
