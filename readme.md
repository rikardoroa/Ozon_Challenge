## OzonChallenge

###### Backend Script en Python que genera una coleccion , una base de datos y un archivo JSON para ser insertado en MongoDB

## Consideraciones Previas

* 1. **__Instalar MongoDB Compass__** para ejecutar el script en local

* 2. Si se va ejecutar el script en otra instancia de mongo, solo cambiar la conexion en el script

* 3. Tener las siguientes librerias instaladas: **_pandas, numpy y pymongo_**

* 4. Las librerias por defecto de Python que son utilizadas en este proyecto son: **_re, json, os y datetime_** 

* 5. Al instalar la libreria **__pymongo__** tambien se instala la libreria **__bson.json_util__**, para esta solucion se debe importar el paquete **__dumps__** de esta libreria de la siguiente manera **__from bson.json_util import dumps__**, con el fin de aplicar todas las conversiones a los datos correspondientes para generar el archivo json final.


## Descripcion del script

* 1 -> El script genera automaticamente un archivo JSON para ser insertado en MongoDB

* 2 -> El script crea una base de datos y una coleccion y hace una validacion de datos


## enjoy!