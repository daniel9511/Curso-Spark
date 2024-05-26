# Fundamentos de Spark

Fecha de creación: May 18, 2024 12:51 PM
Clase: Plazti
Estado: En curso
Última edición: May 26, 2024 1:55 PM

# **Presentación del Curso**

En este curso aprenderemos sobre Spark y sus diferencias con otros frameworks, desarrollaremos un proceso ETL, aprenderemos sobre Manejo de las estructuras base de Spark y fundamentos de calidad de datos

Requisitos previos

- Programación Orientada a Objetos
- Cursos de SQL y MySQL

# **Introduccion a Apache Spark**

Spark es un framework de trabajo para el desarrollo de grandes datos o big data y se preocupa de la velocidad y continuidad del procesamiento de datos, en contraparte de Hadoop que se preocupa por un almacenamiento grande de datos.

Podemos utilizar multiples lenguajes

- Java
- Scala (Spark corre nativamente aquí)
- Python
- R

¿Que no es Apache Spark?

No es una base de datos

![https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_1.png](https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_1.png)

**OLAP:** Base de datos tradicional en tiempo real

Spark debe estar conectado a un Data warehouse para poder aprovechar toda su funcionalidad

**Hisotoria**

- Nace en 2009 en la Universidad Berkeley
- Hereda de Hadoop
- Version3 fue liberada en Junio 2020

**Spark VS Hadoop**

- Spark se enfoca en procesamiento de datos desde la memoria ram.
- Posee naturalmente un modulo para ML, streaming y grafos.
- No depende de un sistema de archivos.

# **Introduccion a los RDDs y DataFrames**

**Componentes de Spark**

Las dos principales que soporta Spark son los **RDD** y los **DataFrames**.

La diferencia reside en la estructura que poseen.

Los **RDD** son el **componente mínimo con el cual podemos comunicarnos con Spark**, es un lenguaje ensamblador de spark.

**Caracteristicas de los RDD**

- **Principal Abstracción de datos** es la unidad básica, existen desde el inicio hasta la version 3 de Spark.
- **Distribución** Los RDD se distribuyen y particionan a lo largo del cluster de maquinas conectadas.
- **Creación simple** no tienen una estructura formalmente, adoptan la mas intuitiva (listas, tuplas, etc).
- **Inmutabilidad** Posterior a su creación no se pueden modificar, permite persistencia en los datos pero en cierto punto tienes que lanzar un garbage collector para eliminar los RDDs basura para poder limpiar la memoria
- **Ejecución perezosa** a menos que se realize una acción.

![https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_2.png](https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_2.png)

Todas las **transformaciones** las realizamos sin problema, realizando una **Acción** damos vida a lo que estamos creando.

![https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_3.png](https://github.com/rb-one/Cuso_Introductorio_de_Spark/raw/master/Notes/src/spark_3.png)

En este ejemplo creamos dos RDD **comala** y **paramo** sin embargo el archivo pedroParamo.txt podría no existir, sin embargo hasta que yo ejecute el count() (una acción sabremos que hubo un error)

**Caracteristicas de los DataFrame**

- **Formato** a diferencia de un RDD poseen columnas lo cual les otorga tipos de datos.
- **Optimizacion** poseen una mejor implementación, lo cual los hace preferibles (aunque están construidos sobre los RDD).
- **Facilidad de creación** Se pueden crear desde una base de datos externa, archivo o RDD existente.

**Cuando Usar RDD**

- > Cuando te interese controlar el flujo de Spark
- > Si eres usuario de Python, convertir a RDD un conjunto permite mejor control de los datos
- > Estas conectándote a versiones antiguas de Spark.

**Cuando Usar DataFrames**

- > Si poseemos semánticas de datos complicadas (operaciones muy computadas).
- > Vamos a realizar tareas de alto nivel como filtros, mapeos, agregaciones, promedios o sumas.
- > Si vamos a usar sentencias SQL-like

**Resumen**

Los RDD y DataFrames tienen 3 caracteristicas base

- Distribuidos
- Inmutables
- Perezosos
- Estructura (solo DataFrame)

### Notas adicionales:

- Repositorio ejemplo notas:
[https://github.com/rb-one/Cuso_Introductorio_de_Spark/tree/master/files](https://github.com/rb-one/Cuso_Introductorio_de_Spark/tree/master/files)
- Como usar pyspark en google colab: 
[https://medium.com/analytics-vidhya/ultimate-guide-for-setting-up-pyspark-in-google-colab-7637f697daf1](https://medium.com/analytics-vidhya/ultimate-guide-for-setting-up-pyspark-in-google-colab-7637f697daf1)
- install anaconda on windows ubuntu terminal: [https://gist.github.com/kauffmanes/5e74916617f9993bc3479f401dfec7da](https://gist.github.com/kauffmanes/5e74916617f9993bc3479f401dfec7da)

[https://repo.anaconda.com/archive/](https://repo.anaconda.com/archive/)

- Instalación de WSL -  pyspark con ambiente conda

[Setting Up a WSL and PySpark Data Engineering Environment](https://heartbeat.comet.ml/setting-up-wsl-and-pyspark-data-engineering-environment-f717f1340115)

[https://heartbeat.comet.ml/setting-up-wsl-and-pyspark-data-engineering-environment-f717f1340115](https://heartbeat.comet.ml/setting-up-wsl-and-pyspark-data-engineering-environment-f717f1340115)