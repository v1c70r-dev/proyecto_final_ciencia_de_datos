# proyecto_final_ciencia_de_datos
Proyecto final introducción a la ciencia de datos

# Env
* create: python -m venv env
* activate: .\env\Scripts\Activate.ps1
* deactivate: deactivate

# Dependency
* pip install -r requirements.txt

# Run
* Install dependencies: docker-compose up -d
* Run server: docker-compose up

# Check the postgres database
*  psql -U airflow -d airflow -h postgres -p 5432

# Arima bug
* https://machinelearningmastery.com/save-arima-time-series-forecasting-model-python/