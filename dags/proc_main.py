from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from statsmodels.tsa.arima.model import ARIMA #, ARIMAResults
from statsmodels.tsa.seasonal import seasonal_decompose
import sqlalchemy
import json
import csv


##create tables
def create_tables():
    # Establish a connection to the PostgreSQL database
    with open('/opt/airflow/data/settings.json', 'r') as file:
        settings = json.load(file)
    conn = psycopg2.connect(
        host = settings["HOST"],
        port = settings["PORT"],
        database = settings["DATABASE"],
        user = settings["USER"],
        password = settings["PASSWORD"]
    )
    # Create a cursor object to interact with the database
    cur = conn.cursor()
    # Create the first table
    cur.execute("""
        create table if not exists nydp_arrest_data(
        ARREST_KEY TEXT,
        ARREST_DATE TEXT,
        PD_CD TEXT,
        PD_DESC TEXT,
        KY_CD TEXT,
        OFNS_DESC TEXT,
        LAW_CODE TEXT,
        LAW_CAT_CD TEXT,
        ARREST_BORO TEXT,
        ARREST_PRECINCT INTEGER,
        JURISDICTION_CODE REAL,
        AGE_GROUP TEXT,
        PERP_SEX TEXT,
        PERP_RACE TEXT,
        X_COORD_CD REAL,
        Y_COORD_CD REAL,
        Latitude REAL,
        Longitude REAL,
        Lon_Lat TEXT);
        """)

    # Create table predictions_bronx_m
    cur.execute(
        """
            create table if not exists predictions_bronx_m(
            week INT not null,
            prediction REAL not null);
        """)
    # Create table predictions_bronx_f
    cur.execute(
        """
            create table if not exists predictions_bronx_f(
            week INT not null,
            prediction REAL not null);
        """)

    # Create table predictions_bronx_v
    cur.execute(
        """
            create table if not exists predictions_bronx_v(
            week INT not null,
            prediction REAL not null);
        """)

    # Create table predictions_manhatan_m
    cur.execute("""
            create table if not exists predictions_manhatan_m(
            week INT not null,
            prediction REAL not null);
            """
        )

    # Create table predictions_manhatan_f
    cur.execute("""
            create table if not exists predictions_manhatan_f(
            week INT not null,
            prediction REAL not null);
            """
        )

    # Create table predictions_manhatan_v
    cur.execute("""
            create table if not exists predictions_manhatan_v(
            week INT not null,
            prediction REAL not null);
            """
        )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the database connection
    cur.close()
    conn.close()


# From csv to data table
def local_csv_to_data_base(filename, **context):
    file_path = f'/opt/airflow/data/{filename}' 

    with open('/opt/airflow/data/settings.json', 'r') as file:
        settings = json.load(file)
    conn = psycopg2.connect(
        host = settings["HOST"],
        port = settings["PORT"],
        database = settings["DATABASE"],
        user = settings["USER"],
        password = settings["PASSWORD"]
    )
    cursor = conn.cursor()
    # Check if the table is empty
    cursor.execute("SELECT COUNT(*) FROM nydp_arrest_data")
    table_count = cursor.fetchone()[0]
    if table_count == 0:
        with open(file_path, 'r') as f:
            # Skip the header row
            next(f)
            cursor.copy_expert(
                """
                COPY nydp_arrest_data
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ',', QUOTE '"', NULL '')
                """
                , f)
        # Commit the changes
        conn.commit()
        rows_affected = cursor.rowcount
        print(f"Number of rows affected: {rows_affected}")
    else:
        print("Table is not empty. Skipping data insertion.")
    cursor.close()
    conn.close()

# def local_csv_to_data_base(filename, **context):
#     file_path = f'/opt/airflow/data/{filename}' 

#     with open('/opt/airflow/data/settings.json', 'r') as file:
#         settings = json.load(file)
#     conn = psycopg2.connect(
#         host=settings["HOST"],
#         port=settings["PORT"],
#         database=settings["DATABASE"],
#         user=settings["USER"],
#         password=settings["PASSWORD"]
#     )
#     cursor = conn.cursor()
#     # Check if the table is empty
#     cursor.execute("SELECT COUNT(*) FROM nydp_arrest_data")
#     table_count = cursor.fetchone()[0]
#     if table_count == 0:
#         with open(file_path, 'r') as f:
#             csv_reader = csv.DictReader(f)
#             rows_to_insert = []
#             for row in csv_reader:
#                 arrest_date = row.get('ARREST_DATE')
#                 law_cat_cd = row.get('LAW_CAT_CD')
#                 arrest_boro = row.get('ARREST_BORO')
#                 if arrest_boro in ['M', 'B']:
#                     rows_to_insert.append((arrest_date, law_cat_cd, arrest_boro))

#             if rows_to_insert:
#                 cursor.executemany(
#                     """
#                     INSERT INTO nydp_arrest_data (ARREST_DATE, LAW_CAT_CD, ARREST_BORO)
#                     VALUES (%s, %s, %s)
#                     """,
#                     rows_to_insert
#                 )

#         # Commit the changes
#         conn.commit()
#         rows_affected = cursor.rowcount
#         print(f"Number of rows affected: {rows_affected}")
#     else:
#         print("Table is not empty. Skipping data insertion.")
#     cursor.close()
#     conn.close()


def retrieve_data(**context):
    # Create a PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    # Retrieve data from the table
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT ARREST_DATE, LAW_CAT_CD, ARREST_BORO FROM nydp_arrest_data WHERE ARREST_BORO IN ('M', 'B');") #***************  CHANGE THISSSSSSS!!!!!!!!!!!!!!!!!!!!!!!!! ******************* (no limit)
    result = cursor.fetchall()
    # Push the data to XCom
    context['ti'].xcom_push(key='nydp_arrest_data_xcom', value=result)

##################################################################################
################## Utils for processing data #####################################
##################################################################################

def clean_data(data_frame):
    """
    * Drop columns not relevant
    * Manage NaN values
    """
    df = data_frame.copy()
    df = df.dropna(subset=['ARREST_DATE', 'LAW_CAT_CD', 'ARREST_BORO'])
    return df
    
def filter_arrest_data_by_category(data_frame):
    df = data_frame.copy()
    # Transform str to date type
    df['ARREST_DATE'] = pd.to_datetime(df.ARREST_DATE)
    # Rows that do not have the type of crime M, F or V, are eliminated
    df = df.dropna(subset=['LAW_CAT_CD'])
    allowed_values = ['M', 'F', 'V']
    filtered_df = df[df['LAW_CAT_CD'].isin(allowed_values)]
    return filtered_df

def data_bronx_by_type_of_crime(data_frame):
    dataBronx = data_frame[data_frame['ARREST_BORO']=='B']
    grouped = dataBronx.groupby('LAW_CAT_CD')
    dataBronx_F = grouped.get_group('F')
    dataBronx_M = grouped.get_group('M')
    dataBronx_V = grouped.get_group('V')
    # Se hace dataframe con FECHA INICIO SEMANA - CANTIDAD DE <tipo de delito> ordenados por fecha
    dataBronx_M_vis = dataBronx_M.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    dataBronx_F_vis = dataBronx_F.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    dataBronx_V_vis = dataBronx_V.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    return dataBronx_M_vis, dataBronx_F_vis, dataBronx_V_vis

def data_manhatan_by_type_of_crime(data_frame):
    dataManhatan = data_frame[data_frame['ARREST_BORO']=='M']
    grouped = dataManhatan.groupby('LAW_CAT_CD')
    dataManhatan_F = grouped.get_group('F')
    dataManhatan_M = grouped.get_group('M')
    dataManhatan_V = grouped.get_group('V')
    # Se hace dataframe con FECHA INICIO SEMANA - CANTIDAD DE <tipo de delito> ordenados por fecha
    dataManhatan_M_vis = dataManhatan_M.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    dataManhatan_F_vis = dataManhatan_F.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    dataManhatan_V_vis = dataManhatan_V.groupby([pd.Grouper(key='ARREST_DATE', freq='W')]).size().to_frame(name='#')
    return dataManhatan_M_vis, dataManhatan_F_vis, dataManhatan_V_vis

def data_processing(**context):
    # Pull the data from XCom
    data = context['ti'].xcom_pull(key='nydp_arrest_data_xcom')
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data, columns=['ARREST_DATE', 'LAW_CAT_CD', 'ARREST_BORO'])
    # clean the data
    df_clean = clean_data(df)
    # Filter data, allowing only the values M, F and V in the dataframe
    df_filtered = filter_arrest_data_by_category(df_clean)
    # data from bronx
    dataBronx_M_vis, dataBronx_F_vis, dataBronx_V_vis = data_bronx_by_type_of_crime(df_filtered)
    # Convert DataFrame to JSON string
    dataBronx_M_vis_json = dataBronx_M_vis.to_json()
    dataBronx_F_vis_json = dataBronx_F_vis.to_json()
    dataBronx_V_vis_json = dataBronx_V_vis.to_json()
    # data from bronx in xcom
    context['ti'].xcom_push(key='dataBronx_M_vis', value=dataBronx_M_vis_json)
    context['ti'].xcom_push(key='dataBronx_F_vis', value=dataBronx_F_vis_json)
    context['ti'].xcom_push(key='dataBronx_V_vis', value=dataBronx_V_vis_json)
    # data from manhatan
    dataManhatan_M_vis, dataManhatan_F_vis, dataManhatan_V_vis = data_manhatan_by_type_of_crime(df_filtered)
    # Convert DataFrame to JSON string
    dataManhatan_M_vis_json = dataManhatan_M_vis.to_json()
    dataManhatan_F_vis_json = dataManhatan_F_vis.to_json()
    dataManhatan_V_vis_json = dataManhatan_V_vis.to_json()
    context['ti'].xcom_push(key='dataManhatan_M_vis', value=dataManhatan_M_vis_json)
    context['ti'].xcom_push(key='dataManhatan_F_vis', value=dataManhatan_F_vis_json)
    context['ti'].xcom_push(key='dataManhatan_V_vis', value=dataManhatan_V_vis_json)

##################################################################################
################## utils for model training  #####################################
##################################################################################

def rolling_prediction(ts, p, d, q, weeks, **context):
    key_name = '{}_prediction'.format(ts)
    # Pull the data from XCom
    data = context['ti'].xcom_pull(key=ts)
    # Convert the data to a pandas DataFrame
    ts = pd.read_json(data)
    """
    ts = time series
    p = ar_order
    d = diff_order
    q = ma_order
    weeks: number of weeks (in our case) the model can predict
    """
    test_size = weeks
    rolling_predictions = []
    for i in range(test_size):
        train = ts[:-(test_size-i)]
        model = ARIMA(train,order = (p, d, q))
        model_fit = model.fit()
        pred = model_fit.forecast(steps=1)
        rolling_predictions.append(pred.values[0])
    #list to dataframe
    df = pd.DataFrame({'week': range(len(rolling_predictions)), 'prediction': rolling_predictions})
    load = df.to_json()
    context['ti'].xcom_push(key=key_name, value=load)

def forecastARIMA(ts, p, d, q, weeks,  **context):
    key_name = '{}_model'.format(ts)
    key_name_seasonal = "seasonal_"+key_name
    # Pull the data from XCom
    data = context['ti'].xcom_pull(key=ts)
    # Convert the data to a pandas DataFrame
    ts = pd.read_json(data)
    """
    ts = time series
    p = ar_order
    d = diff_order
    q = ma_order
    """
    train = ts[:-weeks]
    model = ARIMA(train,order = (p, d, q))
    model_fit = model.fit()
    seasonal = seasonal_decompose(train,model='additive')
    # model's path
    model_path = key_name+'.pkl'
    seasonal_path = key_name_seasonal + '.pkl'
    #Save the models
    model_fit.save(model_path)
    seasonal.save(seasonal_path)
    # Push model path using xcom_push()
    context['ti'].xcom_push(key=key_name, value=model_path)
    context['ti'].xcom_push(key=key_name_seasonal, value=seasonal_path)


##################################################################################
################## utils for model inference  ####################################
##################################################################################

def predict_models(model, table_name, weeks, **context):
    seasonal_key = "seasonal_" + model
    model_fit = context['ti'].xcom_pull(key=model)
    seasonal_ = context['ti'].xcom_pull(key=seasonal_key)

    seasonal = seasonal_.seasonal[-weeks:]
    pred = model_fit.forecast(steps=weeks)
    
    pred_values = pred.values + seasonal.values
    predictions = pd.DataFrame({'week': range(len(pred_values)), 'prediction': pred_values})
    
    # df to database
    with open('/opt/airflow/data/settings.json', 'r') as file:
        settings = json.load(file)
    engine_str = 'postgresql+psycopg2://%s:%s@%s:%s/%s'%(settings["USER"], settings["PASSWORD"], settings["PORT"], settings["HOST"], settings["DATABASE"])
    engine = sqlalchemy.create_engine(engine_str)
    with open('/opt/airflow/data/settings.json', 'r') as file:
        settings = json.load(file)     
    conn = psycopg2.connect(
        host = settings["HOST"],
        port = settings["PORT"],
        database = settings["DATABASE"],
        user = settings["USER"],
        password = settings["PASSWORD"]
    )
    cursor = conn.cursor()
    predictions.to_sql(table_name, engine, if_exists='replace', index=False)
    rows_affected = cursor.rowcount
    print(f"Number of rows affected: {rows_affected}")
    conn.commit()
    conn.close()

#############################################
#############################################
#####             Dag                  ######
#############################################
#############################################

default_args = {
    'timeout': 300, #5 minutes
    'retries': 5,
}

with DAG(
    dag_id='proc_main_v08',
    start_date = datetime(2023, 1, 1),
    schedule = '@daily', 
    catchup = False,
    dagrun_timeout=timedelta(minutes=10)
    ) as dag:

    task1_create_postgres_tables = PythonOperator(
        task_id = 'create_postgres_tables',
        python_callable = create_tables,
    ) 

    task2_local_csv_to_data_base = PythonOperator(
        task_id = 'local_csv_to_data_base_',
        python_callable=local_csv_to_data_base,
        op_kwargs = {'filename': 'NYPD_Arrests_Data__Historic_.csv'},
        provide_context=True 
    )

    task3_data_extraction = PythonOperator(
        task_id = 'data_extraction_',
        python_callable = retrieve_data,
        provide_context=True 
    )
    
    task4_data_processing = PythonOperator(
        task_id = 'data_processing_',
        python_callable = data_processing,
        provide_context = True
    )

    ######### bronx m ################

    task6_1_train_model_bronx_M = PythonOperator(
        task_id = 'model_train_bronx_M',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataBronx_M_vis', 'p':13, 'd':1, 'q':0, 'weeks': 53}, 
        provide_context = True
    )

    task7_1_predict_bronx_M  = PythonOperator(
        task_id = 'model_predict_bronx_M',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataBronx_M_vis_model','table_name':'predictions_bronx_m', 'weeks': 53},
        provide_context = True
    )

    task6_2_train_model_bronx_F = PythonOperator(
        task_id = 'model_train_bronx_F',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataBronx_F_vis', 'p':13, 'd':1, 'q':0, 'weeks': 53}, 
        provide_context = True
    )

    task7_2_predict_bronx_F  = PythonOperator(
        task_id = 'model_predict_bronx_F',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataBronx_F_vis_model','table_name':'predictions_bronx_f', 'weeks': 53},
        provide_context = True
    )

    task6_3_train_model_bronx_V = PythonOperator(
        task_id = 'model_train_bronx_V',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataBronx_V_vis', 'p':17, 'd':2, 'q':11, 'weeks': 53}, 
        provide_context = True
    )

    task7_3_predict_bronx_V  = PythonOperator(
        task_id = 'model_predict_bronx_V',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataBronx_V_vis_model','table_name':'predictions_bronx_v', 'weeks': 53},
        provide_context = True
    )

    #############################################################################################################################
    ######### manhatan m ################

    task8_1_train_model_manhatan_M = PythonOperator(
        task_id = 'model_train_manhatan_M',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataManhatan_M_vis', 'p':8, 'd':1, 'q':0, 'weeks': 53}, 
        provide_context = True
    )

    task9_1_predict_manhatan_M  = PythonOperator(
        task_id = 'model_predict_manhatan_M',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataManhatan_M_vis_model','table_name':'predictions_manhatan_m', 'weeks': 53},
        provide_context = True
    )

    ######### manhatan f ################
    task8_2_train_model_manhatan_F = PythonOperator(
        task_id = 'model_train_manhatan_F',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataManhatan_F_vis', 'p':13, 'd':1, 'q':0, 'weeks': 53}, 
        provide_context = True
    )

    task9_2_predict_manhatan_F  = PythonOperator(
        task_id = 'model_predict_manhatan_F',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataManhatan_F_vis_model','table_name':'predictions_manhatan_f', 'weeks': 53},
        provide_context = True
    )
    ######### manhatan v ################
    task8_3_train_model_manhatan_V = PythonOperator(
        task_id = 'model_train_manhatan_V',
        python_callable = forecastARIMA,
        op_kwargs = {'ts':'dataManhatan_V_vis', 'p':7, 'd':2, 'q':9, 'weeks': 53}, 
        provide_context = True
    )

    task9_3_predict_manhatan_V  = PythonOperator(
        task_id = 'model_predict_manhatan_V',
        python_callable = predict_models,
        op_kwargs = {'model': 'dataManhatan_V_vis_model','table_name':'predictions_manhatan_v', 'weeks': 53},
        provide_context = True
    )
    #############################################
    #############################################
    #####  Task execution and dependencies ######
    #############################################
    #############################################
    
    task1_create_postgres_tables >>task2_local_csv_to_data_base >> task3_data_extraction >> task4_data_processing 
    
    task4_data_processing >> task6_1_train_model_bronx_M >> task7_1_predict_bronx_M
    task4_data_processing >> task6_2_train_model_bronx_F >> task7_2_predict_bronx_F
    task4_data_processing >> task6_3_train_model_bronx_V >> task7_3_predict_bronx_V

    task4_data_processing >> task8_1_train_model_manhatan_M >> task9_1_predict_manhatan_M
    task4_data_processing >> task8_2_train_model_manhatan_F >> task9_2_predict_manhatan_F
    task4_data_processing >> task8_3_train_model_manhatan_V >> task9_3_predict_manhatan_V
    