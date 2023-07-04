from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import psycopg2
#import statsmodels
#from statsmodels.tsa.arima.model import ARIMA

## From csv to data table
def local_csv_to_data_base(filename, **context):
    file_path = f'/opt/airflow/data/{filename}' 
    conn = psycopg2.connect(
        host='postgres',
        port= 5432,
        database='airflow',
        user='airflow',
        password='airflow'
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


# def retrieve_data(**context):
#     # Create a PostgresHook
#     pg_hook = PostgresHook(postgres_conn_id='postgres')

#     # Retrieve data from the table
#     with pg_hook.get_conn() as connection:
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT ARREST_KEY,ARREST_DATE,PD_CD,PD_DESC,KY_CD,OFNS_DESC,LAW_CODE,LAW_CAT_CD,ARREST_BORO,ARREST_PRECINCT,JURISDICTION_CODE,AGE_GROUP,PERP_SEX,PERP_RACE,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lon_Lat FROM nydp_arrest_data")
#             while True:
#                 batch = cursor.fetchmany(1000)
#                 if not batch:
#                     break
#                 context['ti'].xcom_push(key='nydp_arrest_data_xcom', value=batch)

def retrieve_data(**context):
    # Create a PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    # Retrieve data from the table
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM nydp_arrest_data")
    result = cursor.fetchall()
    # Push the data to XCom
    context['ti'].xcom_push(key='nydp_arrest_data_xcom', value=result)

##################################################################################
################## utils for processing data #####################################
##################################################################################
def clean_data(data_frame):
    """
    * Drop columns not relevant
    * Manage NaN values
    """
    df = data_frame.copy()
    # Drop columns not relevant
    columns_to_drop = ['ARREST_KEY', 'PD_CD', 'PD_DESC', 'KY_CD', 'OFNS_DESC', 'LAW_CODE']
    df = df.drop(columns=columns_to_drop)
    # For columns containing null values, the following procedure will apply:
    # if the number of null values ​​for an attribute is less than 25% of the total values, 
    # it will be removed. Instead, if the number of null values ​​is greater than or equal 
    # to 25%, the average value of the column will be calculated and the null values ​
    # ​replaced with the average
    total_rows = df.shape[0]
    na_threshold = total_rows * 0.25
    for column in df.columns:
        if df[column].isna().sum() < na_threshold:
            df = df.dropna(subset=[column])
        else:
            df[column] = df[column].fillna(df[column].mean())
    return df
    
def filter_arrest_data_by_category(data_frame):
    df = data_frame.copy()
    # Transform str to date type
    df = df['ARREST_DATE'] = pd.to_datetime(df.ARREST_DATE)
    # Rows that do not have the type of crime M, F or V, are eliminated
    df = df.dropna(subset=['LAW_CAT_CD'])
    allowed_values = ['M', 'F', 'V']
    filtered_df = df[df['LAW_CAT_CD'].isin(allowed_values)]
    return filtered_df

def data_bronx_by_type_of_crime(data_frame):
    df = data_frame.copy()
    dataBronx = df[data_frame['ARREST_BORO']=='B']
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
    df = data_frame.copy()
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
    df = pd.DataFrame(data, columns=['ARREST_KEY','ARREST_DATE','PD_CD','PD_DESC','KY_CD',
    'OFNS_DESC','LAW_CODE','LAW_CAT_CD','ARREST_BORO','ARREST_PRECINCT','JURISDICTION_CODE',
    'AGE_GROUP','PERP_SEX','PERP_RACE','X_COORD_CD','Y_COORD_CD','Latitude','Longitude','Lon_Lat'])
    # clean the data
    df_clean = clean_data(df)
    # Filter data, allowing only the values M, F and V in the dataframe
    df_filtered = filter_arrest_data_by_category(df_clean)
    # data from bronx
    dataBronx_M_vis, dataBronx_F_vis, dataBronx_V_vis = data_bronx_by_type_of_crime(df_filtered)
    # data from bronx in xcom
    context['ti'].xcom_push(key='dataBronx_M_vis', value=dataBronx_M_vis)
    context['ti'].xcom_push(key='dataBronx_F_vis', value=dataBronx_F_vis)
    context['ti'].xcom_push(key='dataBronx_V_vis', value=dataBronx_V_vis)
    # data from manhatan
    dataManhatan_M_vis, dataManhatan_F_vis, dataManhatan_V_vis = data_manhatan_by_type_of_crime(df_filtered)
    # data from bronx in xcom
    context['ti'].xcom_push(key='dataManhatan_M_vis', value=dataManhatan_M_vis)
    context['ti'].xcom_push(key='dataManhatan_F_vis', value=dataManhatan_F_vis)
    context['ti'].xcom_push(key='dataManhatan_V_vis', value=dataManhatan_V_vis)

##################################################################################
################## utils for model training  #####################################
##################################################################################
# def generate_model_for_dataBronx_M_vis(ts_bronx):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_bronx, order = (p, d, q))
#     model = arima.fit()
#     return model

# def generate_model_for_dataBronx_F_vis(ts_bronx):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_bronx, order = (p, d, q))
#     model = arima.fit()
#     return model

# def generate_model_for_dataBronx_V_vis(ts_bronx):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_bronx, order = (p, d, q))
#     model = arima.fit()
#     return model

# def generate_model_for_dataManhatan_M_vis(ts_manhatan):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_manhatan, order = (p, d, q))
#     model = arima.fit()
#     return model

# def generate_model_for_dataManhatan_F_vis(ts_manhatan):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_manhatan, order = (p, d, q))
#     model = arima.fit()
#     return model

# def generate_model_for_dataManhatan_V_vis(ts_manhatan ):
#     """
#     Model: ARI
#     Parameters:
#         p = 1
#         d = 1
#         q = 0
#     """
#     p=1
#     d=1
#     q=0
#     arima = ARIMA(ts_manhatan, order = (p, d, q))
#     model = arima.fit()
#     return model

def model_training(**context):
    # Pull the data from XCom
    dataBronx_M = context['ti'].xcom_pull(key='dataBronx_M_vis')
    print(dataBronx_M)
    return

#############################################
#############################################
#####             Dags                 ######
#############################################
#############################################

default_args = {
    'timeout': 300 #5 minutes
}

with DAG(
    dag_id='proc_test_v05',
    start_date = datetime(2023, 1, 1),
    schedule = '@daily', 
    catchup = False
    ) as dag:

    task1_create_postgres_table = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres',
        sql = """
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
        """
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

    task5_model_training = PythonOperator(
        task_id = 'model_training_',
        python_callable = model_training,
        provide_context = True
    ) 

    #############################################
    #############################################
    #####  Task execution and dependencies ######
    #############################################
    #############################################
    
    task1_create_postgres_table>> task2_local_csv_to_data_base>> task3_data_extraction >> task4_data_processing >> task5_model_training