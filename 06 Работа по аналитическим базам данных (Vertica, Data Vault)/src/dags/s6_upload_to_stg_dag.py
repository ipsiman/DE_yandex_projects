import logging
import pendulum
import vertica_python

from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

VERTICA_USER = Variable.get('VERTICA_USER')
VERTICA_PASSWORD = Variable.get('VERTICA_PASSWORD')

conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': VERTICA_USER,
             'password': VERTICA_PASSWORD,
             'database': 'dwh',
             'autocommit': True
             }


@dag(
    dag_id='s6_upload_to_stg_dag',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule_interval=None,
    tags=['sprint6', 'upload', 'stg']
)
def s6_upload_to_stg_dag():

    @task(task_id='upload_users')
    def upload_users():
        sql_exec = (
            '''
            -- TRUNCATE TABLE USERYANDEXRU__STAGING.users;
            COPY USERYANDEXRU__STAGING.users
            (id, chat_name, registration_dt, country, age)
            FROM LOCAL '/data/users.csv'
            DELIMITER ',';           
            '''
        )
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_exec)

    @task(task_id='upload_groups')
    def upload_groups():
        sql_exec = (
            '''
            -- TRUNCATE TABLE USERYANDEXRU__STAGING.groups;
            COPY USERYANDEXRU__STAGING.groups
            (id, admin_id, group_name, registration_dt, is_private)
            FROM LOCAL '/data/groups.csv'
            DELIMITER ',';           
            '''
        )
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_exec)

    @task(task_id='upload_dialogs')
    def upload_dialogs():
        sql_exec = (
            '''
            -- TRUNCATE TABLE USERYANDEXRU__STAGING.dialogs;
            COPY USERYANDEXRU__STAGING.dialogs
            (message_id, message_ts, message_from, message_to, message, message_group)
            FROM LOCAL '/data/dialogs.csv'
            DELIMITER ',';           
            '''
        )
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_exec)

    @task(task_id='upload_group_log')
    def upload_group_log():
        sql_exec = (
            '''
            -- TRUNCATE TABLE USERYANDEXRU__STAGING.group_log;
            COPY USERYANDEXRU__STAGING.group_log
            (group_id, user_id, user_id_from, event, date_ts)
            FROM LOCAL '/data/group_log.csv'
            DELIMITER ',';           
            '''
        )
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_exec)

    upload_users = upload_users()
    upload_groups = upload_groups()
    upload_dialogs = upload_dialogs()
    upload_group_log = upload_group_log()

    upload_users >> upload_groups >> upload_dialogs >> upload_group_log


_ = s6_upload_to_stg_dag()
