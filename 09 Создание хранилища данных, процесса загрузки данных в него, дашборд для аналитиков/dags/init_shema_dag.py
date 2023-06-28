import logging
import pendulum

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

log = logging.getLogger(__name__)
postgres_conn_id = 'postgresql_de'


with DAG(
    dag_id='DE_team2_init_schema',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule=None,
    tags=['init', 'sql'],
) as dag:
    init_schema = PostgresOperator(
        task_id='init_schema',
        postgres_conn_id=postgres_conn_id,
        sql='''
            CREATE SCHEMA IF NOT EXISTS dds;

            DROP TABLE IF EXISTS dds.fct_events;
            
            CREATE TABLE dds.fct_events (
                id serial NOT NULL,
                event_id varchar NOT NULL,
                user_custom_id varchar NOT NULL,
                event_timestamp timestamp NOT NULL,
                page_1 varchar NOT NULL,
                page_2 varchar,
                page_3 varchar,
                page_4 varchar,
                PRIMARY KEY (id)
            );
            
            CREATE SCHEMA IF NOT EXISTS cdm;
            
            DROP TABLE IF EXISTS cdm.events_by_dt;
            
            CREATE TABLE cdm.events_by_dt (
                id serial not null,
                dt timestamp not null,
                events_count int not null default 0,
                PRIMARY KEY (id),
                UNIQUE (dt)
            );
            
            DROP TABLE IF EXISTS cdm.sales_by_dt;
            
            CREATE TABLE cdm.sales_by_dt (
                id serial not null,
                dt timestamp not null,
                sales_count int not null default 0,
                primary key (id),
                UNIQUE (dt)
            );
            
            drop table if exists cdm.top_urls;
            
            create table cdm.top_urls (
                id serial not null,
                dt timestamp not null,
                page_path varchar not null,
                sales int not null default 0,
                PRIMARY KEY (id),
                UNIQUE (dt, page_path)
            );
            ''',
    )

    init_schema
