from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_IN = 'topic.user_in'
TOPIC_OUT = 'topic.user_out'

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ','.join([
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
    'org.postgresql:postgresql:42.4.0'
])


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, batch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    dfc = df.persist()

    # записываем df в PostgreSQL с полем feedback
    dfc.withColumn('feedback', lit(None).cast(StringType())) \
        .write \
        .format('jdbc') \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option('user', 'user') \
        .option('password', 'password') \
        .option('schema', 'public') \
        .option('dbtable', 'subscribers_feedback') \
        .mode('append') \
        .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = dfc.withColumn('value',
                              to_json(struct(
                                  col('restaurant_id'),
                                  col('adv_campaign_id'),
                                  col('adv_campaign_content'),
                                  col('adv_campaign_owner'),
                                  col('adv_campaign_owner_contact'),
                                  col('adv_campaign_datetime_start'),
                                  col('adv_campaign_datetime_end'),
                                  col('client_id'),
                                  col('datetime_created'),
                                  col('trigger_datetime_created'))))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write \
        .format('kafka') \
        .mode('append') \
        .option('kafka.bootstrap.servers', 'rc1b-2e.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                'org.apache.kafka.common.security.scram.ScramLoginModule '
                'required username=\"de-user\" password=\"password\";') \
        .option('topic', TOPIC_OUT) \
        .save()

    # очищаем память от df
    dfc.unpersist()


# создаём spark сессию с необходимыми библиотеками для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName('RestaurantSubscribeStreamingService') \
    .master('local') \
    .config('spark.sql.session.timeZone', 'UTC') \
    .config('spark.jars.packages', spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2e.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.sasl.jaas.config',
            'org.apache.kafka.common.security.scram.ScramLoginModule '
            'required username=\"de-user\" password=\"password\";') \
    .option('subscribe', TOPIC_IN) \
    .load()

# определяем схему входного сообщения для json
incoming_message_schema = StructType([
    StructField('restaurant_id', StringType()),
    StructField('adv_campaign_id', StringType()),
    StructField('adv_campaign_content', StringType()),
    StructField('adv_campaign_owner', StringType()),
    StructField('adv_campaign_owner_contact', StringType()),
    StructField('adv_campaign_datetime_start', LongType()),
    StructField('adv_campaign_datetime_end', LongType()),
    StructField('datetime_created', LongType()),
])

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df
    .withColumn('value', col('value').cast(StringType()))
    .withColumn('data', from_json(col('value'), incoming_message_schema))
    .select('data.*')
    .drop('event')
    .where(
        (col('adv_campaign_datetime_start') <= current_timestamp_utc)
        & (col('adv_campaign_datetime_end') >= current_timestamp_utc)
    )
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (
    spark
    .read
    .format('jdbc')
    .option('url', 'jdbc:postgresql://rc1a-fs.mdb.yandexcloud.net:6432/de')
    .option('driver', 'org.postgresql.Driver')
    .option('dbtable', 'subscribers_restaurants')
    .option('user', 'user')
    .option('password', 'password')
    .load()
    .select('client_id', 'restaurant_id')
    .distinct()
)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
# Добавляем время создания события.
result_df = (
    filtered_read_stream_df
    .join(subscribers_restaurant_df, 'restaurant_id', 'inner')
    .withColumn('trigger_datetime_created', lit(unix_timestamp()))
)

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
