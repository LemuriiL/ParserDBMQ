import pika
import logging

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очереди
channel.queue_declare(queue='results')

# Настройки логирования
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(console_handler)

# Обработчик сообщений из очереди results
def callback(ch, method, properties, body):
    result = body.decode()
    logger.info("Получен результат из очереди results: %s", result)

channel.basic_consume(queue='results', on_message_callback=callback, auto_ack=True)

# Запуск чтения сообщений из очереди
logger.info('Ожидание результатов из очереди results...')
channel.start_consuming()

connection.close()