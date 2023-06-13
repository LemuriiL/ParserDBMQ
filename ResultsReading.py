import pika

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очереди
channel.queue_declare(queue='results')

# Обработчик сообщений из очереди results
def callback(ch, method, properties, body):
    result = body.decode()
    print("Получен результат из очереди results:", result)


channel.basic_consume(queue='results', on_message_callback=callback, auto_ack=True)

# Запуск чтения сообщений из очереди
print('Ожидание результатов из очереди results...')
channel.start_consuming()


connection.close()