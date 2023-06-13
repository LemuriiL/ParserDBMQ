import pika
from bs4 import BeautifulSoup
import requests
import textwrap
import logging

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очередей
channel.queue_declare(queue='tasks')
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


def parse_html(url):
    # Отправление GET-запроса
    response = requests.get(url)

    # Проверка успешности запроса (это не вся обработка сетевых ошибок)
    if response.status_code == 200:
        # Парсинг HTML-кода страницы
        soup = BeautifulSoup(response.content, 'html.parser')

        # Извлечение текста из элемента <article> и удаление лишних пробелов
        article = soup.find('article')
        text = article.get_text(strip=True)

        # Форматирование текста
        formatted_text = textwrap.fill(text, width=80)
        return formatted_text

    # Обработка сетевых ошибок (а вот это вся обработка сетевых ошибок)
    logger.error('Ошибка при получении страницы: %s', response.status_code)
    return None


# Обработка сообщений из очереди tasks
def callback(ch, method, properties, body):
    link = body.decode()
    logger.info("Получена ссылка из очереди tasks: %s", link)

    # Парсинг полного текста новости
    parsed_text = parse_html(link)

    if parsed_text:
        # Отправление результатов в очередь results
        channel.basic_publish(exchange='',
                              routing_key='results',
                              body=parsed_text)
        logger.info("Результаты отправлены в очередь results")
    else:
        logger.warning("Не удалось распарсить страницу: %s", link)


channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)

# Запуск чтения сообщений из очереди tasks
logger.info('Ожидание сообщений из очереди tasks...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()
