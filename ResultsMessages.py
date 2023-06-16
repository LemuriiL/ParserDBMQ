import pika
import logging
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очередей
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

# Настройки подключения к Elasticsearch
es = Elasticsearch(
    [{"host": "127.0.0.1", "port": 9200, "scheme": "http"}],
    basic_auth=("elastic", "nuMZ7JRTtJpQYSORhI=n")
)

# Настройки логирования
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(console_handler)


def parse_article(url):
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        article_element = soup.find('div', class_='single__content')
        full_text = article_element.get_text(strip=True)

        try:
            date_element = soup.find('time', class_='localtime meta__date').get('content')
        except AttributeError:
            date_element = soup.find('time', class_='timeago meta__date').get('content')

        author_element = soup.find('div', class_="user-miniature__username")
        title_element = soup.find('h1', class_='single__title')

        author = author_element.text.strip() if author_element else ''
        title = title_element.text.strip() if title_element else ''

        # Поиск документа по URL
        search_result = es.search(index='parsernews', body={"query": {"match": {"url": url}}})

        if search_result['hits']['total']['value'] > 0:
            # Документ найден, обновление его содержимого
            document_id = search_result['hits']['hits'][0]['_id']
            update_body = {
                'doc': {
                    'text': full_text,
                    'date': date_element,
                    'author': author,
                    'title': title
                }
            }
            es.update(index='parsernews', id=document_id, body=update_body)
            logger.info("Данные обновлены в Elasticsearch: %s", update_body)
        else:
            # Документ не найден, создание нового документа
            index_settings = {
                'url': url,
                'text': full_text,
                'date': date_element,
                'author': author,
                'title': title
            }
            es.index(index='parsernews', body=index_settings)
            logger.info("Данные сохранены в Elasticsearch: %s", index_settings)
    else:
        logger.warning("Не удалось распарсить страницу: %s", url)


# Обработка сообщений из очереди tasks
def callback(ch, method, properties, body):
    link = body.decode()
    logger.info("Получена ссылка из очереди tasks: %s", link)

    # Парсинг статьи и обновление данных в Elasticsearch
    parse_article(link)


channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)

# Запуск чтения сообщений из очереди tasks
logger.info('Ожидание сообщений из очереди tasks...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()