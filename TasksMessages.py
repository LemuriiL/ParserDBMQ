from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pika
import logging
from elasticsearch import Elasticsearch
import hashlib

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очередей
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

url = "https://tproger.ru/"

# Настройки логирования
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(console_handler)

# Подключение к Elasticsearch
es = Elasticsearch(
    [{"host": "127.0.0.1", "port": 9200, "scheme": "http"}],
    basic_auth=("elastic", "nuMZ7JRTtJpQYSORhI=n")
)

try:
    response = requests.get(url)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logger.error("Произошла сетевая ошибка: %s", str(e))
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

articles = soup.find_all("article", class_="article")
links = []

for article in articles:
    title_element = article.find("h2", class_="article__title--icon").a
    link = title_element["href"]
    links.append(link)

for link in links:
    # Вычисление хеша ссылки
    link_hash = hashlib.md5(link.encode()).hexdigest()

    # Проверка наличия хеша ссылки в Elasticsearch
    search_result = es.search(index='parsernews', query={"match": {"hash": link_hash}})


    if search_result['hits']['total']['value'] > 0:
        logger.info("Информация по статье уже существует в базе данных. Ссылка: %s", link)
    else:
        # Добавление нового документа с хешем ссылки в Elasticsearch
        index_settings = {
            "hash": link_hash,
            "timestamp": str(datetime.now())
        }
        es.index(index='parsernews', document=index_settings)

        # Отправка хеша ссылки в очередь tasks
        channel.basic_publish(exchange='',
                              routing_key='tasks',
                              body=link)
        logger.info("Хеш ссылки добавлен в базу данных и отправлен в очередь: %s", link_hash)

connection.close()