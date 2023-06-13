import requests
from bs4 import BeautifulSoup
import pika
import logging

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
    link = url + title_element["href"]
    links.append(link)

for link in links:
    # Отправка ссылок в очередь tasks
    channel.basic_publish(exchange='',
                          routing_key='tasks',
                          body=link)
    logger.info("Ссылка отправлена в очередь: %s", link)

connection.close()