import requests
from bs4 import BeautifulSoup
import pika

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очередей
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

url = "https://tproger.ru/"

try:
    response = requests.get(url)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print("Произошла сетевая ошибка:", str(e))
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
    print("Ссылка отправлена в очередь:", link)


connection.close()
