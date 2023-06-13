import pika
from bs4 import BeautifulSoup
import requests
import textwrap

# Настройки подключения к RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Создание очередей
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')


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
    print('Ошибка при получении страницы:', response.status_code)
    return None


# Обработка сообщений из очереди tasks
def callback(ch, method, properties, body):
    link = body.decode()
    print("Получена ссылка из очереди tasks:", link)

    # Парсинг полного текста новости
    parsed_text = parse_html(link)

    if parsed_text:
        # Отправление результатов в очередь results
        channel.basic_publish(exchange='',
                              routing_key='results',
                              body=parsed_text)
        print("Результаты отправлены в очередь results")

channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)

# Запуск чтения сообщений из очереди tasks
print('Ожидание сообщений из очереди tasks...')
channel.start_consuming()


connection.close()