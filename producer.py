import sys
import os
import pika
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import logging

# Настройка логирования для вывода в консоль
logger = logging.getLogger('producer')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(console_handler)

# Функция для получения всех внутренних ссылок на странице
async def get_internal_links(url, base_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Если ссылка абсолютная, добавляем ее как есть
                if href.startswith('http') or href.startswith('https'):
                    if base_url in href:
                        links.append(href)
                # Если ссылка относительная, делаем ее абсолютной
                else:
                    full_url = urljoin(url, href)
                    links.append(full_url)
            return links

# Функция для подключения к RabbitMQ и отправки ссылок
def send_to_queue(links, queue_name, rabbitmq_url):
    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = connection.channel()

    # Декларация очереди с параметром durable=True
    # Это гарантирует, что очередь будет существовать после перезапуска RabbitMQ
    channel.queue_declare(queue=queue_name, durable=True)

    # Отправка сообщений в очередь
    for link in links:
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=link,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Сделать сообщение стойким (persistent)
            )
        )
        logger.info(f"Sent: {link}")

    # Закрытие соединения
    connection.close()

async def main():
    if len(sys.argv) < 2:
        logger.error("Usage: producer.py <URL>")
        return

    url = sys.argv[1]
    base_url = urlparse(url).netloc
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')

    logger.info(f"Processing {url}")

    # Получаем все внутренние ссылки
    links = await get_internal_links(url, base_url)

    logger.info(f"Found {len(links)} internal links")

    # Отправляем ссылки в очередь RabbitMQ
    send_to_queue(links, 'urls', rabbitmq_url)

if __name__ == "__main__":
    asyncio.run(main())
