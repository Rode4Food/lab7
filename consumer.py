import os
import aio_pika
import asyncio
import aiohttp
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

# Настройка логирования для вывода в консоль
logger = logging.getLogger('consumer')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(console_handler)

# Список для хранения обработанных ссылок
processed_urls = set()

# Функция для получения всех внутренних ссылок на странице
async def get_internal_links(url, base_url):
    async with aiohttp.ClientSession() as session:
        try:
            # Добавляем схему, если отсутствует
            if not urlparse(url).scheme:
                url = f"http:/{url}"

            async with session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Логируем название страницы
                page_title = soup.title.string if soup.title else "Без названия"
                logger.info(f"Обрабатывается страница: {page_title} ({url})")

                links = []
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']

                    # Пропускаем некорректные или пустые ссылки
                    if not href or href.startswith('#'):
                        continue

                    # Формируем абсолютную ссылку
                    if not urlparse(href).scheme:
                        href = urljoin(url, href)

                    # Фильтруем внутренние ссылки
                    if urlparse(href).netloc == base_url:
                        if href not in processed_urls:  # Проверяем, была ли ссылка уже обработана
                            link_info = {
                                "text": a_tag.get_text(strip=True),  # Текст внутри тега <a>
                                "url": href  # Абсолютная ссылка
                            }
                            links.append(link_info)

                            # Логируем найденную ссылку
                            logger.info(f"Найдена ссылка: {link_info['text'] or 'Без названия'} ({link_info['url']})")
                            processed_urls.add(href)  # Отмечаем ссылку как обработанную

                return links
        except Exception as e:
            logger.error(f"Ошибка при обработке {url}: {e}")
            return []

async def process_message(message: aio_pika.IncomingMessage, exchange):
    async with message.process():
        url = message.body.decode()
        base_url = urlparse(url).netloc

        # Получаем ссылки на странице
        links = await get_internal_links(url, base_url)

        # Добавляем найденные ссылки обратно в очередь
        for link in links:
            try:
                await exchange.publish(
                    aio_pika.Message(body=link['url'].encode()),
                    routing_key="urls"
                )
                logger.info(f"Добавлена в очередь ссылка: {link['url']}")
            except Exception as e:
                logger.error(f"Ошибка при добавлении ссылки {link['url']}: {e}")

# Асинхронная функция для подключения и обработки очереди RabbitMQ
async def consume():
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')

    connection = await aio_pika.connect_robust(rabbitmq_url)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        # Декларируем обмен и очередь
        exchange = await channel.declare_exchange("urls_exchange", aio_pika.ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue("urls", durable=True)
        await queue.bind(exchange, routing_key="urls")

        # Событие для завершения работы
        stop_event = asyncio.Event()

        # Таймер на 60 секунд без сообщений
        async def check_empty_queue():
            while True:
                await asyncio.sleep(30)
                if not stop_event.is_set():
                    logger.info("Очередь пуста в течение 30 секунд. Завершаем работу.")
                    await connection.close()  # Закрываем соединение и завершаем работу
                    break
                stop_event.clear()  # Сбрасываем событие, если пришло новое сообщение

        # Запускаем проверку таймера в фоне
        asyncio.create_task(check_empty_queue())

        # Начинаем потребление сообщений
        logger.info("Ожидание сообщений...")

        while True:
            try:
                # Ожидаем новое сообщение
                message = await queue.get()

                if message:
                    await process_message(message, exchange)
                    stop_event.set()  # Сбросим таймер, если пришло сообщение

            except Exception as e:
                pass

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume())
