# lab7 RabbitMQ 

# Описание проекта
Этот проект включает два скрипта: `producer.py` и `consumer.py`, которые взаимодействуют с RabbitMQ для обработки внутренних ссылок на веб-страницах.
Скрипт `producer.py`  на входе из аргументов командной строки принимает любой HTTP(S) URL, находит все внутренние ссылки (только дляэтого домена) в HTML коде, помещает их в очередь RabbitMQ по одной.
Скрипт `consumer.py` "вечный" асинхронный, который читает из очереди эти ссылки, также находит внутренние ссылки и помещает в их очередь. 
В скрипте `consumer.py` Предусмотрен таймаут в 30 секунд: если очередь пуста в течение этого таймаута скрипт завершает свою работу.
Для ручной остановки работы `consumer.py` можно использовать сочетание клавиш ctrl+c.
 
## Установка
1. Скачайте и установите Docker Desktop
2. Проверьте установку Docker:
   ```cmd
   docker --version
3. Скачайте `producer.py` и `consumer.py` или скопируйте репозиторий в удобное место
4. Откройте папку, где находятся файлы  `producer.py` и `consumer.py`
5. Установите все необходимые библиотеки для запуска, сделать это можно командой
   ```cmd
   pip install aiohttp aio-pika beautifulsoup4 pika

## Запуск программы
1. Запустите Docker Desktop, если этого не было сделано ранее
2.Запустите RabbitMQ в Docker, сделать это можно командой:
  ```cmd
  docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
  ```
3. Откройте папку, где находятся файлы  `producer.py` и `consumer.py`
4. Откройте консоль cmd/PowerShell
5. Запустите `producer.py` с помощью команды представленной ниже, заменив <YOUR_LINK> на ссылку которую вы хотите обработать:
   ```cmd
   python producer.py <YOUR_LINK>
6. Запустите `consumer.py` с помощью команды:
    ```cmd
   python consumer.py
