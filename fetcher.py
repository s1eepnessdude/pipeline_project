import asyncio
from contextlib import contextmanager
import logging
import logging.handlers
import queue
import sys

import aiohttp


async def fetch_post(session, post_id, semaphore):
    logger = logging.getLogger(f"Worker-fetch_post: {post_id}")

    get_timeout = aiohttp.ClientTimeout(total=5.0, connect=2.0)

    if not isinstance(post_id, int) or post_id <= 0:
        logger.error(f"Ошибка значения id: {post_id}")
        return None

    url = f"https://jsonplaceholder.typicode.com/posts/{post_id}"

    async with semaphore:
        try:
            logger.info(f"Выполняю get-запрос по {url}")
            async with session.get(url, timeout=get_timeout) as response:
                response.raise_for_status()

                data = await response.json()

                logger.info(f"Успешно скачал пост {post_id}")
                return data

        except aiohttp.ClientConnectorError:
            logger.error("Ошибка соединения: невозможно подключиться к серверу.")
            return None
        except TimeoutError:
            logger.error("Ошибка тайм-аута: сервер отвечал слишком долго.")
            return None
        except aiohttp.ClientResponseError as e:
            logger.error(f"Ошибка HTTP: сервер вернул статус {e.status}")
            return None
        except aiohttp.ContentTypeError:
            logger.error("Ошибка контента: сервер вернул плохой JSON или текст вместо JSON.")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Общая ошибка aiohttp: {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return None


async def fetch_all_posts():
    logger = logging.getLogger("Worker-fetch_all_posts")

    semaphore = asyncio.Semaphore(5)
    tasks = []

    async with aiohttp.ClientSession() as session:
        logger.info("Начинаю скачивание 100 постов")
        for i in range(1, 101):
            post_id = i
            task = asyncio.create_task(fetch_post(session, post_id, semaphore))
            tasks.append(task)

        posts = await asyncio.gather(*tasks)
        logger.info("Закончил скачивание 100 постов")

        clean_posts = [post for post in posts if post is not None]
        done_percent = round(len(clean_posts) / len(posts) * 100)
        logger.info(f"Cкачано {len(clean_posts)} постов, успешность {done_percent}%")

        return clean_posts


@contextmanager
def setup_logging():
    log_queue = queue.Queue(-1)

    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler("results.log", encoding="utf-8")

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    listener = logging.handlers.QueueListener(log_queue, stream_handler, file_handler)

    queue_handler = logging.handlers.QueueHandler(log_queue)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)

    listener.start()

    try:
        yield
    finally:
        listener.stop()
        root_logger.removeHandler(queue_handler)
        queue_handler.close()


async def main():
    with setup_logging():
        logging.info("Приложение запущено")

        result = await fetch_all_posts()

        return result


if __name__ == "__main__":
    asyncio.run(main())
