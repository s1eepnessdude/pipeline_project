import asyncio

import aiohttp


def log(message: str) -> list | None:
    if not hasattr(log, "log_tasks"):
        log.log_tasks = []

    if message is not None:
        log_task = asyncio.create_task(logging(message))
        log.log_tasks.append(log_task)

    if message is None:
        result = log.log_tasks.copy()
        log.log_tasks = []
        return result


async def logging(message: str) -> None:
    print(message)
    # try:
    #     async with aiofiles.open("fetcher.log", "a", encoding="utf-8") as file:
    #         await file.write(message+"\n")
    # except:
    #     print("[Logging] Ошибка записи в лог", message)


async def fetch_post(
    post_id: int,
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
) -> dict | None:
    get_timeout = aiohttp.ClientTimeout(total=2.0, connect=1.0)

    if not isinstance(post_id, int) or post_id <= 0:
        return None

    url = f"https://jsonplaceholder.typicode.com/posts/{post_id}"

    async with semaphore:
        try:
            log(f"[Fetch post]: Начало загрузки поста {post_id}")

            async with session.get(url, timeout=get_timeout) as response:
                response.raise_for_status()
                data = await response.json()

        except Exception as e:
            log(f"[Fetch post]: {repr(e)} {post_id}")
            return None
        else:
            return data


async def fetch_all_posts(requested: int) -> list:
    semaphore = asyncio.Semaphore(5)
    tasks = []

    async with aiohttp.ClientSession() as session:
        log("[Fetch_all_posts]: Начинаю скачивание постов")

        for i in range(1, requested + 1):
            post_id = i
            task = asyncio.create_task(fetch_post(post_id, semaphore, session))
            tasks.append(task)

        posts = await asyncio.gather(*tasks)

        log_tasks = log(None)
        await asyncio.gather(*log_tasks)

        return posts
