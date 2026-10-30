import asyncio
from queue import Queue
import time

from analyzer import analyze_posts
from fetcher import fetch_all_posts
from saver import saver


def print_pipeline_report(requested, loaded, processed, saved, start_time):
    report = (
        f"\n=== Отчёт о работе пайплайна ===\n"
        f"Запрошено постов: {requested}\n"
        f"Успешно загружено: {loaded}\n"
        f"Успешно обработано (CPU): {processed}\n"
        f"Успешно сохранено (I/O): {saved}\n"
        f"Общее время: {time.perf_counter() - start_time:.2f} сек."
    )
    print(report)


def run_pipeline():
    start_time = time.perf_counter()
    main_queue = Queue()
    requested = 100

    posts = asyncio.run(fetch_all_posts(requested))

    clean_posts = [post for post in posts if post is not None]
    loaded = len(clean_posts)

    analyzed_posts = analyze_posts(clean_posts)
    processed = len(analyzed_posts)

    for post in analyzed_posts:
        main_queue.put(post)

    saved = saver(main_queue, len(clean_posts))

    print_pipeline_report(requested, loaded, processed, saved, start_time)

    print()


if __name__ == "__main__":
    run_pipeline()
