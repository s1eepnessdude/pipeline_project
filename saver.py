from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty
from threading import Event, Lock, Thread
import time

COLUMN_WIDTHS = (3, 10, 10, 64)
HEADERS = ("id", "title", "complexity", "text_hash")

shared_lock = None
shared_queue = None
shared_counter = None


def format_row(values):
    cells = [
        str(value)[:width].ljust(width)
        for value, width in zip(values, COLUMN_WIDTHS, strict=False)
    ]
    return "| " + " | ".join(cells) + " |"


def init_worker(lock: Lock, queue, counter: int) -> None:
    global shared_lock, shared_queue, shared_counter
    shared_lock = lock
    shared_queue = queue
    shared_counter = counter


def save_post(post_data: dict) -> None:
    time.sleep(0.05)

    with shared_lock:
        with open("results.log", "a", encoding="utf-8") as file:
            file.write(format_row(post_data) + "\n")

        shared_counter[0] += 1


def worker() -> None:
    while True:
        try:
            post = shared_queue.get_nowait()
        except Empty:
            break

        post_data = [
            post["id"],
            post["title"],
            post["complexity_score"],
            post["text_hash"],
        ]

        save_post(post_data)


def progress(counter: int, total: int, lock: Lock, event: Event) -> None:
    while not event.is_set():
        with lock:
            saved = counter[0]

        print(f"[Progress] Прогресс сохранения: {saved}/{total}")
        time.sleep(2)

    with lock:
        saved = counter[0]

    print(f"[Progress] Прогресс сохранения: {saved}/{total}")
    print("[Progress] Сохранение завершено")


def saver(main_queue, len_posts: int) -> int:
    lock = Lock()
    counter = [0]
    event = Event()

    border = "+-" + "-+-".join("-" * width for width in COLUMN_WIDTHS) + "-+"

    with open("results.log", "w", encoding="utf-8") as file:
        file.write(border + "\n")
        file.write(format_row(HEADERS) + "\n")
        file.write(border + "\n")

    progress_thread = Thread(
        target=progress,
        args=(counter, len_posts, lock, event),
    )
    progress_thread.start()

    try:
        with ThreadPoolExecutor(
            max_workers=5,
            initializer=init_worker,
            initargs=(lock, main_queue, counter),
        ) as executor:
            futures = [executor.submit(worker) for _ in range(5)]

            for future in as_completed(futures):
                future.result()

    finally:
        event.set()
        progress_thread.join()

    with lock:
        saved = counter[0]

    print(f"Программа завершила выполнение. Сохранено: {saved}/{len_posts}")

    return saved
