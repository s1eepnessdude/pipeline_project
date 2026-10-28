from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import math
from multiprocessing import Lock, Process, Queue, Value

shared_counter, shared_lock, shared_queue = None, None, None


def init_worker(counter, lock, monitor_queue: Queue) -> None:
    global shared_counter, shared_lock, shared_queue
    shared_counter = counter
    shared_lock = lock
    shared_queue = monitor_queue


def analyzer(post: dict) -> dict:
    title = post["title"]
    body = post["body"]

    complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(body)

    text = title + body
    hash_object = hashlib.sha256(text.encode("utf-8"))

    post["complexity_score"] = complexity_score
    post["text_hash"] = hash_object.hexdigest()

    with shared_lock:
        shared_counter.value += 1

    return post


def monitor(monitor_queue: Queue) -> None:
    while True:
        post_id = monitor_queue.get()
        if post_id is None:
            break
        print(f"[Monitor] Пост {post_id} обработан")


def analyze_posts(posts: list) -> list:
    results = []

    monitor_queue = Queue()
    counter = Value("i", 0)
    lock = Lock()

    monitor_process = Process(target=monitor, args=(monitor_queue,))
    monitor_process.start()

    try:
        with ProcessPoolExecutor(
            initializer=init_worker, initargs=(counter, lock, monitor_queue)
        ) as executor:
            futures = {executor.submit(analyzer, post): post["id"] for post in posts}

            for future in as_completed(futures):
                post_id = futures[future]

                try:
                    result = future.result()
                    monitor_queue.put(post_id)
                    results.append(result)
                except Exception as e:
                    print(f"Ошибка обработки поста {post_id}: {e}")
    finally:
        monitor_queue.put(None)
        monitor_process.join()
    return results
