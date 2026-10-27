from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from queue import Queue, Empty
from threading import Lock, Event
import analyzer
import time

COLUMN_WIDTHS = (6, 40, 16, 32)
HEADERS = ("id", "title", "complexity_score", "text_hash")

shared_lock, shared_queue, shared_event, shared_counter = None, None, None, None

def format_row(values):
    cells = [
        str(value)[:width].ljust(width)
        for value, width in zip(values, COLUMN_WIDTHS)
    ]
    return "| " + " | ".join(cells) + " |"

def init_worker(lock, queue, event, counter):
    global shared_lock, shared_queue, shared_event, shared_counter
    shared_lock = lock
    shared_queue = queue
    shared_event = event
    shared_counter = counter

def save_post(post_data):
    time.sleep(0.05)
    with shared_lock:
        with open("results.log", "a") as file:
            file.write(format_row(post_data) + "\n")
        shared_counter[0] += 1

def worker():
    while True:
        try:
            post = shared_queue.get(timeout=1)
        except Empty:
            if shared_event.is_set():
                break
        else:
            id = str(post['id'])
            title = str(post['title'])
            score = str(post['complexity_score'])
            hash = str(post['text_hash'])
            post_data = [id, title, score, hash]
            save_post(post_data)

def progress(current,Y, lock):
    while True:
        with lock:
            X = current[0]
        print(f"Прогресс сохранения: {X}/{Y}")
        if X >= Y:
            print("Сохранено")
            break
        time.sleep(0.1)
            
    


def main():
    posts = analyzer.main()
    lock = Lock()
    queue = Queue()
    event = Event()
    counter = [0]

    for post in posts:
        queue.put(post)
    event.set()


    border = "+-" + "-+-".join("-" * width for width in COLUMN_WIDTHS) + "-+"

    with open("results.log", "w", encoding="utf-8") as file:
        file.write(border + "\n")
        file.write(format_row(HEADERS) + "\n")
        file.write(border + "\n")


    with ThreadPoolExecutor(max_workers=5, initializer=init_worker, initargs=(lock, queue, event, counter)) as executor:
        progress_thread = Thread(target=progress, args=(counter, len(posts), lock))
        progress_thread.start()
        for _ in range(5):
            executor.submit(worker)

    progress_thread.join()

if __name__ == '__main__':
    main()