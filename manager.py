# -*- coding: utf-8 -*-

import os, sys, time
import pickle
import subprocess

os_path_expand = lambda p: os.path.expandvars(os.path.expanduser(p))

QUEUE_PATH = os_path_expand('~/.heroshi/queue')
MANAGER_SOCKET = os_path_expand('~/.heroshi/manager.sock')
WORKER_EXECV = ['python', 'crawler.py']

crawl_queue = []
workers = []

def load_queue():
    if not os.path.exists(QUEUE_PATH):
        return
# !!! TEMP
    return
    f = open(QUEUE_PATH, 'rb')
    for line in f:
        queue_item = pickle.loads(line)
        crawl_queue.append(queue_item)
    f.close()

def save_queue():
    path_dir = os.path.dirname(QUEUE_PATH)
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)
    f = open(QUEUE_PATH, 'wb')
    for queue_item in crawl_queue:
        f.write(pickle.dumps(queue_item)+'\n')
    f.close()

# !!! TEMP
def start_worker(url):
    proc = subprocess.Popen(WORKER_EXECV + [url])
    workers.append(proc)

def run_workers(num):
    for i in xrange(min(num, len(crawl_queue))):
        url = crawl_queue.pop()
        start_worker(url)
    while crawl_queue or workers:
        for worker in workers:
            if worker.poll() is not None:
                print(" * debug worker with PID %d quit with status %d" % (worker.pid, worker.returncode))
                workers.remove(worker)
                if crawl_queue:
                    url = crawl_queue.pop()
                    start_worker(url)
                break
        else:
            time.sleep(1)
    print(" * debug crawl queue is empty")

def main():
    if len(sys.argv) < 2:
        print("Usage: %s NUM_WORKERS [URL ...]" % os.path.basename(sys.argv[0]))
        exit()
    # TODO: getopt parameters: start urls, workers count, queue location
    num_workers = int(sys.argv[1])
    urls = []
    if len(sys.argv) > 2:
        urls = sys.argv[2:]
    load_queue()
    for url in reversed(urls):
        crawl_queue.insert(0, url)
    try:
        run_workers(num_workers)
    except KeyboardInterrupt:
        print(" * debug ^C catched")
    finally:
        save_queue()

if __name__ == '__main__':
    main()

