import threading


def exceeded_warn(msg):
    reply_to = msg.get('reply_to', None)
    if reply_to is None:
        return
    with reply_to:
        reply_to.put({
            'status': 'error',
            'message': 'maximum number of threads exceeded'
        })


def server(ch, callback, max_threads=float('inf')):
    while True:
        job = ch.get()
        if threading.active_count() > max_threads:
            exceeded_warn(job)
            continue
        t = threading.Thread(target=callback, args=(job,))
        t.start()
