import threading
import traceback


def error(error_message, job):
    reply_to = job.get('reply_to', None)
    if reply_to is None:
        return
    with reply_to:
        reply_to.put({
            'status': 'error',
            'message': error_message
        })


def _callback(cb, job):
    try:
        cb(job)
    except Exception as exc:
        error(traceback.format_exc(), job)


def server(ch, callback, max_threads=float('inf')):
    """A generic server.

    Listen on a channel, invoke callback with the received message.
    Limit threads to ``max_threads``.

    :type ch: Channel
    :type callback: Callable[Dict]
    :type max_threads: float

    """
    while True:
        job = ch.get()
        if threading.active_count() > max_threads:
            error('maximum number of threads exceeded', job)
            continue
        t = threading.Thread(target=_callback, args=(callback, job))
        t.start()
