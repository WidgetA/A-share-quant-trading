"""Native FC event entry point; package this file as handler.py at ZIP root."""

from src.data.fc_yahoo_worker import FCYahooWorker

_worker = FCYahooWorker()


def handler(event, context):
    # Let the FC runtime report exceptions as function failures. A failed Yahoo
    # request is never returned as a successful price envelope.
    return _worker.invoke(event, context)
