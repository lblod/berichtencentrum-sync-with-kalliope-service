import os
from apscheduler.schedulers.background import BackgroundScheduler
from helpers import log
from .tasks import process_berichten_in
from .tasks import process_berichten_out

INTERVAL = 5 if os.environ.get('RUN_INTERVAL') is None else os.environ.get('RUN_INTERVAL')

scheduler = BackgroundScheduler()
# scheduler.add_job(func=process_berichten_in, trigger="interval", minutes=INTERVAL)
log("Registered a task for fetching and processing messages from Kalliope every {} minutes".format(INTERVAL))
# scheduler.add_job(func=process_berichten_out, trigger="interval", minutes=INTERVAL)
log("Registered a task for fetching and processing messages to Kalliope every {} minutes".format(INTERVAL))
scheduler.start()


################# TEMP: test routes ############################################

import flask
from .queries import construct_unsent_berichten_query
from .queries import construct_conversatie_exists_query
from .queries import construct_insert_conversatie_query
from .mocks import mock_bericht, mock_conversatie

ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"

@app.route('/unsent/')
def unsent():
    q = construct_unsent_berichten_query(PUBLIC_GRAPH, ABB_URI)
    return flask.jsonify(helpers.query(q))

@app.route("/conversatie_exists/<dossiernummer>")
def exists(dossiernummer):
    q = construct_conversatie_exists_query(PUBLIC_GRAPH, dossiernummer)
    return flask.jsonify(helpers.query(q))

@app.route("/mock_insert")
def mock():
    b = mock_bericht()
    c = mock_conversatie()
    q = construct_insert_conversatie_query(PUBLIC_GRAPH, c, b)
    return flask.jsonify(helpers.update(q))
