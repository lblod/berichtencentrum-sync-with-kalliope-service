import os
from apscheduler.schedulers.background import BackgroundScheduler
from helpers import log
from .tasks import process_inzendingen
from .tasks import process_berichten_in
from .tasks import process_berichten_out

RUN_INTERVAL_BERICHTEN = int(os.environ.get('RUN_INTERVAL_BERICHTEN')) #in minutes
RUN_INTERVAL_INZENDINGEN = int(os.environ.get('RUN_INTERVAL_INZENDINGEN')) #in minutes

scheduler = BackgroundScheduler()
scheduler.add_job(func=process_inzendingen, trigger="interval", minutes=RUN_INTERVAL_INZENDINGEN)
log("Registered a task for fetching and processing inzendingen to Kalliope every {} minutes".format(RUN_INTERVAL_INZENDINGEN))
scheduler.add_job(func=process_berichten_in, trigger="interval", minutes=RUN_INTERVAL_BERICHTEN)
log("Registered a task for fetching and processing messages from Kalliope every {} minutes".format(RUN_INTERVAL_BERICHTEN))
scheduler.add_job(func=process_berichten_out, trigger="interval", minutes=RUN_INTERVAL_BERICHTEN)
log("Registered a task for fetching and processing messages to Kalliope every {} minutes".format(RUN_INTERVAL_BERICHTEN))
scheduler.start()
