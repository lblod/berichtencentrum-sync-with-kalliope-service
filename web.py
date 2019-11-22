import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from helpers import log
from .tasks import process_inzendingen
from .tasks import process_berichten_in
from .tasks import process_berichten_out

BERICHTEN_CRON_PATTERN = os.environ.get('BERICHTEN_CRON_PATTERN')
INZENDINGEN_CRON_PATTERN = os.environ.get('INZENDINGEN_CRON_PATTERN')

scheduler = BackgroundScheduler()
scheduler.add_job(process_inzendingen, CronTrigger.from_crontab(INZENDINGEN_CRON_PATTERN))
log("Registered a task for fetching and processing inzendingen to Kalliope following pattern {}".format(INZENDINGEN_CRON_PATTERN))
scheduler.add_job(process_berichten_in, CronTrigger.from_crontab(BERICHTEN_CRON_PATTERN))
log("Registered a task for fetching and processing messages from Kalliope following pattern {}".format(BERICHTEN_CRON_PATTERN))
scheduler.add_job(process_berichten_out, CronTrigger.from_crontab(BERICHTEN_CRON_PATTERN))
log("Registered a task for fetching and processing messages to Kalliope following pattern {}".format(BERICHTEN_CRON_PATTERN))
scheduler.start()
