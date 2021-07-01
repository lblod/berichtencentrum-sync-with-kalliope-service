import os
from pytz import timezone
from helpers import log

from .sudo_query_helpers import query, update
from .update_with_supressed_fail import update_with_suppressed_fail

from .queries import STATUS_DELIVERED_CONFIRMED, STATUS_DELIVERED_UNCONFIRMED, STATUS_DELIVERED_CONFIRMATION_FAILED
from .queries import construct_get_messages_by_status, construct_update_bericht_status
from .queries import construct_create_kalliope_sync_error_query
from .queries import construct_increment_confirmation_attempts_query

from .kalliope_adapter import post_kalliope_poststuk_uit_confirmation
from .kalliope_adapter import open_kalliope_api_session

TIMEZONE = timezone('Europe/Brussels')
MAX_CONFIRMATION_ATTEMPTS = int(os.environ.get('MAX_CONFIRMATION_ATTEMPTS'))
PS_UIT_CONFIRMATION_PATH = os.environ.get('KALLIOPE_PS_UIT_CONFIRMATION_ENDPOINT')
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"


def process_confirmations():
    try:
        log("Checking for new delivery confirmations to process")

        query_string = construct_get_messages_by_status(STATUS_DELIVERED_UNCONFIRMED, MAX_CONFIRMATION_ATTEMPTS)
        berichten = query(query_string).get('results', {}).get('bindings', [])

        log("Found {} confirmations that need to be sent to the Kalliope API".format(len(berichten)))

        if len(berichten) == 0:
            log("No confirmations need to be sent, I am going to get a coffee")
        else:
            with open_kalliope_api_session() as session:
                for bericht in berichten:
                    process_confirmation(session, bericht)

    except Exception as e:
        message = """
                General error while trying to run the process confirmations job.
                    Error: {}
                """.format(e)
        # TODO: this PUBLIC_GRAPH should really be another graph!!!! (now done for consistency)
        error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, None, message, e)
        update_with_suppressed_fail(error_query)
        log(message)


def process_confirmation(session, bericht):
    try:
        attempt = bericht["confirmationAttempts"]["value"] if "confirmationAttempts" in bericht.keys() else 0
        log("Attempt to confirm {} number {}".format(bericht["bericht"]["value"], attempt))

        if (int(attempt) >= int(MAX_CONFIRMATION_ATTEMPTS)):
            log('Maximum number of attempts reached. Setting status of {} to {}'.
                format(bericht["bericht"]["value"], STATUS_DELIVERED_CONFIRMATION_FAILED))
            failed_q = construct_update_bericht_status(bericht["bericht"]["value"], STATUS_DELIVERED_CONFIRMATION_FAILED)
            update(failed_q)
        else:
            poststuk_uit_confirmation = {
                'uriPoststukUit': bericht["bericht"]["value"],
                'datumBeschikbaarheid': bericht["deliveredAt"]["value"]
            }

            post_result = post_kalliope_poststuk_uit_confirmation(PS_UIT_CONFIRMATION_PATH,
                                                                  session,
                                                                  poststuk_uit_confirmation)

            if post_result:
                # TODO: note, the implicit assumption here is that in some cases,
                # the same confirmation might be sent twice.
                # (i.e. when confirmation to K. was ok, but next statement fails)
                # Anyway, there is no way around this, if you need robust confirmation...
                confirmation_q = construct_update_bericht_status(bericht["bericht"]["value"],
                                                                 STATUS_DELIVERED_CONFIRMED)
                log("successfully sent confirmation to Kalliope for message {}".format(bericht["bericht"]["value"]))
                update(confirmation_q)

    except Exception as e:
        message = """
                General error while trying to process the confirmation for message {}.
                    Error: {}
                """.format(bericht["bericht"]["value"], e)
        # TODO: this PUBLIC_GRAPH should really be another graph!!!! (now done for consistency)
        error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, bericht["bericht"]["value"], message, e)
        update_with_suppressed_fail(error_query)
        confirmation_query = construct_increment_confirmation_attempts_query(bericht["g"]["value"],
                                                                             bericht["bericht"]["value"])
        update_with_suppressed_fail(confirmation_query)
        log(message)
