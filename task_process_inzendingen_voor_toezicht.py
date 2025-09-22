import os
from pytz import timezone
from datetime import datetime
from helpers import log
from .sudo_query_helpers import query, update
from .kalliope_adapter import post_kalliope_inzending_in
from .kalliope_adapter import open_kalliope_api_session
from .queries import construct_unsent_inzendingen_query
from .queries import construct_increment_inzending_attempts_query
from .queries import construct_inzending_sent_query
from .queries import construct_create_kalliope_sync_error_query
from .queries import verify_eb_has_cb_exclusion_rule
from .queries import verify_eb_exclusion_rule
from .queries import verify_cb_exclusion_rule
from .queries import verify_ro_exclusion_rule
from .queries import verify_go_exclusion_rule
from .queries import verify_po_exclusion_rule
from .queries import verify_mp_exclusion_rule
from .queries import verify_opnavb_exclusion_rule
from .update_with_supressed_fail import update_with_suppressed_fail
from dateutil import parser



TIMEZONE = timezone('Europe/Brussels')
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
INZENDING_IN_PATH = os.environ.get('KALLIOPE_PS_IN_ENDPOINT')
MAX_SENDING_ATTEMPTS = int(os.environ.get('MAX_SENDING_ATTEMPTS'))
INZENDING_BASE_URL = os.environ.get('INZENDING_BASE_URL')
EREDIENSTEN_BASE_URL = os.environ.get('EREDIENSTEN_BASE_URL')


def process_inzendingen():
    """
    Fetch submissions that have to be sent to Kalliope from the triple store,
        convert them to the correct format for the Kalliope API, post them and finally mark them as sent.
    :returns: None
    """
    q = construct_unsent_inzendingen_query(MAX_SENDING_ATTEMPTS)
    inzendingen = query(q)['results']['bindings']

    # Here we remove inzendingen that matches exclusion criteria from business rules
    filtered_inzendingen = exclude_inzendingen_from_rules(inzendingen)

    inzendingen = [parse_inzending_sparql_response(inzending_res) for inzending_res in filtered_inzendingen]
    log("Found {} submissions that need to be sent to the Kalliope API".format(len(inzendingen)))
    if len(inzendingen) == 0:
        return

    with open_kalliope_api_session() as session:
        for inzending in inzendingen:
            try:
                #  NOTE: Add graph as argument to query because Virtuoso
                bestuurseenheid_uuid = inzending['afzenderUri'].split('/')[-1]
                graph = \
                    "http://mu.semte.ch/graphs/organizations/{}/LoketLB-toezichtGebruiker".format(bestuurseenheid_uuid)
                try:
                    post_result = post_kalliope_inzending_in(INZENDING_IN_PATH, session, inzending)
                except Exception as e:
                    message = """
                              Something went wrong while posting following inzending in, skipping: {}\n{}
                              """.format(inzending, e)

                    error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, inzending['uri'], message, e)
                    update(error_query)
                    attempt_query = construct_increment_inzending_attempts_query(graph, inzending['uri'])
                    update(attempt_query)
                    log(message)

                    continue

                if post_result:
                    #  We consider the moment when the api-call succeeded the 'ontvangen'-time
                    ontvangen = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat()
                    q_sent = construct_inzending_sent_query(graph, inzending['uri'], ontvangen)
                    update(q_sent)
                    log("successfully sent submission {} to Kalliope".format(inzending['uri']))

            except Exception as e:
                inzending_uri = inzending.get('uri')
                message = """
                           General error while trying to process inzending {}.
                            Error: {}
                          """.format(inzending_uri, e)
                error_query = construct_create_kalliope_sync_error_query(PUBLIC_GRAPH, inzending_uri, message, e)
                update_with_suppressed_fail(error_query)
                # TODO: graph here should be re-thought...
                # attempt_query = construct_increment_inzending_attempts_query(graph, inzending_uri)
                # update_with_suppressed_fail(attempt_query)
                log(message)
    pass

def determine_url(inzending_res):
    """
    Determine the correct URL for 'urlToezicht'.
    """
    # Rules to decide if it should be worship based
    RULES = [
        #Gemeente / Provincie
        {"decisionType": ["https://data.vlaanderen.be/id/concept/BesluitDocumentType/4f938e44-8bce-4d3a-b5a7-b84754fe981a", "https://data.vlaanderen.be/id/concept/BesluitType/79414af4-4f57-4ca3-aaa4-f8f1e015e71c", "https://data.vlaanderen.be/id/concept/BesluitType/b25faa84-3ab5-47ae-98c0-1b389c77b827"],
            "bestuurseenheidType": ["http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000001", "http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000000"]},
        # Bestuur van de eredienst
        {"decisionType": ["https://data.vlaanderen.be/id/concept/BesluitDocumentType/a970c99d-c06c-4942-9815-153bf3e87df2",
                            "https://data.vlaanderen.be/id/concept/BesluitType/54b61cbd-349f-41c4-9c8a-7e8e67d08347",
                            "https://data.vlaanderen.be/id/concept/BesluitType/e44c535d-4339-4d15-bdbf-d4be6046de2c"],
            "bestuurseenheidType": "http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/66ec74fd-8cfc-4e16-99c6-350b35012e86"},
        #Centraal bestuur van de eredienst
        {"decisionType": "https://data.vlaanderen.be/id/concept/BesluitDocumentType/672bf096-dccd-40af-ab60-bd7de15cc461",
            "bestuurseenheidType": "http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/f9cac08a-13c1-49da-9bcb-f650b0604054"},
        # Representatief orgaan
        {"decisionType": ["https://data.vlaanderen.be/id/concept/BesluitDocumentType/651525f8-8650-4ce8-8eea-f19b94d50b73",
                            "https://data.vlaanderen.be/id/concept/BesluitDocumentType/d611364b-007b-49a7-b2bf-b8f4e5568777",
                            "https://data.vlaanderen.be/id/concept/BesluitDocumentType/6d1a3aea-6773-4e10-924d-38be596c5e2e",
                            "https://data.vlaanderen.be/id/concept/BesluitDocumentType/14793940-5b9c-4172-b108-c73665ad9d6a",
                            "https://data.vlaanderen.be/id/concept/BesluitDocumentType/95a6c5a1-05af-4d48-b2ef-5ebb1e58783b"],
            "bestuurseenheidType": "http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/36372fad-0358-499c-a4e3-f412d2eae213"},
        # (Centraal) bestuur van de eredienst
        {"decisionType": "https://data.vlaanderen.be/id/concept/BesluitType/41a09f6c-7964-4777-8375-437ef61ed946",
            "bestuurseenheidType": ["http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/66ec74fd-8cfc-4e16-99c6-350b35012e86", "http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/f9cac08a-13c1-49da-9bcb-f650b0604054"]},
    ]

    decision_type = inzending_res['decisionType']['value']
    afzender_class = inzending_res['bestuurseenheidType']['value']
    url = INZENDING_BASE_URL + '/' + inzending_res['inzendingUuid']['value']

    for rule in RULES:
        rule_decision_types = rule['decisionType'] if isinstance(rule['decisionType'], list) else [rule['decisionType']]
        rule_bestuurseenheid_types = rule['bestuurseenheidType'] if isinstance(rule['bestuurseenheidType'], list) else [rule['bestuurseenheidType']]

        # update URL if rule matches
        if decision_type in rule_decision_types and afzender_class in rule_bestuurseenheid_types:
            url = EREDIENSTEN_BASE_URL + '/' + inzending_res['inzendingUuid']['value']

    # special case override
    if decision_type == 'https://data.vlaanderen.be/id/concept/BesluitType/95c671c2-3ab7-43e2-a90d-9b096c84bfe7':
        url = EREDIENSTEN_BASE_URL + '/' + inzending_res['inzendingUuid']['value']

    return url

def parse_inzending_sparql_response(inzending_res):
    session_date = inzending_res.get('sessionDate', {}).get('value', '')
    if session_date:
        session_date = parser.isoparse(session_date)
        session_date = session_date.astimezone(TIMEZONE)
        session_date = session_date.strftime('%Y-%m-%d')

    inzending = {
        'uri': inzending_res['inzending']['value'],
        'afzenderUri': inzending_res['bestuurseenheid']['value'],
        'betreft': inzending_res['decisionTypeLabel']['value'] + ' ' +
          session_date,
        'urlToezicht': determine_url(inzending_res),
        'typePoststuk': 'https://kalliope.abb.vlaanderen.be/ld/algemeen/dossierType/besluit',
        'typeMelding': inzending_res['decisionType']['value'],
        'datumVanVerzenden': inzending_res['datumVanVerzenden']['value']
    }

    #  NOTE: Kalliope expects "boekjaar" to be an int.
    #        At this stage we can not guarantee this to be true.
    try:
        value = inzending_res.get('boekjaar', {}).get('value', '')
        if value != "":
            inzending['boekjaar'] = int(value)
    except ValueError:
        log("Invalid value \"{}\" for boekjaar will be ignored, expected an int.".format(value))

    return inzending

def exclude_inzendingen_from_rules(inzendingen):
    """
    This takes an individual submission to run ASK queries to check if it matches the pattern from business rules (a submission's formData who has a specific decisionType and sender needs to be excluded when they match a certain criteria in the list);
    It will then sort them out from the inzendingen.
    see: Leesrechtenlogica Databank Erediensten
    """
    filtered_inzendingen = []

    for inzending in inzendingen:

        submission = inzending['inzending']['value']

        eb_has_cb = query(verify_eb_has_cb_exclusion_rule(submission))['boolean']
        eb = query(verify_eb_exclusion_rule(submission))['boolean']
        cb = query(verify_cb_exclusion_rule(submission))['boolean']
        ro = query(verify_ro_exclusion_rule(submission))['boolean']
        go = query(verify_go_exclusion_rule(submission))['boolean']
        po = query(verify_po_exclusion_rule(submission))['boolean']
        mp = query(verify_mp_exclusion_rule(submission))['boolean'] 
        opnavb = query(verify_opnavb_exclusion_rule(submission))['boolean'] 

        if not (eb_has_cb or eb or cb or ro or go or po or mp or opnavb):
            filtered_inzendingen.append(inzending)

    return filtered_inzendingen

