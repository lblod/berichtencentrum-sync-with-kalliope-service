#!/usr/bin/python3
import requests
import helpers, escape_helpers

def new_conversatie(dossiernummer,
                    betreft,
                    type_communicatie,
                    reactietermijn,
                    berichten=[]):
    conversatie = {}
    conversatie['uuid'] = helpers.generate_uuid()
    conversatie['dossiernummer'] = dossiernummer
    conversatie['betreft'] = betreft
    conversatie['type_communicatie'] = type_communicatie
    conversatie['reactietermijn'] = reactietermijn
    conversatie['berichten'] = list(berichten)
    return conversatie

def new_bericht(verzonden,
                ontvangen,
                van,
                naar,
                inhoud):
    bericht = {}
    bericht['uuid'] = helpers.generate_uuid()
    bericht['verzonden'] = verzonden
    bericht['ontvangen'] = ontvangen
    bericht['van'] = van
    bericht['naar'] = naar
    bericht['inhoud'] = inhoud
    return bericht

def parse_kalliope_poststuk_uit(arg):
    """
    Parse the response from the Kalliope API into our bericht format

    :param ?:
    :returns: dict containing the properties of the bericht
    """
    pass

def construct_kalliope_poststuk_in(arg):
    """
    Prepare the payload for sending messages to the Kalliope API.

    :param ?:
    :returns:
    """
    pass

def get_kalliope_poststukken_uit(arg):
    """
    Perform the API-call to get all poststukken-uit that are ready to be processed.

    :param ?:
    :returns: tuple of poststukken
    """
    pass

def post_kalliope_poststuk_in(arg):
    """
    Perform the API-call to a new poststuk to Kalliope.

    :param ?:
    :returns:
    """
    pass
