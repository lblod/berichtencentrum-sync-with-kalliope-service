#!/usr/bin/python3
from datetime import datetime
import helpers, escape_helpers
from .kalliope_adapter import new_bericht, new_conversatie

ABB_URI = "http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b"
PUBLIC_GRAPH =  "http://mu.semte.ch/graphs/public"

def mock_bericht():
    return new_bericht(datetime.now().isoformat(),
                       datetime.now().isoformat(),
                       ABB_URI,
                       "http://data.lblod.info/id/bestuurseenheden/003e84121111866af60611a59e13d4c478718f60472655936edec1e352a34c5f",
                       "Hello,\n\nThis is a test message from ABB @{}.".format(datetime.now().isoformat()))

def mock_conversatie(dossiernummer):
    return new_conversatie(dossiernummer,
                           "Meerjarenplanwijziging {}".format(dossiernummer),
                           "Opvraging in kader van een toezichtsdossier",
                           "P30D")
