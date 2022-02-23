#!/usr/bin/python3
import copy
import escape_helpers
import helpers
from helpers import log
from datetime import datetime
from pytz import timezone
import re

TIMEZONE = timezone('Europe/Brussels')
STATUS_DELIVERED_UNCONFIRMED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/unconfirmed"
STATUS_DELIVERED_CONFIRMED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/confirmed"
STATUS_DELIVERED_CONFIRMATION_FAILED = \
    "http://data.lblod.info/id/status/berichtencentrum/sync-with-kalliope/delivered/failedConfirmation"


def sparql_escape_string(obj):
    log("""Warning: using a monkey patched
         sparql_escape_string.
         TODO: move this to template""")

    obj = str(obj)

    def replacer(a):
        return "\\"+a.group(0)

    return '"""' + re.sub(r'[\\\"]', replacer, obj) + '"""'


# monkey patch escape_helpers. TODO: mov
escape_helpers.sparql_escape_string = sparql_escape_string


def construct_conversatie_exists_query(graph_uri, referentieABB):
    """
    Construct a query for selecting a conversatie based on referentieABB
    (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param referentieABB: string
    :returns: string containing SPARQL query
    """
    referentieABB = escape_helpers.sparql_escape_string(referentieABB)
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie
        WHERE {{
            GRAPH <{}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier {}.
            }}
        }}
        """.format(graph_uri, referentieABB)
    return q


def construct_bestuurseenheid_exists_query(bestuurseeheid_uri):
    """
    Construct a query for asking if a bestuurseenehid exists in our database.

    :param bestuurseeheid_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

        ASK {{
            <{0}> a besluit:Bestuurseenheid .
        }}
        """.format(bestuurseeheid_uri)
    return q


def construct_bericht_exists_query(graph_uri, bericht_uri):
    """
    Construct a query for selecting a bericht based on its URI, retrieving the conversatie & referentieABB at the same time.

    :param graph_uri: string
    :param bericht_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie ?referentieABB
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                ?conversatie a schema:Conversation;
                    schema:hasPart <{1}>;
                    schema:identifier ?referentieABB.
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q


def construct_insert_conversatie_query(graph_uri, conversatie, bericht, delivery_timestamp):
    """
    Construct a SPARQL query for inserting a new conversatie with a first bericht attached.

    :param graph_uri: string
    :param conversatie: dict containing escaped properties for conversatie
    :param bericht: dict containing escaped properties for bericht
    :returns: string containing SPARQL query
    """
    conversatie = copy.deepcopy(conversatie)  # For not modifying the pass-by-name original
    conversatie['referentieABB'] = escape_helpers.sparql_escape_string(conversatie['referentieABB'])
    conversatie['betreft'] = escape_helpers.sparql_escape_string(conversatie['betreft'])
    conversatie['current_type_communicatie'] =\
        escape_helpers.sparql_escape_string(conversatie['current_type_communicatie'])
    bericht = copy.deepcopy(bericht)  # For not modifying the pass-by-name original
    bericht['inhoud'] = escape_helpers.sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier {1[referentieABB]};
     """
    if conversatie["dossierUri"]:
        q += """
                        ext:dossierUri "{1[dossierUri]}";
             """
    q += """
                    schema:about {1[betreft]};
                    <http://mu.semte.ch/vocabularies/ext/currentType> {1[current_type_communicatie]};
                    schema:processingTime "{1[reactietermijn]}";
                    schema:hasPart <{2[uri]}>.

                <{2[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    schema:dateSent "{2[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{2[ontvangen]}"^^xsd:dateTime;
                    schema:text {2[inhoud]};
                    <http://purl.org/dc/terms/type> "{2[type_communicatie]}";
                    schema:sender <{2[van]}>;
                    schema:recipient <{2[naar]}>;
                    adms:status <{3}>;
                    ext:deliveredAt "{4}"^^xsd:dateTime.
            }}
        }}
    """
    q = q.format(graph_uri, conversatie, bericht, STATUS_DELIVERED_UNCONFIRMED, delivery_timestamp)
    return q


def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri, delivery_timestamp):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :param delivery_timestamp: string
    :returns: string containing SPARQL query
    """
    bericht = copy.deepcopy(bericht)  # For not modifying the pass-by-name original
    bericht['inhoud'] = escape_helpers.sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{2}> a schema:Conversation;
                    schema:hasPart <{1[uri]}>.
                <{1[uri]}> a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:dateSent "{1[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{1[ontvangen]}"^^xsd:dateTime;
                    schema:text {1[inhoud]};
                    schema:sender <{1[van]}>;
                    schema:recipient <{1[naar]}>;
                    adms:status <{3}>;
                    ext:deliveredAt "{4}"^^xsd:dateTime;
                    <http://purl.org/dc/terms/type> "{1[type_communicatie]}".
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri,
                   STATUS_DELIVERED_UNCONFIRMED, delivery_timestamp)
    return q


def construct_update_conversatie_type_query(graph_uri, conversatie_uri, type_communicatie):
    """
    Construct a SPARQL query for updating the type-communicatie of a conversatie.

    :param graph_uri: string
    :param conversatie_uri: string containing the uri of the conversatie we want to update
    :param type_communicatie: string containing the type-communicatie

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> ?type.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> "{2}".
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/ext/currentType> ?type.
            }}
        }}
        """.format(graph_uri, conversatie_uri, type_communicatie)
    return q


def construct_insert_bijlage_query(bericht_graph_uri, bericht_uri, bijlage, file):
    """
    Construct a SPARQL query for inserting a bijlage and attaching it to an existing bericht.

    :param bericht_graph_uri: string
    :param bericht_uri: string
    :param bijlage: dict containing escaped properties for bijlage
    :param file: dict containing escaped properties for file (similar to bijlage, see mu-file-service)
    :returns: string containing SPARQL query
    """
    bijlage = copy.deepcopy(bijlage) # For not modifying the pass-by-name original
    bijlage['name'] = escape_helpers.sparql_escape_string(bijlage['name'])
    bijlage['mimetype'] = escape_helpers.sparql_escape_string(bijlage['mimetype'])
    file = copy.deepcopy(file) # For not modifying the pass-by-name original
    file['name'] = escape_helpers.sparql_escape_string(file['name'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX dbpedia: <http://dbpedia.org/ontology/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1}> nie:hasPart <{2[uri]}>.
                <{2[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    nfo:fileName {2[name]};
                    dct:format {2[mimetype]};
                    dct:created "{2[created]}"^^xsd:dateTime;
                    nfo:fileSize "{2[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{2[extension]}".
                <{3[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{3[uuid]}";
                    nfo:fileName {3[name]};
                    dct:format {2[mimetype]};
                    dct:created "{2[created]}"^^xsd:dateTime;
                    nfo:fileSize "{2[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{2[extension]}";
                    nie:dataSource <{2[uri]}>.
            }}
        }}
        """.format(bericht_graph_uri, bericht_uri, bijlage, file)
    return q


def construct_update_last_bericht_query(conversatie_uri):
    """
    Construct a SPARQL query for keeping the last message of a conversation up to date.

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {{
            GRAPH ?g {{
                ?conversation ext:lastMessage ?message.
            }}
        }}
        INSERT {{
            GRAPH ?g {{
                ?conversation ext:lastMessage ?newMessage.
            }}
        }}
        WHERE {{
            BIND(<{0}> as ?conversation)
            GRAPH ?g {{
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage.
                OPTIONAL {{  ?conversation ext:lastMessage ?message. }}
            }}
            {{
                SELECT (?message AS ?newMessage) WHERE {{
                    GRAPH ?g {{
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {{
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent < ?otherDateSent  )
                        }}
                    }}
                }}
            }}
        }}
        """.format(conversatie_uri)
    return q


def construct_unsent_berichten_query(naar_uri, max_sending_attempts):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param naar_uri: URI of the recipient for which we want to retrieve messages that have yet to be sent.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        SELECT DISTINCT ?referentieABB ?dossieruri ?bericht ?betreft ?uuid ?van ?verzonden ?inhoud
        WHERE {{
            GRAPH ?g {{
                ?conversatie a schema:Conversation;
                    schema:identifier ?referentieABB;
                    schema:about ?betreft;
                    schema:hasPart ?bericht.
                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?uuid;
                    schema:dateSent ?verzonden;
                    schema:text ?inhoud;
                    schema:sender ?van;
                    schema:recipient <{0}>.
                FILTER NOT EXISTS {{ ?bericht schema:dateReceived ?ontvangen. }}
                OPTIONAL {{
                    ?conversatie ext:dossierUri ?dossieruri.
                }}
                BIND(0 AS ?default_attempts)
                OPTIONAL {{ ?bericht ext:failedSendingAttempts ?attempts. }}
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                FILTER(?result_attempts < {1})
            }}
        }}
        """.format(naar_uri, max_sending_attempts)
    return q


def construct_select_bijlagen_query(bericht_uri):
    """
    Construct a SPARQL query for retrieving all bijlages for a given bericht.

    :param bericht_uri: URI of the bericht for which we want to retrieve bijlagen.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?bijlagenaam ?file ?type WHERE {{
            <{0}> a schema:Message;
                nie:hasPart ?bijlage.

            ?bijlage a nfo:FileDataObject;
                nfo:fileName ?bijlagenaam;
                dct:format ?type.
            ?file nie:dataSource ?bijlage.
        }}
        """.format(bericht_uri)
    return q


def construct_increment_bericht_attempts_query(graph_uri, bericht_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a certain message without succes.

    :param graph_uri: string
    :param bericht_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                OPTIONAL {{ <{1}> ext:failedSendingAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q


def construct_bericht_sent_query(graph_uri, bericht_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party (and thus 'sent' by us)

    :param graph_uri: string
    :param bericht_uri: URI of the bericht we would like to mark as sent.
    :param verzonden: ISO-string representation of the datetetime when the message was sent
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT {{
            GRAPH <{0}> {{
                <{1}> schema:dateReceived "{2}"^^xsd:dateTime.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
            }}
        }}
        """.format(graph_uri, bericht_uri, verzonden)
    return q


def construct_select_original_bericht_query(bericht_uri):
    """
    Construct a SPARQL query for selecting the first message in a conversation

    :param bericht_uri: URI of a bericht in a conversation
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        SELECT ?origineelbericht WHERE {{
            GRAPH ?g {{
                ?conversation a schema:Conversation;
                    schema:hasPart ?origineelbericht;
                    schema:hasPart <{0}>.
            }}
            {{
                SELECT (?message AS ?origineelbericht) WHERE {{
                    GRAPH ?g {{
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {{
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent > ?otherDateSent )
                        }}
                    }}
                }}
            }}
        }}
        """.format(bericht_uri)
    return q


def construct_unsent_inzendingen_query(max_sending_attempts):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param max_sending_attempts: the maximum number of delivery attempts that have to be done
    :returns: string containing SPARQL query
    """

    allowedDecisionTypesList = [
        '<https://data.vlaanderen.be/id/concept/BesluitDocumentType/0ee460b1-5ef4-4d4a-b5e1-e2d7c1d5086e>',
        '<https://data.vlaanderen.be/id/concept/BesluitDocumentType/e274f1b1-7e84-457d-befe-070afec6b752>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/f8c070bd-96e4-43a1-8c6e-532bcd771251>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/40831a2c-771d-4b41-9720-0399998f1873>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/380674ee-0894-4c41-bcc1-9deaeb9d464c>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/f56c645d-b8e1-4066-813d-e213f5bc529f>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/e44c535d-4339-4d15-bdbf-d4be6046de2c>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/c945b531-4742-43fe-af55-b13da6ecc6fe>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/bd0b0c42-ba5e-4acc-b644-95f6aad904c7>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/1105564e-30c7-4371-a864-6b7329cdae6f>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/e27ef237-29de-49b8-be22-4ee2ab2d4e5b>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/dbc58656-b0a5-4e43-8e9e-701acb75f9b0>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/b69c9f18-967c-4feb-90a8-8eea3c8ce46b>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/4efa4632-efc6-40d5-815a-dec785fbceac>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/9f12dc58-18ba-4a1f-9e7a-cf73d0b4f025>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/849c66c2-ba33-4ac1-a693-be48d8ac7bc7>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/82d0696e-1225-4684-826a-923b2453f5e3>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/c417f3da-a3bd-47c5-84bf-29007323a362>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/2f189152-1786-4b55-a3a9-d7f06de63f1c>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/a0a709a7-ac07-4457-8d40-de4aea9b1432>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/3fcf7dba-2e5b-4955-a489-6dd8285c013b>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/79414af4-4f57-4ca3-aaa4-f8f1e015e71c>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/df261490-cc74-4f80-b783-41c35e720b46>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/b25faa84-3ab5-47ae-98c0-1b389c77b827>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/8bdc614a-d2f2-44c0-8cb1-447b1017d312>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/d9c3d177-6dc6-4775-8c6a-1055a9cbdcc6>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/bf72e38a-2c73-4484-b82f-c642a4c39d0c>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/4350cdda-8291-4055-9026-5c7429357fce>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/6af621e2-c807-479e-a6f2-2d64d8339491>',
        '<https://data.vlaanderen.be/id/concept/BesluitDocumentType/f4b1b40a-4ac4-4e12-bdd2-84966f274edc>',
        '<https://data.vlaanderen.be/id/concept/BesluitDocumentType/d60eb90b-926b-4c64-b87c-866c0cf92f0a>',
        '<https://data.vlaanderen.be/id/concept/BesluitType/cb361927-1aab-4016-bd8a-1a84841391ba>', # Collectieve motie van wantrouwen
        '<https://data.vlaanderen.be/id/concept/BesluitDocumentType/365d561c-57c7-4523-af04-6e3c91426c56>', # Overzicht vergoedingen en presentiegelden
        '<https://data.vlaanderen.be/id/concept/BesluitType/4511f992-2b52-42fe-9cb6-feae6241ad26>', # Saneringsplan
        '<https://data.vlaanderen.be/id/concept/BesluitType/b04bc642-c892-4aae-ac1f-f6ff21362704>' # code van goed bestuur
    ]
    separator = ' '

    q = """
        PREFIX meb:     <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX dct:     <http://purl.org/dc/terms/>
        PREFIX nmo:     <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX ext:     <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms:    <http://www.w3.org/ns/adms#>
        PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
        PREFIX prov:    <http://www.w3.org/ns/prov#>
        PREFIX eli:     <http://data.europa.eu/eli/ontology#>
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>

        SELECT DISTINCT ?inzending ?inzendingUuid ?bestuurseenheid ?decisionType ?sessionDate
                        ?decisionTypeLabel ?datumVanVerzenden ?boekjaar
        WHERE {{
            GRAPH ?g {{
                ?inzending a meb:Submission ;
                    adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?inzendingUuid ;
                    <http://purl.org/pav/createdBy> ?bestuurseenheid;
                    <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#sentDate> ?datumVanVerzenden;
                    prov:generated ?formData .

                ?formData dct:type ?decisionType .

                OPTIONAL {{ ?formData <http://linkedeconomy.org/ontology#financialYear> ?boekjaar . }}

                VALUES ?decisionType {{ {1} }}

                FILTER NOT EXISTS {{ ?inzending nmo:receivedDate ?receivedDate. }}

                OPTIONAL {{ ?formData ext:sessionStartedAtTime ?sessionDate. }}

                BIND(0 AS ?default_attempts)
                OPTIONAL {{ ?inzending ext:failedSendingAttempts ?attempts. }}
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                FILTER(?result_attempts < {0})
            }}
            GRAPH ?h {{
                OPTIONAL {{ ?decisionType skos:prefLabel ?decisionTypeLabel }} .
            }}
        }}
        """.format(max_sending_attempts, separator.join(allowedDecisionTypesList))
    return q


def construct_increment_inzending_attempts_query(graph_uri, inzending_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a certain inzending without succes.

    :param graph_uri: string
    :param inzending_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedSendingAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a meb:Submission .

                OPTIONAL {{ <{1}> ext:failedSendingAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, inzending_uri)
    return q


def construct_inzending_sent_query(graph_uri, inzending_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party (and thus 'sent' by us)

    :param graph_uri: string
    :param bericht_uri: URI of the bericht we would like to mark as sent.
    :param verzonden: ISO-string representation of the datetetime when the message was sent
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
        PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1}> nmo:receivedDate "{2}"^^xsd:dateTime .
            }}
        }}
        """.format(graph_uri, inzending_uri, verzonden)
    return q


def construct_create_kalliope_sync_error_query(graph_uri, poststuk_uri, message, error):
    now = datetime.now(tz=TIMEZONE).replace(microsecond=0).isoformat()
    uuid = helpers.generate_uuid()
    error_uri = "http://data.lblod.info/kalliope-sync-errors/" + uuid
    """
    Construct a SPARQL query for creating a new KalliopeSyncError

    :param graph_uri: string
    :param poststuk_uri: URI of the message that triggered an error, can be None
    :param message: string describing the error
    :param error: error catched by the exception catcher
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX pav: <http://purl.org/pav/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{5}> a ext:KalliopeSyncError ;
                    rdfs:label {2} ;
                    ext:errorMessage {3} ;
    """
    if poststuk_uri is not None:
        q += """
                    ext:processedMessage <{1}> ;
             """
    q += """
                    pav:createdOn "{4}"^^xsd:dateTime ;
                    pav:createdBy <http://lblod.data.gift/services/berichtencentrum-sync-with-kalliope-service> .
            }}
        }}
    """
    q = q.format(graph_uri,
                 poststuk_uri,
                 escape_helpers.sparql_escape_string(message),
                 escape_helpers.sparql_escape_string(error),
                 now,
                 error_uri)
    return q


def construct_dossierbehandelaar_exists_query(graph_uri, dossierbehandelaar):
    """
    Construct a query for
    selecting a conversatie based on referentieABB (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param referentieABB: string
    :returns: string containing SPARQL query
    """
    identifier = escape_helpers.sparql_escape_string(dossierbehandelaar['identifier'])

    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT DISTINCT ?dossierbehandelaar
        WHERE {{
            GRAPH <{}> {{
                ?dossierbehandelaar a prov:Association ;
                    schema:identifier {} .
            }}
        }}
        """.format(graph_uri, identifier)
    return q


def construct_insert_dossierbehandelaar_query(graph_uri, bericht):
    """
    Construct a SPARQL query for inserting a new dossierbehandelaar.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :returns: string containing SPARQL query
    """

    uuid = helpers.generate_uuid()
    bericht['dossierbehandelaar']['uuid'] = uuid
    bericht['dossierbehandelaar']['uri'] = "http://data.lblod.info/id/dossierbehandelaars/" + uuid

    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a prov:Association;
                    prov:hadRole <http://data.lblod.info/association-role/249969e6-2bfa-48c2-9a37-3f0b97685a24>;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    adms:identifier "{1[identifier]}";
                    schema:email "{1[email]}".
            }}
        }}
        """.format(graph_uri, bericht['dossierbehandelaar'])
    return q


def construct_link_dossierbehandelaar_query(graph_uri, bericht):
    """
    Construct a SPARQL query for linking a dossierbehandelaar to a bericht.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :returns: string containing SPARQL query
    """

    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> ext:heeftBehandelaar <{2[uri]}> .
            }}
        }}
        """.format(graph_uri, bericht, bericht['dossierbehandelaar'])
    return q


def construct_get_messages_by_status(status_uri, max_confirmation_attempts):
    query_str = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        SELECT DISTINCT ?bericht
                        ?uuid
                        ?verzonden
                        ?ontvangen
                        ?inhoud
                        ?van
                        ?naar
                        ?status
                        ?deliveredAt
                        ?typeCommunicatie
                        ?confirmationAttempts
                        ?g
        {{
            GRAPH ?g {{
                BIND(<{0}> as ?status)

                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?uuid;
                    schema:dateSent ?verzonden;
                    schema:dateReceived ?ontvangen;
                    schema:text ?inhoud;
                    schema:sender ?van;
                    schema:recipient ?naar;
                    adms:status ?status;
                    ext:deliveredAt ?deliveredAt;
                    <http://purl.org/dc/terms/type> ?typeCommunicatie.

                OPTIONAL {{ ?bericht ext:failedConfirmationAttempts ?confirmationAttempts. }}
            }}
        }}
    """.format(status_uri, max_confirmation_attempts)

    return query_str


def construct_update_bericht_status(bericht_uri, status_uri):
    query_str = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        DELETE {{
          GRAPH ?g {{
             ?bericht adms:status ?status.
          }}
        }}
        INSERT {{
          GRAPH ?g {{
             ?bericht adms:status <{1}>.
          }}
        }}
        WHERE {{
          BIND(<{0}> as ?bericht)

          GRAPH ?g {{
             ?bericht adms:status ?status.
          }}
        }}
    """.format(bericht_uri, status_uri)

    return query_str


def construct_increment_confirmation_attempts_query(graph_uri, poststuk_uri):
    """
    Construct a SPARQL query for incrementing (+1) the counter that keeps track of how many times
    the service attempted to send out a conformation for a certain message without succes.

    :param graph_uri: string
    :param poststuk_uri: URI of the bericht.
    :returns: string containing SPARQL query
    """

    q = """
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX schema: <http://schema.org/>

        DELETE {{
            GRAPH <{0}> {{
                <{1}> ext:failedConfirmationAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                <{1}> ext:failedConfirmationAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.

                OPTIONAL {{ <{1}> ext:failedConfirmationAttempts ?attempts. }}
                BIND(0 AS ?default_attempts)
                BIND(COALESCE(?attempts, ?default_attempts) AS ?result_attempts)
                BIND((?result_attempts + 1) AS ?incremented_attempts)
            }}
        }}
        """.format(graph_uri, poststuk_uri)

    return q
