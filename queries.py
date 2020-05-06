#!/usr/bin/python3
import copy
import escape_helpers

# TODO Remove this escape method and use the mu one once PR has been merged: https://github.com/MikiDi/mu-python-template/pull/2
import re
def sparql_escape_string(obj):
    obj = str(obj)
    def replacer(a):
        return "\\"+a.group(0)
    return '"""' + re.sub(r'[\\\'"]', replacer, obj) + '"""'


def construct_conversatie_exists_query(graph_uri, dossiernummer):
    """
    Construct a query for selecting a conversatie based on dossiernummer (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param dossiernummer: string
    :returns: string containing SPARQL query
    """
    dossiernummer = escape_helpers.sparql_escape_string(dossiernummer)
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie
        WHERE {{
            GRAPH <{}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier {}.
            }}
        }}
        """.format(graph_uri, dossiernummer)
    return q

def construct_bericht_exists_query(graph_uri, bericht_uri):
    """
    Construct a query for selecting a bericht based on its URI, retrieving the conversatie & dossiernummer at the same time.

    :param graph_uri: string
    :param bericht_uri: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie ?dossiernummer
        WHERE {{
            GRAPH <{0}> {{
                <{1}> a schema:Message.
                ?conversatie a schema:Conversation;
                    schema:hasPart <{1}>;
                    schema:identifier ?dossiernummer.
            }}
        }}
        """.format(graph_uri, bericht_uri)
    return q

def construct_insert_conversatie_query(graph_uri, conversatie, bericht):
    """
    Construct a SPARQL query for inserting a new conversatie with a first bericht attached.

    :param graph_uri: string
    :param conversatie: dict containing escaped properties for conversatie
    :param bericht: dict containing escaped properties for bericht
    :returns: string containing SPARQL query
    """
    conversatie = copy.deepcopy(conversatie) # For not modifying the pass-by-name original
    conversatie['dossiernummer'] = escape_helpers.sparql_escape_string(conversatie['dossiernummer'])
    conversatie['betreft'] = escape_helpers.sparql_escape_string(conversatie['betreft'])
    conversatie['current_type_communicatie'] = escape_helpers.sparql_escape_string(conversatie['current_type_communicatie'])
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{1[uri]}> a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier {1[dossiernummer]};
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
                    schema:recipient <{2[naar]}>.
            }}
        }}
    """
    q = q.format(graph_uri, conversatie, bericht)
    return q

def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :returns: string containing SPARQL query
    """
    bericht = copy.deepcopy(bericht) # For not modifying the pass-by-name original
    bericht['inhoud'] = sparql_escape_string(bericht['inhoud'])
    q = """
        PREFIX schema: <http://schema.org/>

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
                    <http://purl.org/dc/terms/type> "{1[type_communicatie]}".
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri)
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

def construct_insert_bijlage_query(bericht_graph_uri, bijlage_graph_uri, bericht_uri, bijlage, file):
    """
    Construct a SPARQL query for inserting a bijlage and attaching it to an existing bericht.

    :param bericht_graph_uri: string
    :param bijlage_graph_uri: string
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
                <{2}> nie:hasPart <{3[uri]}>.
            }}
            GRAPH <{1}> {{
                <{3[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{3[uuid]}";
                    nfo:fileName {3[name]};
                    dct:format {3[mimetype]};
                    dct:created "{3[created]}"^^xsd:dateTime;
                    nfo:fileSize "{3[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{3[extension]}".
                <{4[uri]}> a nfo:FileDataObject;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{4[uuid]}";
                    nfo:fileName {4[name]};
                    dct:format {3[mimetype]};
                    dct:created "{3[created]}"^^xsd:dateTime;
                    nfo:fileSize "{3[size]}"^^xsd:integer;
                    dbpedia:fileExtension "{3[extension]}";
                    nie:dataSource <{3[uri]}>.
            }}
        }}
        """.format(bericht_graph_uri, bijlage_graph_uri, bericht_uri, bijlage, file)
    return q

def construct_update_last_bericht_query_part1():
    """
    Construct a SPARQL query for keeping the ext:lastMessage of each conversation up to date.
    Part 1/2 (query constructed in 2 parts because Virtuoso)

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        DELETE {
            GRAPH ?g {
                ?conversation ext:lastMessage ?message.
            }
        }
        WHERE {
            GRAPH ?g {
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage;
                    ext:lastMessage ?message.
            }
            {
                SELECT (?message AS ?newMessage) WHERE {
                    GRAPH ?g {
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent < ?otherDateSent  )
                        }
                    }
                }
            }
        }
        """
    return q

def construct_update_last_bericht_query_part2():
    """
    Construct a SPARQL query for keeping the ext:lastMessage of each conversation up to date.
    Part 2/2 (query constructed in 2 parts because Virtuoso)

    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

        INSERT {
            GRAPH ?g {
                ?conversation ext:lastMessage ?newMessage.
            }
        }
        WHERE {
            GRAPH ?g {
                ?conversation a schema:Conversation;
                    schema:hasPart ?newMessage.
            }
            {
                SELECT (?message AS ?newMessage) WHERE {
                    GRAPH ?g {
                        ?conversation a schema:Conversation;
                            schema:hasPart ?message.
                        ?message schema:dateSent ?dateSent.
                        FILTER NOT EXISTS {
                            ?conversation schema:hasPart/schema:dateSent ?otherDateSent.
                            FILTER( ?dateSent < ?otherDateSent  )
                        }
                    }
                }
            }
        }
        """
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

        SELECT DISTINCT ?dossiernummer ?dossieruri ?bericht ?betreft ?uuid ?van ?verzonden ?inhoud
        WHERE {{
            GRAPH ?g {{
                ?conversatie a schema:Conversation;
                    schema:identifier ?dossiernummer;
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

def construct_select_bijlagen_query(bijlagen_graph_uri, bericht_uri):
    """
    Construct a SPARQL query for retrieving all bijlages for a given bericht.

    :param bijlagen_graph_uri: string, graph where file information is stored
    :param bericht_uri: URI of the bericht for which we want to retrieve bijlagen.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT ?bijlagenaam ?file ?type WHERE {{
            GRAPH ?g {{
                <{1}> a schema:Message;
                    nie:hasPart ?bijlage.
            }}
            GRAPH <{0}> {{
                ?bijlage a nfo:FileDataObject;
                    nfo:fileName ?bijlagenaam;
                    dct:format ?type.
                ?file nie:dataSource ?bijlage.
            }}
        }}
        """.format(bijlagen_graph_uri, bericht_uri)
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
        '<https://data.vlaanderen.be/id/concept/BesluitType/6af621e2-c807-479e-a6f2-2d64d8339491>'
    ]
    separator = ', '

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

        SELECT DISTINCT ?inzending ?inzendingUuid ?bestuurseenheid ?decisionType ?sessionDate ?decisionTypeLabel
        WHERE {{
            GRAPH ?g {{
                ?inzending a meb:Submission ;
                    adms:status <http://lblod.data.gift/concepts/9bd8d86d-bb10-4456-a84e-91e9507c374c> ;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?inzendingUuid ;
                    <http://purl.org/pav/createdBy> ?bestuurseenheid;
                    prov:generated ?formData .

                ?formData dct:type ?decisionType .

                FILTER ( ?decisionType IN ( {1} ) )

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
                ?submission ext:failedSendingAttempts ?result_attempts.
            }}
        }}
        INSERT {{
            GRAPH <{0}> {{
                ?submission ext:failedSendingAttempts ?incremented_attempts.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                ?submission a meb:Submission ;
                    prov:generated <{1}> .

                OPTIONAL {{ ?submission ext:failedSendingAttempts ?attempts. }}
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

        INSERT {{
            GRAPH <{0}> {{
                ?submission nmo:receivedDate "{2}"^^xsd:dateTime .
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                ?submission a meb:Submission ;
                    prov:generated <{1}> .
            }}
        }}
        """.format(graph_uri, inzending_uri, verzonden)
    return q
