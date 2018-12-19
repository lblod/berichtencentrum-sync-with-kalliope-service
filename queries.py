#!/usr/bin/python3

def construct_conversatie_exists_query(graph_uri, dossiernummer):
    """
    Construct a query for selecting a conversatie based on dossiernummer (thereby also testing if the conversatie already exists)

    :param graph_uri: string
    :param dossiernummer: string
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?conversatie
        WHERE {{
            GRAPH <{}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier "{}".
            }}
        }}
        """.format(graph_uri, dossiernummer)
    return q

def construct_insert_conversatie_query(graph_uri, conversatie, bericht):
    """
    Construct a SPARQL query for inserting a new conversatie with a first bericht attached.

    :param graph_uri: string
    :param conversatie: dict containing escaped properties for conversatie
    :param bericht: dict containing escaped properties for bericht
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT {{
            GRAPH <{0}> {{
                ?conversatie a schema:Conversation;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:identifier "{1[dossiernummer]}";
                    schema:about "{1[betreft]}";
                    <http://purl.org/dc/terms/type> "{1[type_communicatie]}";
                    schema:processingTime "{1[reactietermijn]}";
                    schema:hasPart ?bericht.

                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{2[uuid]}";
                    schema:dateSent "{2[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{2[ontvangen]}"^^xsd:dateTime;
                    schema:text \"\"\"{2[inhoud]}\"\"\";
                    schema:sender <{2[van]}>;
                    schema:recipient <{2[naar]}>.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                BIND(IRI(CONCAT("http://data.lblod.info/id/conversaties/", "{1[uuid]}")) AS ?conversatie)
                BIND(IRI(CONCAT("http://data.lblod.info/id/berichten/", "{2[uuid]}")) AS ?bericht)
            }}
        }}
        """.format(graph_uri, conversatie, bericht)
    return q

def construct_insert_bericht_query(graph_uri, bericht, conversatie_uri):
    """
    Construct a SPARQL query for inserting a bericht and attaching it to an existing conversatie.

    :param graph_uri: string
    :param bericht: dict containing escaped properties for bericht
    :param conversatie_uri: string containing the uri of the conversatie that the bericht has to get attached to
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT DATA {{
            GRAPH <{0}> {{
                <{2}> a schema:Conversation;
                    schema:hasPart ?bericht.
                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> "{1[uuid]}";
                    schema:dateSent "{1[verzonden]}"^^xsd:dateTime;
                    schema:dateReceived "{1[ontvangen]}"^^xsd::dateTime;
                    schema:text \"\"\"{1[inhoud]}\"\"\";
                    schema:sender <{1[van]}>;
                    schema:recipient <{1[naar]}>.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                BIND(IRI(CONCAT("http://data.lblod.info/id/berichten/", "{1[uuid]}")) AS ?bericht)
            }}
        }}
        """.format(graph_uri, bericht, conversatie_uri)
    return q

def construct_unsent_berichten_query(graph_uri, naar_uri):
    """
    Construct a SPARQL query for retrieving all messages for a given recipient that haven't been received yet by the other party.

    :param graph_uri: string
    :param naar_uri: URI of the recpient for which we want to retrieve messages that have yet to be sent.
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?dossiernummer ?bericht ?uuid ?van ?verzonden ?inhoud
        WHERE {{
            GRAPH <{0}> {{
                ?conversatie a schema:Conversation;
                    schema:identifier ?dossiernummer;
                    schema:hasPart ?bericht.
                ?bericht a schema:Message;
                    <http://mu.semte.ch/vocabularies/core/uuid> ?uuid;
                    schema:dateSent ?verzonden;
                    schema:text ?inhoud;
                    schema:sender ?van;
                    schema:recipient ?naar.
                BIND(IRI("{1}") AS ?naar)
                FILTER NOT EXISTS {{?bericht schema:dateReceived ?ontvangen.}} #Bericht hasn't been received yet, this means we have yet to send it to the other party
            }}
        }}
        """.format(graph_uri, naar_uri)
    return q

def construct_bericht_sent_query(graph_uri, bericht_uri, verzonden):
    """
    Construct a SPARQL query for marking a bericht as received by the other party.

    :param graph_uri: string
    :param bericht_uri: URI of the bericht we would like to mark as sent.
    :param verzonden: ISO-string representation of the datetetime when the message was sent
    :returns: string containing SPARQL query
    """
    q = """
        PREFIX schema: <http://schema.org/>

        INSERT {{
            GRAPH <{0}> {{
                ?bericht schema:dateReceived "{2}"^^xsd:dateTime.
            }}
        }}
        WHERE {{
            GRAPH <{0}> {{
                ?bericht a schema:Message;
                BIND(IRI("{1}") AS ?bericht)
            }}
        }}
        """.format(graph_uri, bericht_uri, verzonden)
    return q
