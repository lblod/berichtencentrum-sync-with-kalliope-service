from sudo_query_helpers import update
from helpers import log


def update_with_suppressed_fail(query_string):
    """
    A last resort update, if the potential error of an update needs supression.

    :param query_string: string
    """
    try:
        update(query_string)
    except Exception as e:
        log("""
              WARNING: an error occured during the update_with_suppressed_fail.
                       Message {}
                       Query {}
            """.format(e, query_string))
        log("""WARNING: I am sorry you have to read this message""")
