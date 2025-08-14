# *-* coding: utf-8 *-*

def trim_string(s):
    """This function removes not just leading and trailing whitespace, but also whitespaces between words, keeping only single spaces."""
    if isinstance(s, str):
        return ' '.join(s.split())
    else:
        return s
