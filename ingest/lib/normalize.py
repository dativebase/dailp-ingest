import unicodedata


def get_unicode_names(string_):
    """Returns a string of comma-delimited unicode character names corresponding
    to the characters in the input string_.
    """
    try:
        return ', '.join([unicodedata.name(c, '<no name>') for c in
                          string_])
    except TypeError:
        return ', '.join([unicodedata.name(str(c), '<no name>')
                          for c in string_])
    except UnicodeDecodeError:
        return string_


def get_unicode_code_points(string_):
    """Returns a string of comma-delimited unicode code points corresponding
    to the characters in the input string_.
    """
    return ', '.join(['U+%04X' % ord(c) for c in string_])


def normalize(unistr):
    """Return a unistr using canonical decompositional normalization (NFD)."""
    try:
        return unicodedata.normalize('NFD', unistr)
    except TypeError:
        return unicodedata.normalize('NFD', str(unistr))
    except UnicodeDecodeError:
        return unistr
