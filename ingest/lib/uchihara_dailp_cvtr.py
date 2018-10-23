import re

from .normalize import normalize, get_unicode_names


DEBUG = False


# T/TH TO D/T MAPPINGS
# Note: these are not needed for this converter but may be useful for others in
# the future.
TTH_TO_DT_MAPPINGS = (
    (re.compile('tl([^h])'), r'dl\1'),
    (re.compile('tl$'), r'dl'),
    (re.compile('tlh'), 'tl'),
    (re.compile('kw([^h])'), r'gw\1'),
    (re.compile('kw$'), r'gw'),
    (re.compile('kwh'), 'kw'),
    (re.compile('t([^hl])'), r'd\1'),
    (re.compile('t$'), r'd'),
    (re.compile('th'), 't'),
    (re.compile('k([^hw])'), r'g\1'),
    (re.compile('k$'), r'g'),
    (re.compile('kh'), 'k'),
    (re.compile('c'), 'j'),
)


UCHIHARA_TO_DAILP_MAPPING = (
    # VOWEL LENGTH MAPPINGS
    (re.compile('([aeiouv](?:\u0301)?):'), r'\1\1'),
    # WEIRD ASTERISK METATHESIS
    (re.compile(r'([nywstcl]+)\*'), r'*\1'),
    # TONE MAPPINGS:
    (re.compile(r'(.)\1!'), '\\1\u0301\\1\u0301'),
    (re.compile('(.)!'), '\\1\u0301'),
    (re.compile(r'(.)\1\*'), '\\1\\1\u0301'),
    (re.compile(r'(.)\*'), '\\1\u0301'),
    (re.compile(r'(.)\1`'), '\\1\u0300\\1\u0300'),
    (re.compile(r'(.)`'), '\\1\u0300'),
    (re.compile(r'(.)\1\^'), '\\1\u0301\\1'),
    (re.compile(r'(.)\^'), '\\1'),
    (re.compile(r'(.)\(:\)'), r'\1\1'),
)


def uchihara_original_transcription_to_dailp(original, clue=None):
    untouched_original = original
    original = normalize(original)
    if clue:
        clue = normalize(clue)
    if original == clue:
        return original
    for patt, replace in UCHIHARA_TO_DAILP_MAPPING:
        original = patt.sub(replace, original)
    original = original.replace("'", 'ʔ')
    if original != clue and DEBUG:
        print('untouched original  : {}'.format(untouched_original))
        print('transformed original: {}'.format(original))
        print('transformed original: {}'.format(get_unicode_names(original)))
        print('clue                : {}'.format(clue))
        print('\n')
    return original

"""

a:`dansi:*n(i^)
ààdansiín(i^)

hada!nv'(a^)
hadánvʔ(a^)

a:`da:sda:^yv:hv!sg(a^)
ààdaasdáayvvhv́sg(a^)

hadi:ta!sg(a^)
hadiitásg(a^)

higi!('a^)
higí(ʔa^)

hihi!'l(i^)
hihíʔl(i^)

ha(:)hne!tldi:ha^
ha(:)hnétldiiha

halihe:*li:^g(a^)
haliheélíig(a^)

halsda^yv:hv!sg(a^)
halsda^yvvhv́sg(a^)

hasu:yv!sg(a^)
hasuuyv́sg(a^)

hatl(i^)
hatl(i^)

hatv!sg(a^)
hatv́sg(a^)




"""
