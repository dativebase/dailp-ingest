"""Ingest Cherokee Verbs (Feeling 1975) to an OLD instance.

The following credentials should be stored in environment variables:

    - OLD_URL
    - OLD_USERNAME
    - OLD_PASSWORD

Typical usage is to load the environment variables from a config file and then
supply the password variable at execute time::

    $ export OLD_URL="https://linguisticsdb.library.northeastern.edu/olds/chrolddev"
    $ export OLD_USERNAME="someusername"
    $ source config/chrolddev
    $ OLD_PASSWORD="somepassword" python ingest_verbs.py

Note that this script crucially assumes the presence of the following CSV files
under ./inputs/:

    - output-VERB-Uchihara-and-AllDictionaryEntries.csv
    - uchihara-original-cherokee-database-verb-table.csv
    - source3outmainverbs.csv
    - source3outpluralverbs.csv
    - source3outdefectiveverbs.csv

"""

from collections import namedtuple
import csv
import datetime
import os
import pprint
import re
import string
import sys

from ingest.lib.hash import sha256sum
from ingest.lib.normalize import normalize
from ingest.lib.oldclient import OLDClient
from ingest.lib.uchihara_dailp_cvtr import uchihara_original_transcription_to_dailp
from ingest.lib.utils import levenshtein_distance


DFLT_VAL = '???'

OLD_URL_DFLT = 'http://127.0.0.1:61001/chrold'
OLD_USERNAME_DFLT = 'jdunham'
OLD_PASSWORD_DFLT = 'abc123XYZ!'
DEV_MODE_DFLT = 'false'
DRY_RUN_DFLT = 'false'

OLD_URL = os.environ.get('OLD_URL', OLD_URL_DFLT)
OLD_USERNAME = os.environ.get('OLD_USERNAME', OLD_USERNAME_DFLT)
OLD_PASSWORD = os.environ.get('OLD_PASSWORD', OLD_PASSWORD_DFLT)
DEV_MODE = os.environ.get('DEV_MODE', DEV_MODE_DFLT) == 'true'
DRY_RUN = os.environ.get('DRY_RUN', DRY_RUN_DFLT) == 'true'

INPUTS_DIR = 'inputs'
VERB_FILE_NAME = 'output-VERB-Uchihara-and-AllDictionaryEntries.csv'
VERB_ORIGINAL_FILE_NAME = 'uchihara-original-cherokee-database-verb-table.csv'
VERB_SOURCE_3_FILE_NAME = 'source3outmainverbs.csv'
VERB_SOURCE_3_PLURAL_FILE_NAME = 'source3outpluralverbs.csv'
VERB_SOURCE_3_DEFECTIVE_FILE_NAME = 'source3outdefectiveverbs.csv'
PRONOMINAL_PREFIXES_FILE_NAME = 'sets-a-b-pronominal-prefixes.csv'

VERB_FILE_PATH = os.path.join(INPUTS_DIR, VERB_FILE_NAME)
VERB_ORIGINAL_FILE_PATH = os.path.join(INPUTS_DIR, VERB_ORIGINAL_FILE_NAME)
VERB_SOURCE_3_FILE_PATH = os.path.join(INPUTS_DIR, VERB_SOURCE_3_FILE_NAME)
VERB_SOURCE_3_PLURAL_FILE_PATH = os.path.join(
    INPUTS_DIR, VERB_SOURCE_3_PLURAL_FILE_NAME)
VERB_SOURCE_3_DEFECTIVE_FILE_PATH = os.path.join(
    INPUTS_DIR, VERB_SOURCE_3_DEFECTIVE_FILE_NAME)
PRONOMINAL_PREFIXES_FILE_PATH = os.path.join(
    INPUTS_DIR, PRONOMINAL_PREFIXES_FILE_NAME)


INPUTS = {
    VERB_FILE_PATH:
    'f934e003de3982c1b66b55b6d800166d13074cbfdf3a8255e065bf7c78dab09b',
    VERB_ORIGINAL_FILE_PATH:
    '5f7c3e411c684891407757112f75bd79d6bacd6c5e337c773f9e10726c2df3f2',
    VERB_SOURCE_3_FILE_PATH:
    '41440db1db5cfe84c269006e8e9d098785861a526e2350b6ee1820e7d4b85d99',
    VERB_SOURCE_3_PLURAL_FILE_PATH:
    '5cd30e0223421d9c9355c4351ecb767fb4fc0a96249c2d8a09d8e3b14371e1cb',
    VERB_SOURCE_3_DEFECTIVE_FILE_PATH:
    'ebd4f6db2fb31530a764bbab6a3bd977efd6d1d1e45592364b3a0477e04f831e',
    PRONOMINAL_PREFIXES_FILE_PATH:
    'fa48a325dfadde4c5ef905425d7267558fe1156e5d6eb664f728aa6023380935',
}


INGEST_TAG_NAMESPACE = 'ingest-uchihara-root'
NOW = datetime.datetime.now().isoformat()
INGEST_TAG_NAME = '{}:{}'.format(INGEST_TAG_NAMESPACE, NOW)


# TODO: where this is used needs fixing because it is incorrectly assuming that
# first person is always the target when "glottal stop grade present" is
# actually the target.
ENG_TO_1_REPLACEMENTS = (
    ('she\'s', 'I\'m'),
    ('she is', 'I am'),
    ('he\'s', 'I\'m'),
    ('he is', 'I am'),
    ('it\'s', 'I\'m'),
    ('it is', 'I am'),
    ('you\'re', 'I\'m'),
    ('you are', 'I am'),
    ('they\'re', 'We\'re'),
    ('they are', 'We are'),
)

ENG_TO_2_REPLACEMENTS = (
    ('she\'s', 'you\'re'),
    ('she is', 'you are'),
    ('he\'s', 'you\'re'),
    ('he is', 'you are'),
    ('it\'s', 'you\'re'),
    ('it is', 'you are'),
    ('they\'re', 'you\'re'),
    ('they are', 'you are'),
)

PRE_C_PRO_ALLOMORPHS = {
    '1SG.A': 'ci',
    '2SG.A': 'hi',
    '1DU.IN.A': 'iinii',
    '1DU.EX.A': 'oostii',
    '2DU': 'stii',
    '1PL.IN.A': 'iitii',
    '1PL.EX.A': 'oocii',
    '2PL': 'iicii',
    '1SG>AN': 'cii',
    '2SG>AN': 'hii',
    '1DU.IN>AN': 'eenii',
    '1DU.EX>AN': 'oostii',
    '2DU>AN': 'eestii',
    '1PL.IN>AN': 'eetii',
    '1PL.EX>AN': 'oocii',
    '2PL>AN': 'eecii',
    '1SG.B': 'aki',
    '2SG.B': 'ca',
    '1DU.IN.B': 'kinii',
    '1DU.EX.B': 'ookinii',
    '1PL.IN.B': 'iikii',
    '1PL.EX.B': 'ookii',
}

PRE_V_PRO_ALLOMORPHS = {
    '1SG.A': 'k',
    '2SG.A': 'h',
    '1DU.IN.A': 'iin',
    '1DU.EX.A': 'oost',
    '2DU': 'st',
    '1PL.IN.A': 'iit',
    '1PL.EX.A': 'ooc',
    '2PL': 'iic',
    '1SG>AN': 'ciiy',
    '2SG>AN': 'hiiy',
    '1DU.IN>AN': 'een',
    '1DU.EX>AN': 'oost',
    '2DU>AN': 'eest',
    '1PL.IN>AN': 'eet',
    '1PL.EX>AN': 'ooc',
    '2PL>AN': 'eec',
    '1SG.B': 'akw',
    '2SG.B': 'c',
    '1DU.IN.B': 'kin',
    '1DU.EX.B': 'ookin',
    '1PL.IN.B': 'iik',
    '1PL.EX.B': 'ook',
}

# tense class      number       index prototype-person
# PRS   hgrade     SG           [1]   3
# PRS   glottgrade SG           [2]   1
# PRS   hgrade     DU/PL        [3]   ...
# PRS   sh         SG           [4]   ...

# PRS   hgrade     SG TAG       [1] prs_prs_lparen_h_grade_rparen_colon_sg_tag: 3SG
# PRS   hgrade     SG SF        [1] prs_prs_lparen_h_grade_rparen_colon_sg_sf: a:`dade:!ga
# PRS   hgrade     SG [TAG]     [1] prs_prs_lparen_h_grade_rparen_colon_sg_lbrack_tag_rbrack
# PRS   hgrade     SG TRANS     [1] prs_prs_lparen_h_grade_rparen_colon_sg_trans
# PRS   hgrade     SG SOURCE    [1] prs_prs_lparen_h_grade_rparen_colon_sg_source: (DF1975)

# PRS   glottgrade SG TAG       [2] prs_prs_lparen_glottstop_grade_rparen_tag: 1SG
# PRS   glottgrade SG SF        [2] prs_prs_lparen_glottstop_grade_rparen_sf: gadade:!ga
# PRS   glottgrade SG [TAG]     [2] prs_prs_lparen_glottstop_grade_rparen_lbrack_tag_rbrack
# PRS   glottgrade SG TRANS     [2] prs_prs_lparen_glottstop_grade_rparen_trans
# PRS   glottgrade SG SOURCE    [2] prs_prs_lparen_glottstop_grade_rparen_source: (DF1975)

# PRS   hgrade     DU/PL TAG    [3] prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_tag
# PRS   hgrade     DU/PL SF     [3] prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_sf
# PRS   hgrade     DU/PL [TAG]  [3] prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_lbrack_tag_rbrack
# PRS   hgrade     DU/PL TRANS  [3] prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_trans
# PRS   hgrade     DU/PL SOURCE [3] prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_source

# PRS   SH         SG TAG       [4] prs_prs_plus_sh_lbrack_tag_rbrack
# PRS   SH         SG SF        [4] prs_prs_plus_sh_sf
# PRS   SH         SG [TAG]     [4] prs_prs_plus_sh_source
# PRS   SH         SG TRANS     [4] prs_prs_plus_sh_tag
# PRS   SH         SG SOURCE    [4] prs_prs_plus_sh_trans

# Maps a 3-tuple of a class, a tense and a number value to a dict from OLD
# field names to the keys needed to get the values for the key field from the
# objects extracted from the source spreadsheets.
# '1' =>  'hgrade'
SURFACE_FORM_TYPE_2_KEYS = {
    ('hgrade', 'PRS', 'SG'): {
        'narr_phon_transcr': 'prs_prs_lparen_h_grade_rparen_colon_sg_sf',
        'pro_pfx_gloss':     'prs_prs_lparen_h_grade_rparen_colon_sg_tag',
        'phon_transcr':      'simple_phonetics_lparen_with_fslash_h_fslash_plus_fslash_glottstop_fslash_rparen_1',
        'syllabary':         'source_3_3sg_prs_syllabary',
        'asp':               'prs_asp',
        'numeric':           'source_3_3sg_prs_numeric',
    },
    ('hgrade', 'PRS', 'PL'): {
        'syllabary':         'source_3_3pl_prs_syllabary',
        'numeric':           'source_3_3pl_prs_numeric',
    },
    ('glottgrade', 'PRS', 'SG'): {
        'narr_phon_transcr': 'prs_prs_lparen_glottstop_grade_rparen_sf',
        'pro_pfx_gloss':     'prs_prs_lparen_glottstop_grade_rparen_tag',
        'phon_transcr':      'simple_phonetics_lparen_with_fslash_h_fslash_plus_fslash_glottstop_fslash_rparen_2',
        'syllabary':         'source_3_1sg_prs_syllabary',
        'asp':               'prs_asp',
        'numeric':           'source_3_1sg_prs_numeric',
    },
    ('glottgrade', 'PRS', 'PL'): {
        'syllabary':         'source_3_1pl_prs_syllabary',
        'numeric':           'FIX ME I BREAK',
    },
    ('hgrade', 'PRS', 'DU/PL'): {
        'narr_phon_transcr': 'prs_prs_lparen_h_grade_rparen_colon_du_fslash_pl_sf',
        'phon_transcr':      'simple_phonetics_lparen_with_fslash_h_fslash_plus_fslash_glottstop_fslash_rparen_3',
        'syllabary':         'FIX ME I BREAK',
        'asp':               'prs_asp',
        'numeric':           'FIX ME I BREAK',
    }
}

# This is what we expect to be able to extract from the source spreadsheets
# encoding the Feeling 1975 dictionary.
# TODO: adding more rows here will control the set of surface forms that are
# extracted for each verb root.
VERB_SURFACE_FORMS = (
    ('hgrade', 'PRS', 'SG'),
    ('glottgrade', 'PRS', 'SG'),
    #('hgrade', 'PRS', 'DU/PL'),
)

# tense class      number       index
# PRS   glottgrade SG           [1]
# PRS   hgrade     SG           [2]
# PRS   hgrade     DU/PL        [3]
# PRS   sh         SG           [4]


HEADER_REPLACEABLE = (
    (' ', '_'),
    ('-', '_'),
    ('(', '_lparen_'),
    (')', '_rparen_'),
    (':', '_colon_'),
    ('/', '_fslash_'),
    ('+', '_plus_'),
    ('.', '_point_'),
    ('\'', '_quote_'),
    ('[', '_lbrack_'),
    (']', '_rbrack_'),
    ('ʔ', '_glottstop_'),
)

UNDERSCORE_PATT = re.compile('_{2,}')
WHITESPACE_REMOVER = re.compile(r'\s+')

SOURCE_3_ATTRS = (
    'verbs_source_3',
    'verbs_source_3_plural',
    'verbs_source_3_defective'
)


def clean_header(header):
    header = header.lower()
    for rplcbl, rplcmnt in HEADER_REPLACEABLE:
        header = header.replace(rplcbl, rplcmnt)
    return header


def process_header_row(row):
    headers = []
    for header in row:
        headers.append(clean_header(header))
    return headers


def get_source_3_extension(all_entries_key, state):
    for key in SOURCE_3_ATTRS:
        vs3 = state[key]
        try:
            return [o for o in vs3 if
                    o['all_entries_key'] == all_entries_key][0]
        except IndexError:
            pass
    print('WARNING: Unable to find "Source 3" match for key "{}"'.format(
        all_entries_key))
    return {}


def get_true_gloss(gloss, all_entries_key, state):
    """Find a better gloss for ``gloss`` using the "verbs_source_3_..." tables
    in ``state``.
    """
    for key in SOURCE_3_ATTRS:
        vs3 = state[key]
        try:
            source_3_obj = [o for o in vs3 if
                            o['all_entries_key'] == all_entries_key][0]
            return source_3_obj['morphemegloss']
        except IndexError:
            pass
    if len(gloss.split()) > 1:
        print('No match for {} "{}"'.format(all_entries_key, gloss))
    return gloss


def bad_spell_gloss(gloss):
    """Convert the correctly spelled ``gloss`` to an incorrectly spelled one
    that matches what is in the original Uchihara database spreadsheet.
    """
    if gloss == 'sandwich':
        return 'sandwitch'
    if gloss == 'snore':
        return 'snort'
    if gloss == 'break LG, cut LG':
        return 'break LG, vut LG'
    return gloss


def get_uchihara_original_match(gloss, root, state):
    """Attempt to match the supplied ``gloss`` value to a unique entry from the
    original Uchihara database table (in ``state``).
    """
    matches = [o for o in state['stems_glosses'] if o['gloss'] == gloss]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        candidates = [
            (levenshtein_distance(root, o['stem']), o) for o in matches]
        return candidates[0][1]
    gloss = bad_spell_gloss(gloss)
    matches = [o for o in state['stems_glosses'] if o['gloss'] == gloss]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        candidates = [
            (levenshtein_distance(root, o['stem']), o) for o in matches]
        return candidates[0][1]
    print('WARNING: Unable to match "{}" "{}" to anything in the original'
          ' Uchihara database spreadsheet.'.format(root, gloss))
    return None


def upsert_generic_tag(state, tag_name):
    """Create a tag in the OLD for ``tag_name`` only if it does not exist
    already.
    """
    tag_name = normalize(tag_name)
    known_tags = state.get('known_tags', {})
    try:
        return known_tags[tag_name]
    except KeyError:
        existing_tags = old_client.get('tags')
        try:
            tag = [t for t in existing_tags if t['name'] == tag_name][0]
            known_tags[tag_name] = tag
        except IndexError:
            tag_dict = {'name': tag_name, 'description': ''}
            tag = state['old_client'].create(
                'tags', data=tag_dict)
            print(tag)
            print('Created new tag "{}"'.format(tag_name))
            known_tags[tag_name] = tag
        state['known_tags'] = known_tags
        return tag


def extract_verb_root_tags(state, row_dict):
    """Return a list of dicts representing the tags that should be used on the
    verb root form represented by ``row_dict``. The tag is created if it does
    not already exist. If it does exist, a cached value is found and a request
    is avoided.
    """
    keys = {
        'all_entries_key': 'all-entries-key',
        'class': 'uchihara-db-class',
        'tr': 'transitivity',
        'pp': 'pp-set',
        'ppp': 'prepronominal-prefix',
    }
    tags = []
    for key, tag_namespace in keys.items():
        val = row_dict[key]
        if not val.strip():
            continue
        tag_name = '{}:{}'.format(tag_namespace, val)
        tags.append(upsert_generic_tag(state, tag_name)['id'])
    return tags


def get_verb_root_dict(row_dict, state):
    """Given a row from output-VERB-Uchihara-and-AllDictionaryEntries.numbers
    with an "ALL ENTRIES KEY" (i.e., a verb root), return a Python dict
    containing IGT data and metadata. Attributes of the dict are constructed by
    cross-referencing data from multiple input files. The following are added
    in particular:

    - ingest tag
    - tags for "ALL ENTRIES KEY", "pp-set", etc.
    - the "V" category
    - the Uchihara Database source
    """
    all_entries_key = row_dict['all_entries_key']
    root = row_dict['root_lparen_mod_rparen']
    gloss = row_dict['gloss']
    true_gloss = get_true_gloss(gloss, all_entries_key, state)
    match = get_uchihara_original_match(gloss, root, state)
    new_root = root
    if match:
        new_root = uchihara_original_transcription_to_dailp(match['stem'], root)
    tags = [state['ingest_tag']['id']] + extract_verb_root_tags(state, row_dict)
    return {
        'transcription': new_root,
        'morpheme_break': new_root,
        'morpheme_gloss': true_gloss,
        'translations': [
            {'transcription': gloss, 'grammaticality': ''},
        ],
        'syntactic_category': state['v_category']['id'],
        'source': state['uchihara_db_source']['id'],
        'tags': tags,
    }


def get_asp_sfx(class_, tense, row_dict):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['asp']
    # QUESTION_FOR_JB: is category "T" good here?
    return (uchihara_original_transcription_to_dailp(row_dict[key]), 'PRS', 'T')


def get_mod_sfx(class_, tense, row_dict, narr_phon_transcr, phon_transcr):
    """The modal suffix is deduced from shape of phonetic transcription."""
    if phon_transcr.endswith('a') or narr_phon_transcr.endswith('a'):
        mod_sfx = 'a'
        mod_sfx_gloss = 'IND'
    elif phon_transcr.endswith('i') or narr_phon_transcr.endswith('i'):
        mod_sfx = 'i'
        mod_sfx_gloss = 'MOT'
    else:
        print('WARNING: Unable to determine modal suffix for "{}" "{}"'
              ' ({}).'.format(narr_phon_transcr, phon_transcr,
                              row_dict['all_entries_key']))
        mod_sfx = '???'
        mod_sfx_gloss = '???'
    mod_sfx_cat = 'MOD'  # QUESTION_FOR_JB: is category "MOD" good here?
    return mod_sfx, mod_sfx_gloss, mod_sfx_cat


def get_morpheme_analysis(class_, tense, state, row_dict, verb_root_dict,
                          narr_phon_transcr, phon_transcr, singular):
    #pprint.pprint(row_dict)
    base = verb_root_dict['morpheme_break']
    asp_sfx, asp_sfx_gloss, asp_sfx_cat = get_asp_sfx(class_, tense, row_dict)
    mod_sfx, mod_sfx_gloss, mod_sfx_cat = get_mod_sfx(
        class_, tense, row_dict, narr_phon_transcr, phon_transcr)
    pro_pfx, pro_pfx_gloss, pro_pfx_cat = get_pro_pfx(
        class_, tense, base, narr_phon_transcr, row_dict, singular)
    state['pronominal_prefixes'].setdefault((pro_pfx, pro_pfx_gloss, pro_pfx_cat), []).append(
        {'base': base, 'phon_transcr': phon_transcr,
         'all_entries_key': row_dict['all_entries_key'],})

    morpheme_break = '{}-{}-{}-{}'.format(pro_pfx, base, asp_sfx, mod_sfx)
    if DEV_MODE:
        print('               morpheme break: {}'.format(morpheme_break))
    base_gloss = verb_root_dict['morpheme_gloss']
    morpheme_gloss = '{}-{}-{}-{}'.format(
        pro_pfx_gloss, base_gloss, asp_sfx_gloss, mod_sfx_gloss)
    if DEV_MODE:
        print('               morpheme gloss: {}'.format(morpheme_gloss))
    base_cat = 'V'
    cat_string = '{}-{}-{}-{}'.format(
        pro_pfx_cat, base_cat, asp_sfx_cat, mod_sfx_cat)
    if DEV_MODE:
        print('              category string: {}'.format(cat_string))
    return morpheme_break, morpheme_gloss, cat_string, pro_pfx_gloss


def get_present_hgrade_pro_pfx(base, narr_phon_transcr, row_dict):
    pprint.pprint(row_dict)
    print('WARNING: Not yet able to retrieve pronominal prefix for 1PRS'
          ' forms.')
    return DFLT_VAL, DFLT_VAL, DFLT_VAL


def _get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict, singular):
    """Return the pronominal prefix by examining the tense/class_-specific TAG
    column.
    """
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['pro_pfx_gloss']
    pro_pfx_gloss = row_dict[key]
    pro_pfx = uchihara_original_transcription_to_dailp(row_dict['pp']).replace(
        '-', '')
    pro_pfx_cat = 'PRO'
    A_GRP = ('Ø', 'a', 'ka', 'ka\u0301', 'kaa\u0301', 'ka\u0301a\u0301',
                  'k', 'kaa')
    KA_GRP = ('ka', 'ka\u0301', 'kaa\u0301', 'ka\u0301a\u0301', 'kaa')
    A_I_GRP = KA_GRP + ('k',)
    A_I_GRP = ('ka', 'ka\u0301', 'kaa\u0301', 'ka\u0301a\u0301', 'k', 'kaa')
    A_II_GRP = ('Ø', 'a')
    B_GRP = ('uu', 'uuw', 'anii', 'an')
    A_MIXED_GRP_1 = ('a, uu', 'a (AN); uu (INAN)')
    A_MIXED_GRP_2 = ('kaa/Ø',)
    DOT_A_APPENDABLE = ('1SG', '2SG', '3SG', '1DU.IN', '1DU.EX', '1PL.IN',
                        '1PL.EX', '3PL',)
    DOT_B_APPENDABLE = ('1SG', '2SG', '3SG', '1DU.IN', '1DU.EX', '1PL.IN',
                        '1PL.EX', '3PL',)
    if pro_pfx in A_GRP:
        if pro_pfx_gloss in DOT_A_APPENDABLE:
            pro_pfx_gloss = pro_pfx_gloss + '.A'
            if pro_pfx_gloss.startswith('3SG'):
                if pro_pfx in A_I_GRP:
                    pro_pfx_gloss = pro_pfx_gloss + '.i'
                if pro_pfx in A_II_GRP:
                    pro_pfx_gloss = pro_pfx_gloss + '.ii'
    elif pro_pfx in B_GRP:
        if pro_pfx in DOT_B_APPENDABLE:
            pro_pfx_gloss = pro_pfx_gloss + '.B'
    elif pro_pfx in A_MIXED_GRP_1:  # QUESTION_FOR_JB: how should these "mixed" cases be glossed?
        pro_pfx_orig = pro_pfx
        if pro_pfx_gloss in DOT_A_APPENDABLE:
            pro_pfx_gloss = pro_pfx_gloss + '.A'
            if pro_pfx_gloss.startswith('3SG'):
                pro_pfx_gloss = pro_pfx_gloss + '.ii'
        pro_pfx = 'a'
        # print('WARNING: Unclear pronominal prefix group "{}" for "{}"'
        #       ' ({}). Assuming it is "{}" "{}".'.format(
        #           pro_pfx_orig, narr_phon_transcr, row_dict['all_entries_key'],
        #           pro_pfx, pro_pfx_gloss))
    elif pro_pfx in A_MIXED_GRP_2:
        pro_pfx_orig = pro_pfx
        if pro_pfx_gloss == '1SG':
            pro_pfx_gloss = '1SG>AN'
        elif pro_pfx_gloss in DOT_A_APPENDABLE:
            pro_pfx_gloss = pro_pfx_gloss + '.A'
            if pro_pfx_gloss.startswith('3SG'):
                pro_pfx_gloss = pro_pfx_gloss + '.i'
        pro_pfx = 'ka'
        # print('WARNING: Unclear pronominal prefix group "{}" for "{}"'
        #       ' ({}). Assuming it is "{}" "{}".'.format(
        #           pro_pfx_orig, narr_phon_transcr, row_dict['all_entries_key'],
        #           pro_pfx, pro_pfx_gloss))
    else:
        print('WARNING: Unrecognized pronominal prefix "{}" for "{}" with verb'
              ' root/base "{}" ({})'.format(
                  pro_pfx, narr_phon_transcr, base,
                  row_dict['all_entries_key']))
        pro_pfx_gloss = '???'
        pro_pfx = '???'
        pro_pfx_gloss = '???'
    # QUESTION: what is the origin of the boolean ``singular``?
    #if not singular:
    #    pro_pfx_gloss = pro_pfx_gloss.replace('SG', 'PL')  # QUESTION_FOR_JB: are the "3PL.A" and "3PL.B" glosses correct?
    #print('CLASS {}\nPRO GLOSS {}\n\n'.format(class_, pro_pfx_gloss))
    if pro_pfx in KA_GRP:
        pro_pfx = 'ka'
    if pro_pfx_gloss.startswith(('1', '2')):
        if base.startswith(('a', 'e', 'i', 'o', 'u')):
            pro_pfx = PRE_V_PRO_ALLOMORPHS.get(pro_pfx_gloss, pro_pfx)
        else:
            pro_pfx = PRE_C_PRO_ALLOMORPHS.get(pro_pfx_gloss, pro_pfx)
    return pro_pfx, pro_pfx_gloss, pro_pfx_cat

def get_present_glottgrade_pro_pfx(base, narr_phon_transcr, row_dict, singular):
    # TODO START HERE: look into stopping using ``singular`` to modify the gloss!
    # pronominal prefix from "PP" column of spreadsheet
    key = SURFACE_FORM_TYPE_2_KEYS[('glottgrade', 'PRS', 'SG')]['pro_pfx_gloss']
    pro_pfx_gloss = row_dict[key]
    pro_pfx = uchihara_original_transcription_to_dailp(row_dict['pp']).replace(
        '-', '')
    pro_pfx_cat = 'PRO'
    A_GRP = ('Ø', 'a', 'ka', 'ka\u0301', 'kaa\u0301', 'ka\u0301a\u0301',
                  'k', 'kaa')
    B_GRP = ('uu', 'uuw', 'anii', 'an')
    A_MIXED_GRP_1 = ('a, uu', 'a (AN); uu (INAN)')
    A_MIXED_GRP_2 = ('kaa/Ø',)
    if pro_pfx in A_GRP:
        pro_pfx_gloss = pro_pfx_gloss + '.A'
    elif pro_pfx in B_GRP:
        pro_pfx_gloss = pro_pfx_gloss + '.B'
    elif pro_pfx in A_MIXED_GRP_1:  # QUESTION_FOR_JB: how should these "mixed" cases be glossed?
        pro_pfx_orig = pro_pfx
        pro_pfx_gloss = pro_pfx_gloss + '.A'
        pro_pfx = 'a'
        print('WARNING: Unclear pronominal prefix group "{}" for "{}"'
              ' ({}). Assuming it is "{}" "{}".'.format(
                  pro_pfx_orig, narr_phon_transcr, row_dict['all_entries_key'],
                  pro_pfx, pro_pfx_gloss))
    elif pro_pfx in A_MIXED_GRP_2:
        pro_pfx_orig = pro_pfx
        pro_pfx_gloss = pro_pfx_gloss + '.A'
        pro_pfx = 'kaa'
        print('WARNING: Unclear pronominal prefix group "{}" for "{}"'
              ' ({}). Assuming it is "{}" "{}".'.format(
                  pro_pfx_orig, narr_phon_transcr, row_dict['all_entries_key'],
                  pro_pfx, pro_pfx_gloss))
    else:
        print('WARNING: Unrecognized pronominal prefix "{}" for "{}" with verb'
              ' root/base "{}" ({})'.format(
                  pro_pfx, narr_phon_transcr, base,
                  row_dict['all_entries_key']))
        pro_pfx_gloss = '???'
        pro_pfx = '???'
        pro_pfx_gloss = '???'
    if not singular:
        pro_pfx_gloss = pro_pfx_gloss.replace('SG', 'PL')  # QUESTION_FOR_JB: are the "3PL.A" and "3PL.B" glosses correct?
    return pro_pfx, pro_pfx_gloss, pro_pfx_cat


def get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict, singular):
    return _get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict, singular)

    if (class_, tense) == ('glottgrade', 'PRS'):
        return get_present_glottgrade_pro_pfx(base, narr_phon_transcr, row_dict, singular)
    if (class_, tense) == ('hgrade', 'PRS'):
        return get_present_hgrade_pro_pfx(base, narr_phon_transcr, row_dict)
    print('WARNING: Not yet able to retrieve pronominal prefix for {}{}'
          ' forms.'.format(class_, tense))
    return DFLT_VAL, DFLT_VAL, DFLT_VAL


def get_comments(class_, tense, source_3_extension, row_dict):
    default_comments = 'narrow phonetic transcription source: Uchihara DB.'
    try:
        feeling_page_no = source_3_extension['df75_page_ref']
    except KeyError:
        print('WARNING: Unable to find Feeling 1975 page reference for'
              ' {}'.format(row_dict['all_entries_key']))
        return default_comments
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['numeric']
    try:
        numeric = source_3_extension[key]
    except KeyError:
        key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'PL')]['numeric']
        try:
            numeric = source_3_extension[key]
        except KeyError:
            print('WARNING: Unable to find numeric value for {}'.format(
                row_dict['all_entries_key']))
            return default_comments
    comments = ('Feeling 1975:{} ({}); '
                'narrow phonetic transcription source: '
                'Uchihara DB.'.format(feeling_page_no, numeric))
    if DEV_MODE:
        print('                     comments: {}'.format(comments))
    return comments


def construct_translation(class_, tense, source_3_extension, row_dict,
                          pro_pfx_gloss):
    translation = get_present_glottgrade_translation(
        source_3_extension, row_dict).lower()
    if pro_pfx_gloss.startswith('1'):
        replacements = ENG_TO_1_REPLACEMENTS
    elif pro_pfx_gloss.startswith('1'):
        replacements = ENG_TO_2_REPLACEMENTS
    else:
        print('WARNING: Unable to construct translation for {}{} from'
              ' "{}".'.format(class_, tense, translation))
        return 'FIXME'
    for patt, replacement in replacements:
        translation = translation.replace(patt, replacement)
    translation = translation + ' (FIXME)'
    if DEV_MODE:
        print('                  translation: \'{}\''.format(translation))
    return translation


def get_translation(source_3_extension, row_dict, class_, tense, pro_pfx_gloss):
    if (class_, tense) == ('hgrade', 'PRS'):
        return get_present_glottgrade_translation(source_3_extension, row_dict)
    return construct_translation(class_, tense, source_3_extension, row_dict,
                                 pro_pfx_gloss)


def get_present_glottgrade_translation(source_3_extension, row_dict):
    key = 'source_3_headword_translation'
    try:
        translation = source_3_extension[key]
    except KeyError:
        translation = DFLT_VAL
        print('WARNING: Unable to find translation for "{}"; using'
              ' "{}" provisionally.'.format(
                  row_dict['all_entries_key'], translation))
    if DEV_MODE:
        print('                  translation: \'{}\''.format(translation))
    return translation


def get_singularity(source_3_extension, class_, tense):
    """Returns the "singularity" of the surface form as a boolean; returns
    ``True`` if a syllabary value can be retrieved and ``False`` otherwise.
    This does not seem like a good strategy.

    QUESTION_FOR_JB: I plan to stop doing this, but can you see the original
    motivation for doing this and whether it is still convincing?
    """
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['syllabary']
    try:
        transcr = source_3_extension[key]
        return True
    except KeyError:
        return False


def get_transcr(source_3_extension, class_, tense):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['syllabary']
    try:
        transcr = source_3_extension[key]
    except KeyError:
        if DEV_MODE:
            pprint.pprint(source_3_extension)
        key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'PL')]['syllabary']
        try:
            transcr = source_3_extension[key]
        except KeyError:
            if DEV_MODE:
                pprint.pprint(source_3_extension)
            transcr = '???'
    if DEV_MODE:
        print('                transcription: {}'.format(transcr))
    return transcr


def get_narr_phon_transcr(row_dict, class_, tense):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['narr_phon_transcr']
    narr_phon_transcr = uchihara_original_transcription_to_dailp(
        row_dict[key].strip())
    if not narr_phon_transcr:
        print('WARNING: No {}{} narrow phonetic transcription for all entries'
              ' key {}. Assuming that it has no {}{} form.'.format(
            class_, tense, row_dict['all_entries_key'], class_, tense))
    if DEV_MODE:
        print('narrow phonetic transcription: {}'.format(narr_phon_transcr))
    return narr_phon_transcr


def get_phon_transcr(row_dict, class_, tense):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['phon_transcr']
    try:
        phon_transcr = row_dict[key]
    except KeyError:
        pprint.pprint(row_dict)
        raise
    if DEV_MODE:
        print('       phonetic transcription: {}'.format(phon_transcr))
    return phon_transcr


def get_form_dict(class_, tense, number, row_dict, verb_root_dict, state):
    """Return a dict for creating a form representing a surface verb form from
    Feeling 1975.
    """
    #pprint.pprint(row_dict)

    narr_phon_transcr = get_narr_phon_transcr(row_dict, class_, tense)
    if not narr_phon_transcr:  # QUESTION_FOR_JB: assuming lack of a narr phon transcr here ("PRS (h-grade): SG SF") means that there is no such form for this entry
        return None
    phon_transcr = get_phon_transcr(row_dict, class_, tense)
    source_3_extension = get_source_3_extension(
        row_dict['all_entries_key'], state)
    singular = get_singularity(source_3_extension, class_, tense)
    transcr = get_transcr(source_3_extension, class_, tense)
    morpheme_break, morpheme_gloss, cat_string, pro_pfx_gloss = (
        get_morpheme_analysis(class_, tense, state, row_dict, verb_root_dict,
                              narr_phon_transcr, phon_transcr, singular))
    translation = get_translation(source_3_extension, row_dict, class_, tense, pro_pfx_gloss)
    comments = get_comments(class_, tense, source_3_extension, row_dict)
    form_create_params = state['old_client'].form_create_params.copy()
    form_create_params.update({
        'transcription': transcr,
        'phonetic_transcription': phon_transcr,
        'narrow_phonetic_transcription': narr_phon_transcr,
        'morpheme_break': morpheme_break,
        'morpheme_gloss': morpheme_gloss,
        'translations': [{'transcription': translation, 'grammaticality': ''}],
        'comments': comments,
        'tags': [state['ingest_tag']['id']],
        # QUESTION_FOR_JB: do we want other tags for surface verb forms from
        # Feeling 1975, e.g., (from ``extract_verb_root_tags``)
        # all-entries-key, uchihara-db-class, transitivity, pp-set,
        # prepronominal-prefix,
        'source': state['feeling_source']['id'],
        'syntactic_category': state['s_category']['id'],
        # QUESTION_FOR_JB: what category should these have? S or VP or other?
    })
    return form_create_params


def get_verb_surface_forms(row_dict, verb_root_dict, state):
    surface_forms = []
    for class_, tense, number in VERB_SURFACE_FORMS:
        form_dict = get_form_dict(
            class_, tense, number, row_dict, verb_root_dict, state)
        if form_dict:
            surface_forms.append(form_dict)
    return surface_forms


def process_row_dict(row_dict, state):
    """Return a list of dicts, where each dict represents a form that should be
    added to the OLD instance. In this case, the ``verb_root_dict`` represents
    a verb root (i.e., category V), and the other dicts represent surface forms
    that contain the verb root.
    """
    verb_root_dict = get_verb_root_dict(row_dict, state)
    return [verb_root_dict] + get_verb_surface_forms(
        row_dict, verb_root_dict, state)


def row2obj(headers, row):
    return {k: v for k, v in zip(headers, row)}


def process_original_verbs_file():
    """Process the uchihara-original-cherokee-database-verb-table.csv file and
    return a list of dicts with ``'stem'`` and ``'gloss'`` keys.

    NOTE: we are only extracting data from the "STEM" and "GLOSS" columns of
    this CSV file. There are many other columns in this file that are not being
    used in this script. I believe that this decision was made because the data
    in those other columns are redundant given other input sources. We may want
    to verify this.
    """
    stems_glosses = []
    with open(VERB_ORIGINAL_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index < 2:
                continue
            stems_glosses.append({
                'stem': row[0],
                'gloss': row[1]
            })
    return stems_glosses


def process_auxiliary_verb_input_files(state):
    """Process the auxiliary verb input files and add them to the ``state``
    dict.
    """
    verbs_source_3 = process_verb_source_3_file(VERB_SOURCE_3_FILE_PATH)
    verbs_source_3_plural = process_verb_source_3_file(
        VERB_SOURCE_3_PLURAL_FILE_PATH)
    verbs_source_3_defective = process_verb_source_3_file(
        VERB_SOURCE_3_DEFECTIVE_FILE_PATH)
    stems_glosses = process_original_verbs_file()
    state.update({
        'verbs_source_3': verbs_source_3,
        'verbs_source_3_plural': verbs_source_3_plural,
        'verbs_source_3_defective': verbs_source_3_defective,
        'stems_glosses': stems_glosses,
    })
    return state


def whitespace2underscore(string_):
    return WHITESPACE_REMOVER.sub('_', string_)


def process_verb_source_3_file(file_path):
    """Return a list of dicts encoding the Verb Source 3 CSV file.

    This function converts each of the column names from this file to strings
    that could be valid Python snake_case variable names. It returns a list of
    dicts where each dict represents a row from the input file.
    """
    ret = []
    headers = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index == 0:
                headers = [whitespace2underscore(h.lower()) for h in row]
                continue
            ret.append({k: v for k, v in zip(headers, row)})
    return ret


def get_verb_table_headers():
    """The input file output-VERB-Uchihara-and-AllDictionaryEntries.csv
    contains two header rows. This function cleans and concatenates the header
    values for each column into one column header (i.e., dict key) and returns
    a list of unique header strings.
    """
    headers = []
    headers0 = None
    with open(VERB_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index == 0:
                headers0 = process_header_row(row)
            elif index == 1:
                headers1 = process_header_row(row)
                for header in zip(headers0, headers1):
                    header = '_'.join(list(filter(None, header)))
                    header = UNDERSCORE_PATT.sub('_', header).strip('_')
                    headers.append(header)
            else:
                break
    return headers


def should_process_row(row_index):
    #if DEV_MODE:
    #    return row_index in (2, 3, 4)
    return row_index > 1


def get_verb_objects(headers, state):
    verb_objects = []
    with open(VERB_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if should_process_row(index):
                row_dict = row2obj(headers, row)
                # We only care about rows that have values in the "ALL ENTRIES
                # KEY" column.
                if row_dict['all_entries_key']:
                    verb_objects += process_row_dict(row_dict, state)
    return verb_objects


def process_verbs(state):
    """Process the verbs in output-VERB-Uchihara-and-AllDictionaryEntries.csv
    and return a list of dicts.
    """
    headers = get_verb_table_headers()
    return get_verb_objects(headers, state)


def get_old_client():
    old_client = OLDClient(OLD_URL)
    assert old_client.login(OLD_USERNAME, OLD_PASSWORD)
    return old_client


def upload_verbs(verb_objects, state):
    for verb_object in verb_objects:
        form_create_params = state['old_client'].form_create_params.copy()
        form_create_params.update(verb_object)
        pprint.pprint(state['old_client'].create('forms', data=form_create_params))


def delete_all_forms(state):
    """Delete all forms on the target OLD instance.

    WARNING: only use this in development---very ad hoc.
    """
    verbs = state['old_client'].get('forms')
    for verb in verbs:
        state['old_client'].delete('forms/{}'.format(verb['id']))
        print('deleted {}'.format(verb['morpheme_break']))


def delete_all_tags(state):
    """Delete all tags on the target OLD instance.

    WARNING: only use this in development---very ad hoc.
    """
    tags = state['old_client'].get('tags')
    for tag in tags:
        print('delete {}'.format(tag['name']))
        state['old_client'].delete('tags/{}'.format(tag['id']))


def clean_up_old_instance(state):
    """Perform an initial cleanup of the OLD instance."""
    delete_all_forms(state)
    delete_all_tags(state)


def create_ingest_tag(state):
    tag_dict = {'name': INGEST_TAG_NAME, 'description': ''}
    return state['old_client'].create('tags', data=tag_dict)


def upsert_category(state, category_name):
    """Create a category with name ``category_name`` in the OLD only if it does
    not exist already.
    """
    existing_categories = old_client.get('syntacticcategories')
    try:
        return [c for c in existing_categories if c['name'] == category_name][0]
    except IndexError:
        category_dict = {
            'name': category_name,
            'description': 'Verbs',
            'type': 'lexical',
        }
        return state['old_client'].create(
            'syntacticcategories', data=category_dict)

def upsert_v_category(state):
    """Create a V category in the OLD only if it does not exist already."""
    return upsert_category(state, 'V')


def upsert_s_category(state):
    """Create a S category in the OLD only if it does not exist already."""
    return upsert_category(state, 'S')


def upsert_feeling_source(state):
    """Create a source in the OLD for Feeling 1975 only if it does not exist
    already.
    """
    KEY = 'feeling1975cherokee'
    existing_sources = old_client.get('sources')
    try:
        return [s for s in existing_sources if s['key'] == KEY][0]
    except IndexError:
        source_create_params = state['old_client'].source_create_params.copy()
        source_dict = {
            'type': 'book',
            'key': KEY,
            'title': 'Cherokee-English Dictionary',
            'author': 'Feeling, Durbin',
            'year': 1975,
            'publisher': 'Cherokee Nation of Oklahoma',
        }
        source_create_params.update(source_dict)
        return state['old_client'].create(
            'sources', data=source_create_params)


def upsert_uchihara_db_source(state):
    """Create a source in the OLD for the Uchihara Database only if it does not
    exist already.
    """
    KEY = 'uchihara2018cherokee'
    existing_sources = old_client.get('sources')
    try:
        return [s for s in existing_sources if s['key'] == KEY][0]
    except IndexError:
        source_create_params = state['old_client'].source_create_params.copy()
        source_dict = {
            'type': 'unpublished',
            'key': KEY,
            'title': 'Cherokee Database',
            'author': 'Uchihara, Hiroto',
            'note': 'Do not cite.',
            'year': 2018,
        }
        source_create_params.update(source_dict)
        return state['old_client'].create(
            'sources', data=source_create_params)


def create_auxiliary_resources(state):
    state['ingest_tag'] = create_ingest_tag(state)
    state['v_category'] = upsert_v_category(state)
    state['s_category'] = upsert_s_category(state)
    state['feeling_source'] = upsert_feeling_source(state)
    state['uchihara_db_source'] = upsert_uchihara_db_source(state)
    return state


def extract_pronominal_prefixes(state):
    ret = []
    headers = []
    with open(PRONOMINAL_PREFIXES_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index == 0:
                headers = [whitespace2underscore(h.strip().lower()) for h in row]
                continue
            ret.append({k: v for k, v in zip(headers, row)})
    return ret


class InputsChangedError(Exception):
    """Raise this if any of the input files have content that we do not
    expect.
    """


def verify_inputs():
    for file_path, known_file_hash in INPUTS.items():
        current_file_hash = sha256sum(file_path)
        if current_file_hash != known_file_hash:
            raise InputsChangedError(
                'File {} is not what we expect it to be. Expected hash {}. Got'
                ' hash {}.'.format(file_path, known_file_hash, current_file_hash))


def get_inputs_hashes():
    return {fp: sha256sum(fp) for fp in INPUTS}


def main():
    verify_inputs()
    sys.exit(0)

    # Get the OLD client and confirm credentials work.
    old_client = get_old_client()
    state = {'old_client': old_client,
             'pronominal_prefixes': {}}

    # Perform an initial cleanup of the OLD instance.
    if not DRY_RUN:
        clean_up_old_instance(state)

    pronominal_prefixes = extract_pronominal_prefixes(state)
    pprint.pprint(pronominal_prefixes)
    sys.exit(0)

    # Process the auxiliary verb input files and add them to the ``state`` dict.
    state = process_auxiliary_verb_input_files(state)

    # Create auxiliary resources (i.e., tags, categories and sources) that will
    # be needed.
    state = create_auxiliary_resources(state)

    # Get the verb roots (V) and surface forms (S)
    verb_objects = process_verbs(state)

    if not DRY_RUN:
        upload_verbs(verb_objects, state)

    for key in sorted(state['pronominal_prefixes']):
        vallist = state['pronominal_prefixes'][key]
        print('\n')
        print('{} {} {}'.format(*key))
        pprint.pprint(vallist)
        print('\n')

    for key in sorted(state['pronominal_prefixes']):
        print('{} {} {}'.format(*key))


if __name__ == '__main__':
    main()

"""


??? ??? PRO
a 2/1SG PRO
a 3SG.A.ii PRO
aki 1SG.B PRO
akw 1SG.B PRO
c 2SG.B PRO
ci 1SG.A PRO
cii 1SG>AN PRO
ciiy 1SG>AN PRO
h 2SG.A PRO
hi 2SG.A PRO
k 1SG.A PRO
k 3SG.A.i PRO
ka 2/1SG(PL?) PRO
ka 3SG.A.i PRO
uu 1SG PRO
uu 3SG PRO
uu 3SG.B PRO
uuw 1SG PRO
Ø 2/1SG PRO
Ø 3SG.A.ii PRO

Look into:

- ??? ??? PRO
- a 2/1SG PRO
- ka 2/1SG(PL?) PRO
- Ø 2/1SG PRO

Good:

- a 3SG.A.ii PRO
- Ø 3SG.A.ii PRO
- aki 1SG.B PRO
- akw 1SG.B PRO
- c 2SG.B PRO
- ci 1SG.A PRO
- k 1SG.A PRO
- cii 1SG>AN PRO
- ciiy 1SG>AN PRO
- h 2SG.A PRO
- hi 2SG.A PRO
- k 3SG.A.i PRO
- ka 3SG.A.i PRO
- uu 3SG.B PRO
"""
