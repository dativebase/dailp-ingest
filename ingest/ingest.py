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
import unicodedata

from ingest.lib.hash import sha256sum
from ingest.lib.normalize import normalize, normalize_nfc
from ingest.lib.oldclient import OLDClient
from ingest.lib.uchihara_dailp_cvtr import uchihara_original_transcription_to_dailp
from ingest.lib.utils import levenshtein_distance


DFLT_VAL = '???'

OLD_URL_DFLT = 'http://127.0.0.1:61001/chrold'
OLD_USERNAME_DFLT = 'jdunham'
OLD_PASSWORD_DFLT = 'abc123XYZ!'
DEV_MODE_DFLT = 'false'
VERBOSE_DFLT = 'false'

OLD_URL = os.environ.get('OLD_URL', OLD_URL_DFLT)
OLD_USERNAME = os.environ.get('OLD_USERNAME', OLD_USERNAME_DFLT)
OLD_PASSWORD = os.environ.get('OLD_PASSWORD', OLD_PASSWORD_DFLT)
DEV_MODE = os.environ.get('DEV_MODE', DEV_MODE_DFLT) == 'true'
VERBOSE = os.environ.get('VERBOSE', VERBOSE_DFLT) == 'true'

INPUTS_DIR = 'inputs'
VERB_FILE_NAME = 'output-VERB-Uchihara-and-AllDictionaryEntries.csv'
VERB_ORIGINAL_FILE_NAME = 'uchihara-original-cherokee-database-verb-table.csv'
VERB_SOURCE_3_FILE_NAME = 'source3outmainverbs.csv'
VERB_SOURCE_3_PLURAL_FILE_NAME = 'source3outpluralverbs.csv'
VERB_SOURCE_3_DEFECTIVE_FILE_NAME = 'source3outdefectiveverbs.csv'
PRONOMINAL_PREFIXES_A_B_FILE_NAME = 'sets-a-b-pronominal-prefixes.csv'
PRONOMINAL_PREFIXES_COMBINED_FILE_NAME = 'combined-pronominal-prefixes.csv'
PREPRONOMINAL_PREFIXES_FILE_NAME = 'prepronominal-prefixes.csv'
REFLEXIVE_MIDDLE_PRONOMINAL_PREFIXES_FILE_NAME = (
    'reflexive-middle-pronominal-prefixes.csv')
MODAL_SUFFIXES_FILE_NAME = 'modal-suffixes.csv'
CLITICS_FILE_NAME = 'clitics.csv'
ORTHOGRAPHIES_FILE_NAME = 'orthographies.csv'

VERB_FILE_PATH = os.path.join(INPUTS_DIR, VERB_FILE_NAME)
VERB_ORIGINAL_FILE_PATH = os.path.join(INPUTS_DIR, VERB_ORIGINAL_FILE_NAME)
VERB_SOURCE_3_FILE_PATH = os.path.join(INPUTS_DIR, VERB_SOURCE_3_FILE_NAME)
VERB_SOURCE_3_PLURAL_FILE_PATH = os.path.join(
    INPUTS_DIR, VERB_SOURCE_3_PLURAL_FILE_NAME)
VERB_SOURCE_3_DEFECTIVE_FILE_PATH = os.path.join(
    INPUTS_DIR, VERB_SOURCE_3_DEFECTIVE_FILE_NAME)
PRONOMINAL_PREFIXES_A_B_FILE_PATH = os.path.join(
    INPUTS_DIR, PRONOMINAL_PREFIXES_A_B_FILE_NAME)
PRONOMINAL_PREFIXES_COMBINED_FILE_PATH = os.path.join(
    INPUTS_DIR, PRONOMINAL_PREFIXES_COMBINED_FILE_NAME)
PREPRONOMINAL_PREFIXES_FILE_PATH = os.path.join(
    INPUTS_DIR, PREPRONOMINAL_PREFIXES_FILE_NAME)
REFLEXIVE_MIDDLE_PRONOMINAL_PREFIXES_FILE_PATH = os.path.join(
    INPUTS_DIR, REFLEXIVE_MIDDLE_PRONOMINAL_PREFIXES_FILE_NAME)
MODAL_SUFFIXES_FILE_PATH = os.path.join(
    INPUTS_DIR, MODAL_SUFFIXES_FILE_NAME)
CLITICS_FILE_PATH = os.path.join(INPUTS_DIR, CLITICS_FILE_NAME)
ORTHOGRAPHIES_FILE_PATH = os.path.join(INPUTS_DIR, ORTHOGRAPHIES_FILE_NAME)

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
    PRONOMINAL_PREFIXES_A_B_FILE_PATH:
    'fa48a325dfadde4c5ef905425d7267558fe1156e5d6eb664f728aa6023380935',
    PRONOMINAL_PREFIXES_COMBINED_FILE_PATH:
    '9d47b6f80e52bf6024e2058e9520f9ec208fa138cbeb01b578d0ddffb586ed21',
    PREPRONOMINAL_PREFIXES_FILE_PATH:
    'da1c82fd1c7e2488ef9e85738eccc456ad16f9076dfe9639ded45e28cfe85e90',
    REFLEXIVE_MIDDLE_PRONOMINAL_PREFIXES_FILE_PATH:
    'f21c42ab6a2302506ed867656b76df56f7a919f56ea7703424e8675daf2dbc23',
    MODAL_SUFFIXES_FILE_PATH:
    'a11e472cdb1f13ef9692fe41210d4484572828837ff5ff790bdd1f487b4e5992',
    CLITICS_FILE_PATH:
    '0d552ebc91d220a25f7f4bf10d5ab23b5a9825261509f91b06d10c07d80c8ddb',
    ORTHOGRAPHIES_FILE_PATH:
    'ff6d9f7860413e33476235c1b5c381e0da61b4dcf21ea04c498a258afb385247',
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

PRE_C_PP_ALLOMORPHS = {
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

PRE_V_PP_ALLOMORPHS = {
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

PP_PRE_VOCALIC = 'pp_pre_vocalic'
PP_PRE_CONSONANTAL = 'pp_pre_consonantal'
PP_PRE_V = 'pp_pre_v'

PPP_PRE_CONSONANTAL = 'ppp_pre_consonantal'
PPP_PRE_VOCALIC = 'ppp_pre_vocalic'
PPP_ELSEWHERE = 'ppp_elsewhere'

REFL_PRE_CONSONANTAL = 'refl_pre_consonantal'
REFL_PRE_VOCALIC = 'refl_pre_vocalic'
REFL_PRE_A = 'refl_pre_a'
REFL_PRE_H_S = 'refl_pre_h_s'

MOD_PRE_CONSONANTAL = 'mod_pre_consonantal'
MOD_PRE_VOCALIC = 'mod_pre_vocalic'
MOD_PRE_V = 'mod_pre_v'

CL_PRE_CONSONANTAL = 'cl_pre_consonantal'
CL_PRE_VOCALIC = 'cl_pre_vocalic'
CL_PRE_V = 'cl_pre_v'

# Encodes mapping from prefix category to column names and the OLD tag
# identifiers that correspond to them.
#--------------+-------------+--------------+---------------+------+
# allomorph 1  | allomorph 2 |  allomorph 3 |  allomorph 4  | CAT  |
#--------------+-------------+--------------+---------------+------+
# tee          | t           |  too         |               |      |
# PPP_PRE_C    | PPP_PRE_VOC |  PPP_ELSE    |               | PPP  |
#--------------+-------------+--------------+---------------+------+
# ataat        | ataa        |  ata         |  at           | REFL |
# REFL_PRE_VOC | REFL_PRE_C  |  REFL_PRE_C  |  REFL_PRE_A   |      |
#--------------+-------------+--------------+---------------+------+
# ali          | ataa        |  ata         |  at           | MID  |
# REFL_PRE_H_S | REFL_PRE_C  |  REFL_PRE_C  |  REFL_PRE_VOC |      |
#--------------+-------------+--------------+---------------+------+
PREFIX_COL_TAG = {
    'pp_category': (
        ('allomorph_1', PP_PRE_CONSONANTAL),
        ('allomorph_2', PP_PRE_VOCALIC),
        ('allomorph_3', PP_PRE_V),),
    'ppp_category': (
        ('allomorph_1', PPP_PRE_CONSONANTAL),
        ('allomorph_2', PPP_PRE_VOCALIC),
        ('allomorph_3', PPP_ELSEWHERE),),
    'refl_category': (
        ('allomorph_1', {'REFL': REFL_PRE_VOCALIC, 'MID': REFL_PRE_H_S}),
        ('allomorph_2', REFL_PRE_CONSONANTAL),
        ('allomorph_3', REFL_PRE_CONSONANTAL),
        ('allomorph_4', {'REFL': REFL_PRE_A, 'MID': REFL_PRE_VOCALIC}),),
    'mod_category': (
        ('allomorph_1', MOD_PRE_CONSONANTAL),
        ('allomorph_2', MOD_PRE_VOCALIC),
        ('allomorph_3', MOD_PRE_V),),
    'cl_category': (
        ('allomorph_1', CL_PRE_CONSONANTAL),
        ('allomorph_2', CL_PRE_VOCALIC),
        ('allomorph_3', CL_PRE_V),),
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


ORTHOGRAPHY_SEGMENT_ORDER = (
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'short low (L)',
    'short high (H)',
    'short lowfall',
    'short superhigh',
    'long low (LL)',
    'long high (HH)',
    'rising (LH)',
    'falling (HL)',
    'lowfall (LF)',
    'superhigh (SH) = highfall',
    'any vowel',
    'unaspirated alveolar stop',
    'aspirated alveolar stop',
    'unaspirated velar stop',
    'aspirated velar stop',
    'unaspirated labiovelar stop',
    'aspirated labiovelar stop',
    'unaspirated alveolar affricate',
    'aspirated alveolar affricate',
    'lateral alveolar affricate',
    'voiceless alveolar affricate',
    'alveolar fricative',
    'glottal fricative',
    'glottal stop',
    'alveolar liquid',
    'palatal glide',
    'labiovelar glide',
    'bilabial nasal',
)


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
    state['warnings'].setdefault('Source 3 match failures', []).append(
        'Unable to find "Source 3" match for all_entries_key "{}"'.format(
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
        state['warnings'].setdefault('True gloss retrieval failure', []).append(
            'No match for {} "{}"'.format(all_entries_key, gloss))
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
    state['warnings'].setdefault('Uchihara original match failures', []).append(
        'Unable to match "{}" "{}" to anything in the original'
        ' Uchihara database spreadsheet.'.format(root, gloss))
    return None


def upsert_generic_tag(state, tag_name, tag_description=''):
    """Create a tag in the OLD for ``tag_name`` only if it does not exist
    already.
    """
    tag_name = normalize(tag_name)
    known_tags = state.get('known_tags', {})
    try:
        return known_tags[tag_name]
    except KeyError:
        existing_tags = state['old_client'].get('tags')
        try:
            tag = [t for t in existing_tags if t['name'] == tag_name][0]
            known_tags[tag_name] = tag
        except IndexError:
            tag_dict = {'name': tag_name, 'description': tag_description}
            tag = state['old_client'].create(
                'tags', data=tag_dict)
            if VERBOSE:
                print('Created new tag "{}".'.format(tag_name))
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


def get_mod_sfx(class_, tense, row_dict, narr_phon_transcr, phon_transcr,
                state):
    """The modal suffix is deduced from shape of phonetic transcription."""
    if phon_transcr.endswith('a') or narr_phon_transcr.endswith('a'):
        mod_sfx = 'a'
        mod_sfx_gloss = 'IND'
    elif phon_transcr.endswith('i') or narr_phon_transcr.endswith('i'):
        mod_sfx = 'i'
        mod_sfx_gloss = 'MOT'
    else:
        state['warnings'].setdefault(
            'Modal suffix identification failure', []).append(
                'Unable to determine modal suffix for "{}" "{}"'
                ' ({}).'.format(narr_phon_transcr, phon_transcr,
                                row_dict['all_entries_key']))
        mod_sfx = '???'
        mod_sfx_gloss = '???'
    mod_sfx_cat = 'MOD'
    return mod_sfx, mod_sfx_gloss, mod_sfx_cat


def get_morpheme_analysis(class_, tense, state, row_dict, verb_root_dict,
                          narr_phon_transcr, phon_transcr, singular):
    base = verb_root_dict['morpheme_break']
    asp_sfx, asp_sfx_gloss, asp_sfx_cat = get_asp_sfx(class_, tense, row_dict)
    mod_sfx, mod_sfx_gloss, mod_sfx_cat = get_mod_sfx(
        class_, tense, row_dict, narr_phon_transcr, phon_transcr, state)
    pro_pfx, pro_pfx_gloss, pro_pfx_cat = get_pro_pfx(
        class_, tense, base, narr_phon_transcr, row_dict, singular, state)

    """
    TODO: HERE
    state['verb_extracted_pronominal_prefixes'].setdefault(
            (pro_pfx, pro_pfx_gloss, pro_pfx_cat), []).append(
        {'base': base,
         'phon_transcr': phon_transcr,
         'all_entries_key': row_dict['all_entries_key'],})
    """

    morpheme_break = '{}-{}-{}-{}'.format(pro_pfx, base, asp_sfx, mod_sfx)
    if VERBOSE:
        print('               morpheme break: {}'.format(morpheme_break))
    base_gloss = verb_root_dict['morpheme_gloss']
    morpheme_gloss = '{}-{}-{}-{}'.format(
        pro_pfx_gloss, base_gloss, asp_sfx_gloss, mod_sfx_gloss)
    if VERBOSE:
        print('               morpheme gloss: {}'.format(morpheme_gloss))
    base_cat = 'V'
    cat_string = '{}-{}-{}-{}'.format(
        pro_pfx_cat, base_cat, asp_sfx_cat, mod_sfx_cat)
    if VERBOSE:
        print('              category string: {}'.format(cat_string))
    return morpheme_break, morpheme_gloss, cat_string, pro_pfx_gloss


def get_present_hgrade_pro_pfx(base, narr_phon_transcr, row_dict):
    if VERBOSE:
        pprint.pprint(row_dict)
    state['warnings'].setdefault(
        'Modal suffix identification failure', []).append(
            'Not yet able to retrieve pronominal prefix for 1PRS'
            ' forms.')
    return DFLT_VAL, DFLT_VAL, DFLT_VAL


def _get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict, singular,
                 state):
    """Return the pronominal prefix by examining the tense/class_-specific TAG
    column.
    """
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['pro_pfx_gloss']
    pro_pfx_gloss = row_dict[key]
    pro_pfx = uchihara_original_transcription_to_dailp(row_dict['pp']).replace(
        '-', '')
    pro_pfx_cat = 'PP'
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
        state['warnings'].setdefault(
            'Unclear pronominal prefix failure', []).append(
                'Unclear pronominal prefix group "{}" for "{}"'
                ' ({}). Assuming it is "{}" "{}".'.format(
                    pro_pfx_orig, narr_phon_transcr,
                    row_dict['all_entries_key'], pro_pfx, pro_pfx_gloss))
    elif pro_pfx in A_MIXED_GRP_2:
        pro_pfx_orig = pro_pfx
        if pro_pfx_gloss == '1SG':
            pro_pfx_gloss = '1SG>AN'
        elif pro_pfx_gloss in DOT_A_APPENDABLE:
            pro_pfx_gloss = pro_pfx_gloss + '.A'
            if pro_pfx_gloss.startswith('3SG'):
                pro_pfx_gloss = pro_pfx_gloss + '.i'
        pro_pfx = 'ka'
        state['warnings'].setdefault(
            'Unclear pronominal prefix failure', []).append(
                'Unclear pronominal prefix group "{}" for "{}"'
                ' ({}). Assuming it is "{}" "{}".'.format(
                    pro_pfx_orig, narr_phon_transcr,
                    row_dict['all_entries_key'], pro_pfx, pro_pfx_gloss))
    else:
        state['warnings'].setdefault(
            'Unrecognized pronominal prefix failure', []).append(
                'Unrecognized pronominal prefix "{}" for "{}" with verb'
                ' root/base "{}" ({})'.format(
                    pro_pfx, narr_phon_transcr, base,
                    row_dict['all_entries_key']))
        pro_pfx_gloss = '???'
        pro_pfx = '???'
        pro_pfx_gloss = '???'
    # QUESTION: what is the origin of the boolean ``singular``?
    # if not singular:
    #     pro_pfx_gloss = pro_pfx_gloss.replace('SG', 'PL')  # QUESTION_FOR_JB: are the "3PL.A" and "3PL.B" glosses correct?
    # print('CLASS {}\nPRO GLOSS {}\n\n'.format(class_, pro_pfx_gloss))
    if pro_pfx in KA_GRP:
        pro_pfx = 'ka'
    if pro_pfx_gloss.startswith(('1', '2')):
        if base.startswith(('a', 'e', 'i', 'o', 'u')):
            pro_pfx = PRE_V_PP_ALLOMORPHS.get(pro_pfx_gloss, pro_pfx)
        else:
            pro_pfx = PRE_C_PP_ALLOMORPHS.get(pro_pfx_gloss, pro_pfx)
    return pro_pfx, pro_pfx_gloss, pro_pfx_cat


def get_present_glottgrade_pro_pfx(base, narr_phon_transcr, row_dict, singular,
                                   state):
    # TODO: is this function needed, given that we have ``_get_pro_pfx`` above?
    # TODO START HERE: look into stopping using ``singular`` to modify the gloss!
    # pronominal prefix from "PP" column of spreadsheet
    key = SURFACE_FORM_TYPE_2_KEYS[('glottgrade', 'PRS', 'SG')]['pro_pfx_gloss']
    pro_pfx_gloss = row_dict[key]
    pro_pfx = uchihara_original_transcription_to_dailp(row_dict['pp']).replace(
        '-', '')
    pro_pfx_cat = 'PP'
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
        state['warnings'].setdefault(
            'Unclear pronominal prefix failure', []).append(
                'Unclear pronominal prefix group "{}" for "{}"'
                ' ({}). Assuming it is "{}" "{}".'.format(
                    pro_pfx_orig, narr_phon_transcr,
                    row_dict['all_entries_key'], pro_pfx, pro_pfx_gloss))
    elif pro_pfx in A_MIXED_GRP_2:
        pro_pfx_orig = pro_pfx
        pro_pfx_gloss = pro_pfx_gloss + '.A'
        pro_pfx = 'kaa'
        state['warnings'].setdefault(
            'Unclear pronominal prefix failure', []).append(
                'Unclear pronominal prefix group "{}" for "{}"'
                ' ({}). Assuming it is "{}" "{}".'.format(
                    pro_pfx_orig, narr_phon_transcr,
                    row_dict['all_entries_key'], pro_pfx, pro_pfx_gloss))
    else:
        state['warnings'].setdefault(
            'Unrecognized pronominal prefix failure', []).append(
                'Unrecognized pronominal prefix "{}" for "{}" with verb'
                ' root/base "{}" ({})'.format(
                    pro_pfx, narr_phon_transcr, base,
                    row_dict['all_entries_key']))
        pro_pfx_gloss = '???'
        pro_pfx = '???'
        pro_pfx_gloss = '???'
    if not singular:
        pro_pfx_gloss = pro_pfx_gloss.replace('SG', 'PL')  # QUESTION_FOR_JB: are the "3PL.A" and "3PL.B" glosses correct?
    return pro_pfx, pro_pfx_gloss, pro_pfx_cat


def get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict, singular,
                state):
    return _get_pro_pfx(class_, tense, base, narr_phon_transcr, row_dict,
                        singular, state)

    # TODO: is the following needed?
    if (class_, tense) == ('glottgrade', 'PRS'):
        return get_present_glottgrade_pro_pfx(
            base, narr_phon_transcr, row_dict, singular, state)
    if (class_, tense) == ('hgrade', 'PRS'):
        return get_present_hgrade_pro_pfx(base, narr_phon_transcr, row_dict)
    state['warnings'].setdefault(
        'Class/tense verb ingestion not yet supported', []).append(
            'Not yet able to retrieve pronominal prefix for {}{}'
            ' forms.'.format(class_, tense))
    return DFLT_VAL, DFLT_VAL, DFLT_VAL


def get_comments(class_, tense, source_3_extension, row_dict, state):
    default_comments = 'narrow phonetic transcription source: Uchihara DB.'
    try:
        feeling_page_no = source_3_extension['df75_page_ref']
    except KeyError:
        state['warnings'].setdefault(
            'Feeling 1975 page ref retrieval failure', []).append(
                'Unable to find Feeling 1975 page reference for'
                ' all_entries_key "{}".'.format(row_dict['all_entries_key']))
        return default_comments
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['numeric']
    try:
        numeric = source_3_extension[key]
    except KeyError:
        key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'PL')]['numeric']
        try:
            numeric = source_3_extension[key]
        except KeyError:
            state['warnings'].setdefault(
                'Numeric all_entries_key match failures', []).append(
                    'Unable to find numeric value for {}'.format(
                        row_dict['all_entries_key']))
            return default_comments
    comments = ('Feeling 1975:{} ({}); '
                'narrow phonetic transcription source: '
                'Uchihara DB.'.format(feeling_page_no, numeric))
    if VERBOSE:
        print('                     comments: {}'.format(comments))
    return comments


def construct_translation(class_, tense, source_3_extension, row_dict,
                          pro_pfx_gloss, state):
    translation = get_present_glottgrade_translation(
        source_3_extension, row_dict, state).lower()
    if pro_pfx_gloss.startswith('1'):
        replacements = ENG_TO_1_REPLACEMENTS
    elif pro_pfx_gloss.startswith('2'):
        replacements = ENG_TO_2_REPLACEMENTS
    else:
        state['warnings'].setdefault(
            'Translation construction failures', []).append(
                'Unable to construct translation for {}{} from original'
                ' translation "{}".'.format(class_, tense, translation))
        return 'FIXME'
    for patt, replacement in replacements:
        translation = translation.replace(patt, replacement)
    translation = translation + ' (FIXME)'
    if VERBOSE:
        print('                  translation: \'{}\''.format(translation))
    return translation


def get_translation(source_3_extension, row_dict, class_, tense, pro_pfx_gloss,
                    state):
    if (class_, tense) == ('hgrade', 'PRS'):
        return get_present_glottgrade_translation(source_3_extension, row_dict,
                                                  state)
    return construct_translation(class_, tense, source_3_extension, row_dict,
                                 pro_pfx_gloss, state)


def get_present_glottgrade_translation(source_3_extension, row_dict, state):
    key = 'source_3_headword_translation'
    try:
        translation = source_3_extension[key]
    except KeyError:
        translation = DFLT_VAL
        state['warnings'].setdefault(
            'Translation retrieval failures', []).append(
                'Unable to find translation for all_entries_key "{}"; using'
                ' "{}" provisionally.'.format(
                  row_dict['all_entries_key'], translation))
    if VERBOSE:
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
        if VERBOSE:
            pprint.pprint(source_3_extension)
        key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'PL')]['syllabary']
        try:
            transcr = source_3_extension[key]
        except KeyError:
            if VERBOSE:
                pprint.pprint(source_3_extension)
            transcr = '???'
    if VERBOSE:
        print('                transcription: {}'.format(transcr))
    return transcr


def get_narr_phon_transcr(row_dict, class_, tense, state):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['narr_phon_transcr']
    narr_phon_transcr = uchihara_original_transcription_to_dailp(
        row_dict[key].strip())
    if not narr_phon_transcr:
        state['warnings'].setdefault(
            'Narrow phonetic transcription retrieval failures', []).append(
                'No {}{} narrow phonetic transcription for all entries'
                ' key {}. Assuming that it has no {}{} form.'.format(
                    class_, tense, row_dict['all_entries_key'], class_, tense))
    if VERBOSE:
        print('narrow phonetic transcription: {}'.format(narr_phon_transcr))
    return narr_phon_transcr


def get_phon_transcr(row_dict, class_, tense):
    key = SURFACE_FORM_TYPE_2_KEYS[(class_, tense, 'SG')]['phon_transcr']
    try:
        phon_transcr = row_dict[key]
    except KeyError:
        pprint.pprint(row_dict)
        raise
    if VERBOSE:
        print('       phonetic transcription: {}'.format(phon_transcr))
    return phon_transcr


def get_form_dict(class_, tense, number, row_dict, verb_root_dict, state):
    """Return a dict for creating a form representing a surface verb form from
    Feeling 1975.
    """
    narr_phon_transcr = get_narr_phon_transcr(row_dict, class_, tense, state)
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
    translation = get_translation(source_3_extension, row_dict, class_, tense,
                                  pro_pfx_gloss, state)
    comments = get_comments(class_, tense, source_3_extension, row_dict, state)
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


def should_process_verb_row(row_index):
    if DEV_MODE:
        return row_index in (2, 3, 4)
    return row_index > 1


def get_verb_objects(headers, state):
    verb_objects = []
    with open(VERB_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if should_process_verb_row(index):
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
        resp = state['old_client'].create('forms', data=form_create_params)
        try:
            assert 'id' in resp
        except AssertionError:
            state['warnings'].setdefault(
                'Verb creation failure', []).append(
                    'Failed to create verb "{}" glossed'
                    ' "{}".'.format(verb_object['morpheme_break'],
                                    verb_object['morpheme_gloss']))


def delete_all_forms(state):
    """Delete all forms on the target OLD instance.

    WARNING: only use this in development---very ad hoc.
    """
    verbs = state['old_client'].get('forms')
    for verb in verbs:
        state['old_client'].delete('forms/{}'.format(verb['id']))
        if VERBOSE:
            print('Deleted form "{}".'.format(verb['morpheme_break']))


def delete_all_tags(state):
    """Delete all tags on the target OLD instance.

    WARNING: only use this in development---very ad hoc.
    """
    tags = state['old_client'].get('tags')
    for tag in tags:
        state['old_client'].delete('tags/{}'.format(tag['id']))
        if VERBOSE:
            print('Deleted tag "{}".'.format(tag['name']))


def clean_up_old_instance(state):
    """Perform an initial cleanup of the OLD instance."""
    delete_all_forms(state)
    delete_all_tags(state)


def create_ingest_tag(state):
    tag_dict = {'name': INGEST_TAG_NAME, 'description': ''}
    return state['old_client'].create('tags', data=tag_dict)


def upsert_category(state, category_name, description, type_='lexical'):
    """Create a category with name ``category_name`` in the OLD only if it does
    not exist already.
    """
    existing_categories = state['old_client'].get('syntacticcategories')
    try:
        return [c for c in existing_categories if c['name'] == category_name][0]
    except IndexError:
        category_dict = {
            'name': category_name,
            'description': description,
            'type': type_,
        }
        return state['old_client'].create(
            'syntacticcategories', data=category_dict)


def upsert_v_category(state):
    """Create a V category in the OLD only if it does not exist already."""
    return upsert_category(state, 'V', 'Verbs')


def upsert_s_category(state):
    """Create a S category in the OLD only if it does not exist already."""
    return upsert_category(state, 'S', 'Sentences', type_='sentential')


def upsert_pp_category(state):
    """Create a PP category in the OLD only if it does not exist already."""
    return upsert_category(state, 'PP', 'Pronominal Prefixes')


def upsert_ppp_category(state):
    """Create a PPP category in the OLD only if it does not exist already."""
    return upsert_category(state, 'PPP', 'Prepronominal Prefixes')


def upsert_refl_category(state):
    """Create a REFL category in the OLD only if it does not exist already."""
    return upsert_category(
        state, 'REFL',
        'Category for reflexive (REFL) and middle (MID) prefixes')


def upsert_mod_category(state):
    """Create a MOD category in the OLD only if it does not exist already."""
    return upsert_category(
        state, 'MOD', 'Category for modal (MOD) suffixes')


def upsert_cl_category(state):
    """Create a CL category in the OLD only if it does not exist already."""
    return upsert_category(
        state, 'CL', 'Category for clitics (CL)')


def upsert_feeling_source(state):
    """Create a source in the OLD for Feeling 1975 only if it does not exist
    already.
    """
    KEY = 'feeling1975cherokee'
    existing_sources = state['old_client'].get('sources')
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
    existing_sources = state['old_client'].get('sources')
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


def upsert_pp_tags(state):
    for attr, name, description in (
        (PP_PRE_VOCALIC, 'pp-pre-vocalic', 'Pre-vocalic pronominal prefix'),
        (PP_PRE_CONSONANTAL, 'pp-pre-consonantal',
            'Pre-consonantal pronominal prefix'),
        (PP_PRE_V, 'pp-pre-v',
            'Pre-v pronominal prefix. This tag marks the "3SG.B"'
            ' allomorph "uwa-" of pre-consonantal "uu-" (and'
            ' pre-vocalic "uw-"). It occurs before stem-initial /v-/,'
            ' which is a schwa-like vowel in Cherokee.'),):
        state[attr] = upsert_generic_tag(
            state, name, tag_description=description)
    return state


def upsert_ppp_tags(state):
    for attr, name, description in (
        (PPP_PRE_VOCALIC, 'ppp-pre-vocalic',
            'Pre-vocalic prepronominal prefix'),
        (PPP_PRE_CONSONANTAL, 'ppp-pre-consonantal',
            'Pre-consonantal prepronominal prefix'),
        (PPP_ELSEWHERE, 'ppp-elsewhere',
            'Elsewhere prepronominal prefix. Prepronominal prefix allomorphs'
            ' marked with this tag (e.g., /too/) occur before the CISL1, CISL2,'
            ' and ITER1 prepronominal prefixes.'),):
        state[attr] = upsert_generic_tag(
            state, name, tag_description=description)
    return state


def upsert_refl_tags(state):
    for attr, name, description in (
        (REFL_PRE_CONSONANTAL, 'refl-pre-consonantal',
            'Reflexive or middle prefix allomorphs before consonants.'),
        (REFL_PRE_VOCALIC, 'refl-pre-vocalic',
            'Reflexive or middle prefix allomorphs before vowels.'),
        (REFL_PRE_A, 'refl-pre-a',
            'Reflexive or middle prefix allomorphs before the vowel /a/.'),
        (REFL_PRE_H_S, 'refl-pre-h-s',
            'Reflexive or middle prefix allomorphs before the segments /h/ or'
            ' /s/.'),):
        state[attr] = upsert_generic_tag(
            state, name, tag_description=description)
    return state


def upsert_mod_tags(state):
    for attr, name, description in (
        (MOD_PRE_CONSONANTAL, 'mod-pre-consonantal',
            'Modal suffix allomorphs before consonants.'),
        (MOD_PRE_VOCALIC, 'mod-pre-vocalic',
            'Modal suffix allomorphs before vowels.'),
        (MOD_PRE_V, 'mod-pre-v',
            'Modal suffix allomorphs before the vowel /v/.'),):
        state[attr] = upsert_generic_tag(
            state, name, tag_description=description)
    return state


def upsert_cl_tags(state):
    for attr, name, description in (
        (CL_PRE_CONSONANTAL, 'cl-pre-consonantal',
            'Clitic allomorphs before consonants.'),
        (CL_PRE_VOCALIC, 'cl-pre-vocalic',
            'Clitic allomorphs before vowels.'),
        (CL_PRE_V, 'cl-pre-v',
            'Clitic allomorphs before the vowel /v/.'),):
        state[attr] = upsert_generic_tag(
            state, name, tag_description=description)
    return state


def create_auxiliary_resources(state):
    state['ingest_tag'] = create_ingest_tag(state)
    state['v_category'] = upsert_v_category(state)
    state['s_category'] = upsert_s_category(state)
    state['pp_category'] = upsert_pp_category(state)
    state['ppp_category'] = upsert_ppp_category(state)
    state['refl_category'] = upsert_refl_category(state)
    state['mod_category'] = upsert_mod_category(state)
    state['cl_category'] = upsert_cl_category(state)
    state['feeling_source'] = upsert_feeling_source(state)
    state['uchihara_db_source'] = upsert_uchihara_db_source(state)
    state = upsert_pp_tags(state)
    state = upsert_ppp_tags(state)
    state = upsert_refl_tags(state)
    state = upsert_mod_tags(state)
    state = upsert_cl_tags(state)
    return state


def extract_and_upload_orthographies(state):
    orthographies = extract_orthographies()
    upload_orthographies(state, orthographies)


def get_orthography_dicts(orthographies):
    ret = {k: [] for k, _ in orthographies[0].items() if k != 'segment'}
    for segment_dict in sorted(
            orthographies,
            key=lambda o: ORTHOGRAPHY_SEGMENT_ORDER.index(o['segment'])):
        for orth_name, graph in segment_dict.items():
            if orth_name == 'segment':
                continue
            ret[orth_name].append(graph.strip())
    ret = [{'name': k, 'orthography': ', '.join(v)} for k, v in ret.items()]
    return ret


def noncombining_length(unistr):
    return sum(1 for ch in unistr if unicodedata.combining(ch) == 0)


def get_orthography_page(orthography_dicts):
    """TODO: this needs to be changed to use simple tables instead of grid
    tables. The currently generated ReST does not render. See
    https://github.com/agda/agda/issues/2244
    """
    ret = []
    headers = (
        'segment', 'Uchihara database', 'Feeling 1975', 'TAOC, Feeling 2003',
        'CRG', 'TAOC, modified community', 'Cherokee Tone Project',
        'Cherokee Narratives', 'DAILP')
    lengths = [len(h) for h in headers]
    max_seg_len = len(max(ORTHOGRAPHY_SEGMENT_ORDER, key=len))
    lengths[0] = max_seg_len
    line_row = ['+']
    header_row = ['|']
    for index, length in enumerate(lengths):
        header = headers[index]
        line_row.append('-' * (length + 4))
        line_row.append('+')
        if index == 0:
            header_row.append(' {}{}   |'.format(header, ' ' * (max_seg_len - 7)))
        else:
            header_row.append(' {}   |'.format(header))
    line_row = ''.join(line_row)
    header_row = ''.join(header_row)
    divider_row = line_row.replace('-', '=')
    ret.append(line_row)
    ret.append(header_row)
    ret.append(divider_row)
    for index, segment_name in enumerate(ORTHOGRAPHY_SEGMENT_ORDER):
        row = ['| {}{}   |'.format(segment_name, ' ' * (max_seg_len - len(segment_name)))]
        for header in headers[1:]:
            orth_dict = [od for od in orthography_dicts if od['name'] == header][0]
            orth_list = orth_dict['orthography'].split(', ')
            orth = orth_list[index]
            row.append(' {}{}   |'.format(orth, ' ' * (len(header) - noncombining_length(orth))))
        row = ''.join(row)
        ret.append(row)
    ret.append(line_row)
    return {'content': '\n'.join(ret)}


def upload_orthographies(state, orthographies):
    """FOX HERE WORK ON ORTHOGRAPHIES
    CURRENT PROBLEM: lots of question marks in the orthgoraphies ... where they
    shouldn't be ...
    """
    print('hello')
    orthography_dicts = get_orthography_dicts(orthographies)
    pprint.pprint(orthography_dicts)
    orthography_page = get_orthography_page(orthography_dicts)
    for od in orthography_dicts:
        print(od)
        orth_create_params = state[
            'old_client'].orthography_create_params.copy()
        print('1')
        orth_create_params.update(od)
        print('2')
        resp = state['old_client'].create('orthographies', data=orth_create_params)
        print('3')
        try:
            print('4')
            assert 'id' in resp
        except AssertionError as err:
            print('5')
            print(resp)
            print(err)
            state['warnings'].setdefault(
                'Orthography creation failure', []).append(
                    'Failed to create orthography "{}". Blargon {}'.format(
                        od['name'], err))


def fix_orth(orth_dict):
    orth_dict['segment'] = orth_dict['segment'].strip(' "').replace('\n', '')
    return orth_dict


def extract_orthographies():
    ret = []
    headers = []
    with open(ORTHOGRAPHIES_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index == 0:
                headers = [h.strip().replace('\n', '') for h in row]
                continue
            ret.append({k: v for k, v in zip(headers, row)})
    return [fix_orth(d) for d in ret if fix_orth(d)['segment']]


def extract_affixes(state, file_path):
    """Extract the "Sets A & B Pronominal Prefixes" CSV file as a list of
    dicts.
    """
    ret = []
    headers = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            if index == 0:
                headers = [whitespace2underscore(h.strip().lower()) for h in row]
                continue
            ret.append({k: v for k, v in zip(headers, row)})
    return ret


def extract_combined_pronominal_prefixes(state):
    return extract_affixes(
        state, PRONOMINAL_PREFIXES_COMBINED_FILE_PATH)


def extract_refl_mid_pronominal_prefixes(state):
    return extract_affixes(
        state, REFLEXIVE_MIDDLE_PRONOMINAL_PREFIXES_FILE_PATH)


def extract_mod_suffixes(state):
    return extract_affixes(state, MODAL_SUFFIXES_FILE_PATH)


def extract_clitics(state):
    return extract_affixes(state, CLITICS_FILE_PATH)


def extract_a_b_pronominal_prefixes(state):
    return extract_affixes(state, PRONOMINAL_PREFIXES_A_B_FILE_PATH)


def extract_prepronominal_prefixes(state):
    return extract_affixes(state, PREPRONOMINAL_PREFIXES_FILE_PATH)


def split_affix_dict(affix, state, syntactic_category_key='pp_category'):
    """Split the supplied dict ``affix`` into a list of dicts, one
    for each allomorph. Each dict in the returned list is also modified to be
    compatible with the OLD's form resource data structure.
    """
    ret = []
    morpheme_gloss = affix['tag']
    if morpheme_gloss in ('NEG1', 'NEG2'):
        state['warnings'].setdefault(
            'Negative PPP omission warnings', []).append(
                'Omitting ingestion of NEG morpheme with first allomorph "{}"'
                ' glossed as "{}".'.format(
                    affix['allomorph_1'], morpheme_gloss))
        return ret
    translations = [{'transcription': affix['morpheme_name'],
                     'grammaticality': ''}]
    syntactic_category = state[syntactic_category_key]['id']
    comments_getter = {'pp_category': get_comments_pp,
                       'ppp_category': get_comments_ppp,
                       'refl_category': get_comments_refl,
                       'mod_category': get_comments_mod,
                       'cl_category': get_comments_cl,}
    comments = comments_getter[syntactic_category_key](affix)
    col_tag = PREFIX_COL_TAG[syntactic_category_key]
    for allomorph_attr, allomorph_tag_state_attr in col_tag:
        if isinstance(allomorph_tag_state_attr, dict):
            allomorph_tag_state_attr = allomorph_tag_state_attr[morpheme_gloss]
        allomorph = affix.get(allomorph_attr, '').strip()
        if not allomorph:
            continue
        ret.append({
            'transcription': allomorph,
            'morpheme_break': allomorph,
            'morpheme_gloss': morpheme_gloss,
            'translations': translations,
            'syntactic_category': syntactic_category,
            'comments': comments,
            'tags': [state[allomorph_tag_state_attr]['id'],
                     state['ingest_tag']['id']],
        })
    return ret


def split_prepro_pre_dict(pronominal_prefix, state):
    return split_affix_dict(
        pronominal_prefix, state, syntactic_category_key='ppp_category')


def split_refl_mid_pre_dict(pronominal_prefix, state):
    return split_affix_dict(
        pronominal_prefix, state, syntactic_category_key='refl_category')


def split_mod_suf_dict(suffix, state):
    return split_affix_dict(
        suffix, state, syntactic_category_key='mod_category')


def split_clitic_dict(clitic, state):
    return split_affix_dict(
        clitic, state, syntactic_category_key='cl_category')


def get_comments_pp(pronominal_prefix):
    """Get a string of comments for a pronominal prefix from either of the
    "Sets A & B Pronominal Prefixes" or "Combined Pronominal Prefixes" input
    sources.
    """
    comments_attrs = (
        ('prefix_series', 'prefix series'),
        ('laryngeal_alternation', 'laryngeal alternation'),
        ('taoc', 'TAOC'),
        ('crg', 'CRG'),
        ('crg_form', 'CRG form'),
        ('crg_tag', 'CRG tag'),
        ('bma_2008', 'BMA 2008'),
        ('bma_2008_form', 'BMA 2008 form'),
    )
    ret = []
    for attr, name in comments_attrs:
        val = pronominal_prefix.get(attr, '').strip()
        if val:
            ret.append('{}: {}.'.format(name, val))
    return ' '.join(ret)


def get_comments_refl(pronominal_prefix):
    """Get a string of comments for a reflexive/middle prefix."""
    comments_attrs = (
        ('prefix_series', 'prefix series'),
        ('laryngeal_alternation', 'laryngeal alternation'),
        ('taoc', 'TAOC'),
        ('crg', 'CRG'),
        ('crg_form', 'CRG form'),
        ('crg_tag', 'CRG tag'),
        ('bma_2008', 'BMA 2008'),
        ('bma_2008_form', 'BMA 2008 form'),
    )
    ret = []
    for attr, name in comments_attrs:
        val = pronominal_prefix.get(attr, '').strip()
        if val:
            ret.append('{}: {}.'.format(name, val))
    return ' '.join(ret)


def get_comments_mod(mod_suffix):
    """Get a string of comments for a modal suffix."""
    comments_attrs = (
        ('taoc', 'TAOC'),
        ('crg', 'CRG'),
        ('crg_form', 'CRG form'),
        ('crg_tag', 'CRG tag'),
        ('crg_morpheme_name', 'CRG morpheme name'),
        ('bma_2008', 'BMA 2008'),
        ('bma_2008_form', 'BMA 2008 form'),
        ('bma_2008_tag', 'BMA 2008 tag'),
        ('bma_2008_morpheme_name', 'BMA 2008 morpheme name'),
    )
    ret = []
    for attr, name in comments_attrs:
        val = mod_suffix.get(attr, '').strip()
        if val:
            ret.append('{}: {}.'.format(name, val))
    return ' '.join(ret)


def get_comments_cl(clitic):
    """Get a string of comments for a clitic."""
    comments_attrs = (
        ('crg', 'CRG'),
        ('crg_form', 'CRG form'),
        ('crg_tag', 'CRG tag'),
        ('crg_morpheme_name', 'CRG morpheme name'),
        ('bma_2008', 'BMA 2008'),
        ('bma_2008_form', 'BMA 2008 form'),
        ('bma_2008_tag', 'BMA 2008 tag'),
        ('bma_2008_morpheme_name', 'BMA 2008 morpheme name'),
    )
    ret = []
    for attr, name in comments_attrs:
        val = clitic.get(attr, '').strip()
        if val:
            ret.append('{}: {}.'.format(name, val))
    return ' '.join(ret)


def get_comments_ppp(pronominal_prefix):
    """Get a string of comments for a prepronominal prefix from."""
    comments_attrs = (
        ('h3_specification', 'H3 specification'),
        ('tonicity', 'tonicity'),
        ('taoc', 'TAOC'),
        ('taoc_tag', 'TAOC tag'),
    )
    to_many_attrs = {
        'crg': 'CRG',
        'bma_2008': 'BMA 2008',
    }
    ret = []
    first_to_manys = {}
    final_to_manys = []
    for attr, name in comments_attrs:
        val = pronominal_prefix.get(attr, '').strip()
        if val:
            ret.append('{}: {}.'.format(name, val))
    for key, val in pronominal_prefix.items():
        val = val.strip()
        if not val:
            continue
        try:
            entity, index, attr = key.split('.')
        except ValueError as err:
            continue
        true_index = int(index)
        human_index = str(true_index + 1)
        attr = attr.replace('_', ' ')
        if attr == 'pp':
            attr = 'pp.'
        entities = first_to_manys.setdefault(entity, {})
        entity_list = entities.setdefault(human_index, [])
        entity_list.append('{} {}'.format(attr, val))
    for key, manys in first_to_manys.items():
        key = to_many_attrs[key]
        for index, sentence_parts in manys.items():
            subkey = '{} reference {}'.format(key, index)
            subval = ', '.join(sentence_parts)
            final_to_manys.append('{}: {}.'.format(subkey, subval))
    return ' '.join(ret + sorted(final_to_manys))


def upload_affixes(affixes, state, processor_func):
    """Upload the supplied list of dicts ``affixes`` to the target
    OLD as form resources.
    """
    for affix in affixes:
        for pro_pre in processor_func(affix, state):
            form_create_params = state['old_client'].form_create_params.copy()
            form_create_params.update(pro_pre)
            resp = state['old_client'].create('forms', data=form_create_params)
            try:
                assert 'id' in resp
                state.setdefault('created_pronominal_prefixes', []).append(resp)
            except AssertionError:
                state['warnings'].setdefault(
                    'Pronominal prefix creation failure', []).append(
                        'Failed to create pronominal prefix "{}" glossed'
                        ' "{}".'.format(pro_pre['morpheme_break'],
                                        pro_pre['morpheme_gloss']))


class InputsChangedError(Exception):
    """Raise this if any of the input files have content that we do not
    expect.
    """


def verify_inputs():
    """Confirm that the inputs have not changed from what we expect them to
    be.
    """
    for file_path, known_file_hash in INPUTS.items():
        current_file_hash = sha256sum(file_path)
        if current_file_hash != known_file_hash:
            raise InputsChangedError(
                'File {} is not what we expect it to be. Expected hash {}. Got'
                ' hash {}.'.format(file_path, known_file_hash,
                                   current_file_hash))


def get_inputs_hashes():
    return {fp: sha256sum(fp) for fp in INPUTS}


def print_warnings(state):
    for warning_type, warning_list in state['warnings'].items():
        print('\n')
        print(warning_type)
        print('=' * 80)
        print('\n')
        for warning in warning_list:
            print('- {}'.format(warning))
        print('\n')


def extract_and_upload_prefixes(state):

    # PPP. Get and upload the set A & B pronominal prefixes (PP).
    a_b_pronominal_prefixes = extract_a_b_pronominal_prefixes(state)
    upload_affixes(
        a_b_pronominal_prefixes, state, split_affix_dict)

    # PPP. Get and upload the combined pronominal prefixes (PP).
    combined_pronominal_prefixes = extract_combined_pronominal_prefixes(state)
    upload_affixes(
        combined_pronominal_prefixes, state, split_affix_dict)

    # PPP. Get and upload the prepronominal prefixes (PPP).
    prepronominal_prefixes = extract_prepronominal_prefixes(state)
    upload_affixes(
        prepronominal_prefixes, state, split_prepro_pre_dict)

    # REFL. Get and upload the reflexive and middle pronominal prefixes
    # (REFL/MID).
    refl_mid_pronominal_prefixes = extract_refl_mid_pronominal_prefixes(state)
    upload_affixes(
        refl_mid_pronominal_prefixes, state, split_refl_mid_pre_dict)


def extract_and_upload_suffixes(state):

    # MOD. Get and upload the modal suffixes (MOD).
    mod_suffixes = extract_mod_suffixes(state)
    upload_affixes(
        mod_suffixes, state, split_mod_suf_dict)

    # CL. Get and upload the clitics (CL).
    clitics = extract_clitics(state)
    upload_affixes(clitics, state, split_clitic_dict)


def extract_and_upload_affixes(state):
    extract_and_upload_prefixes(state)
    extract_and_upload_suffixes(state)


def main():
    # Confirm that the inputs have not changed
    verify_inputs()

    # Get the OLD client and confirm credentials work.
    old_client = get_old_client()
    state = {'old_client': old_client,
             'created_pronominal_prefixes': [],
             'warnings': {},}

    extract_and_upload_orthographies(state)
    print_warnings(state)
    sys.exit(0)

    # Perform an initial cleanup of the OLD instance.
    clean_up_old_instance(state)

    # Process the auxiliary verb input files and add them to the ``state`` dict.
    state = process_auxiliary_verb_input_files(state)

    # Create auxiliary resources (i.e., tags, categories and sources) that will
    # be needed.
    state = create_auxiliary_resources(state)

    # Upload all of the affixes (prefixes, suffixes and enclitics)
    extract_and_upload_affixes(state)

    # Get the verb roots (V) and surface forms (S)
    verb_objects = process_verbs(state)
    upload_verbs(verb_objects, state)

    print_warnings(state)


if __name__ == '__main__':
    main()
