================================================================================
  DAILP Ingest
================================================================================

This repository contains scripts for ingesting DAILP Cherokee data into the
DAILP Cherokee OLD.

Note: this repository does *not* contain any Cherokee data. This script assumes
that certain specific files are in a sister directory to ingest/ named inputs/.
If these files are not present or if their contents are not what the main
script expects them to be, then the ingest will fail.


Installation
================================================================================

Create a python (3.6, 3.7) virtual environment and install the dependencies and
dailp-ingest in development mode::

    $ python --version
    Python 3.6.0
    $ python -m venv venv
    $ source venv/bin/activate
    (venv) pip install -e .


Ingest
================================================================================

Once the dailp-ingest package is installed, you should be able to call the
``dailp-ingest`` executable::

    (venv) dailp-ingest


Configuration
--------------------------------------------------------------------------------

The ``dailp-ingest`` executable needs to know the URL and authentication
credentials of the OLD instance that you want to modify. These can be set with
the following environment variables::

    OLD_URL
    OLD_USERNAME
    OLD_PASSWORD

It can be convenient to define these environment variables in config files
under a config/ directory. For example, the following config file at
``config/chroldlocaldev`` could be used to configure ``dailp-ingest`` to target
a locally served `Docker Compose DativeBase Deploy`_::

    export OLD_URL="http://127.0.0.1:61001/chrold"
    export OLD_USERNAME="admin"
    export OLD_PASSWORD="adminA_1"

Then to load the environment::

    (venv) source config/chroldlocaldev


Next Steps
================================================================================

- TODO: remove allomorph tags from Modals

- Clean up current ingest script as per Jeff's instructions.

  - Doc with instructions:
    https://docs.google.com/document/d/1MtsPkfe7PyNo58Rps8fpYAdTdj65QtexO_BbBJVtK4M/edit

    Does this mean that the tags on the OLD forms like "pp-set:ka:",
    "pp-set:a", etc. need to be changed?

- Fix "Modal Suffixes" allomorph tags

  - QUESTION: How should I tag the allomorphs? Waiting for Jeff to respond.

- Fix "Clitics" allomorph tags

  - QUESTION: How should I tag the allomorphs? Waiting for Jeff to respond.

- Make sure inflected verbs use only the ingested affixes

- Ingest surface forms from the other tense/aspect classes in Liwen's
  spreadsheet. Get input from Jeff during or first.


- Possible alteration to "Prepronominal Prefixes (PPP)" ingest:

  - The following note may or may not be relevant when ingesting inflected
    verbs.

  - NOTE: when analyzing 'output-VERB-Uchihara-and-AllDictionaryEntries.csv',
    some tags will need modification: For the PPPs, remember that a few need
    their tags modified:

    - Tags in the PPP Column need to be modified:

          DIST -> DIST1
          PART -> PART1

      Note that these PPPs take a different form for certain categories,
      possibly requiring manual annotation:

      - Verbs specified for DIST1 select for DIST2 in PCT forms of the Imperative
      - Verbs specified for PART1 select for PART2 in INF forms
      - Also, I noticed that the tag for one of PPPs in this column is
        misspelled: TRSNL should be TRNSL (= Translocative) (I think there are
        only two instances)

- Ingest "Aspectual Suffixes--DAILP" spreadsheet. BLOCKED BY JEFF.


.. _`Docker Compose DativeBase Deploy`: https://github.com/dativebase/dativebase/blob/master/docker-compose/README.rst
