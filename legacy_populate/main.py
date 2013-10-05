# -*- coding: utf-8 -*-
import os
import argparse
from io import BytesIO
import lxml.html
import psycopg2
import requests

from legacy_populate import parsers


MODULE = 'Module'
COLLECTION = 'Collection'
TYPES_TO_PARSERS = {
    MODULE: parsers.parse_module_xml,
    COLLECTION: parsers.parse_collection_xml,
    }
TYPES_TO_FILENAMES = {
    MODULE: 'index.cnxml',
    COLLECTION: 'collection.xml',
    }


def id_to_type(id):
    if id.startswith('m'):
        type = MODULE
    elif id.startswith('c'):
        type = COLLECTION
    else:
        raise ValueError("invalid id: {}".format(id))
    return type


def type_to_filename(type):
    return TYPES_TO_FILENAMES[type]


def parse_to_metadata(type, document):
    """Parse the document to metadata."""
    parser = TYPES_TO_PARSERS[type]
    parsed_item_keys = ['abstract', 'license_url', 'metadata',
                        'keywords', 'subjects',
                        ]
    return dict(zip(parsed_item_keys, parser(BytesIO(document))))


class Resolver:
    """utility for source resolution about a piece of content."""

    def __init__(self, host):
        self.host = host

    def to_url(self, mid, version='latest'):
        return "http://{}/content/{}/{}".format(self.host, mid, version)

    def to_source_url(self, mid, version):
        return "{}/source".format(self.to_url(mid, version))

    def __call__(self, mid):
        for version in self.get_versions(mid):
            resp = requests.get(self.to_source_url(mid, version))
            document = resp.content
            metadata = parse_to_metadata(id_to_type(mid), document)
            yield metadata, document
        raise StopIteration

    def get_versions(self, mid):
        """Parse the html document to find the versions for this module."""
        resp = requests.get("{}/content_info".format(self.to_url(mid)))
        doc = lxml.html.parse(BytesIO(resp.content))
        xpath_exp = '//div[@id="cnx_history_section"]//a[@class="cnxn"]/text()'
        return doc.xpath(xpath_exp)



def resolve_to_content(host, mid):
    """resolve the given id to content.
    Iterate the content by version in ascending order.
    If a collection is given, the connected modules will not be resolved.
    """

def _insert_abstract(abstract_text, cursor):
    """insert the abstract"""
    cursor.execute("INSERT INTO abstracts (abstract) "
                   "VALUES (%s) "
                   "RETURNING abstractid;", (abstract_text,))
    id = cursor.fetchone()[0]
    return id
def _find_license_id_by_url(url, cursor):
    cursor.execute("SELECT licenseid FROM licenses "
                   "WHERE url = %s;", (url,))
    id = cursor.fetchone()[0]
    return id
def _insert_module(metadata, cursor):
    metadata = metadata.items()
    metadata_keys = ', '.join([x for x, y in metadata])
    metadata_value_spaces = ', '.join(['%s'] * len(metadata))
    metadata_values = [y for x, y in metadata]
    cursor.execute("INSERT INTO modules  ({}) "
                   "VALUES ({}) "
                   "RETURNING module_ident, portal_type;".format(
            metadata_keys,
            metadata_value_spaces),
                   metadata_values)
    id, type = cursor.fetchone()
    return id, type
def _insert_module_file(module_id, filename, mimetype, file, cursor):
    try:
        file = file.read()
    except AttributeError:
        pass
    payload = (psycopg2.Binary(file),)
    cursor.execute("INSERT INTO files (file) VALUES (%s) "
                   "RETURNING fileid;", payload)
    file_id = cursor.fetchone()[0]
    cursor.execute("INSERT INTO module_files "
                   "  (module_ident, fileid, filename, "
                   "   mimetype) "
                   "  VALUES (%s, %s, %s, %s) ",
                   (module_id, file_id, filename, mimetype,))
def _insert_subject_for_module(subject_text, module_id, cursor):
    cursor.execute("INSERT INTO moduletags (module_ident, tagid) "
                   "  VALUES (%s, "
                   "          (SELECT tagid FROM tags "
                   "             WHERE tag = %s)"
                   "          );",
                   (module_id, subject_text))
def _insert_keyword_for_module(keyword, module_id, cursor):
    try:
        cursor.execute("SELECT keywordid FROM keywords "
                       "  WHERE word = %s;", (keyword,))
        keyword_id = cursor.fetchone()[0]
    except TypeError:
        cursor.execute("INSERT INTO keywords (word) "
                       "  VALUES (%s) "
                       "  RETURNING keywordid", (keyword,))
        keyword_id = cursor.fetchone()[0]
    cursor.execute("INSERT INTO modulekeywords "
                   "  (module_ident, keywordid) "
                   "  VALUES (%s, %s)",
                   (module_id, keyword_id,))


class Populator:
    """main logic"""

    def __init__(self, connection_string, source_host):
        self.connection_string = connection_string
        self.source_host = source_host
        self.resolver = Resolver(self.source_host)

    def __call__(self, mid):
        """Given the moduleid populate the database with all the versions
        of this module.
        """
        for metadata, content in self.resolver(mid):
            m_ident, m_type = self.insert_module(metadata, content)
            yield m_ident
            # Resolve modules connected to a module.
            if m_type == COLLECTION:
                for ext_mid in self._get_module_contents(m_ident):
                    for ext_ident in self(ext_mid):
                        yield ext_ident
        # TODO Resolve resources for each module.
        raise StopIteration

    def insert_module(self, module_data, document):
        metadata = module_data['metadata']
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                abstract_id = _insert_abstract(module_data['abstract'],
                                               cursor)
                metadata['abstractid'] = abstract_id
                metadata['licenseid'] = _find_license_id_by_url(
                        module_data['license_url'], cursor)
                module_ident, module_type = _insert_module(metadata, cursor)
                _insert_module_file(module_ident,
                                    type_to_filename(module_type),
                                    'text/xml', document, cursor)
                for subject in module_data['subjects']:
                    _insert_subject_for_module(subject, module_ident, cursor)
                for keyword in module_data['keywords']:
                    _insert_keyword_for_module(keyword, module_ident, cursor)
        return module_ident, module_type

    def _get_module_contents(self, ident):
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT f.file FROM files AS f, "
                               "                 module_files AS mf"
                               "  WHERE mf.module_ident = %s "
                               "        AND f.fileid = mf.fileid;",
                               (ident,))
                file = cursor.fetchone()[0]
        mids = parsers.parse_collection_xml_contents(BytesIO(file[:]))
        return mids


def main(argv=None):
    """command interface to the utility"""
    parser = argparse.ArgumentParser("legacy database population utility")
    parser.add_argument('-s', '--source-host', default="cnx.org",
                        help="defaults to cnx.org")
    parser.add_argument('-c', '--connection-string',
                        default="dbname=rhaptos_dev_db",
                        help="database connection string passed to psycopg2")
    parser.add_argument('modules', nargs='+',
                        help="document ids (example, m42119)")
    args = parser.parse_args(argv)
    populator = Populator(args.connection_string, args.source_host)

    for mid in args.modules:
        for ident in populator(mid):
            with psycopg2.connect(args.connection_string) as db_connection:
                with db_connection.cursor() as cursor:
                    cursor.execute("SELECT moduleid, version, portal_type "
                                   "  FROM modules "
                                   "  WHERE module_ident = %s", (ident,))
                    id, version, type = cursor.fetchone()
                    print("-- INSERTED -- ident={} id={} version={} type={}" \
                              .format(ident, id, version, type))


if __name__ == '__main__':
    main()
