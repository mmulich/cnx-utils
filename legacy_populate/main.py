# -*- coding: utf-8 -*-
import os
import argparse
import hashlib
import sqlite3
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

CACHE_DIRECTORY = os.path.expanduser('~/.cache/cnx-utils/legacy_populate')
if not os.path.exists(CACHE_DIRECTORY):
    os.makedirs(CACHE_DIRECTORY)
RESOLVER_CACHE_FILEPATH = os.path.join(CACHE_DIRECTORY, 'resolver.db')


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
    return dict(zip(parsed_item_keys,
                    parser(BytesIO(document.encode('utf8')))))


class Resolver:
    """utility for source resolution about a piece of content."""

    def __init__(self, host, enable_cache=True):
        self.host = host
        self.is_cache_enabled = enable_cache
        self._cache_connection = sqlite3.connect(RESOLVER_CACHE_FILEPATH)
        self._cache_setup()

    def report_activity(self, activity, message):
        print("-- {} -- {}".format(activity.upper(), message))

    def _cache_setup(self):
        with self._cache_connection as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS 'modules_cache' ("
                           "  url TEXT PRIMARY KEY, "
                           "  document TEXT );")

    def _retrieve_document(self, url):
        with self._cache_connection as cursor:
            e = cursor.execute("SELECT document "
                               "  FROM modules_cache WHERE url = ?;",
                               (url,))
            try:
                document = e.fetchone()[0]
            except TypeError:
                document = None
        return document

    def _cache_document(self, url, document):
        with self._cache_connection as cursor:
            cursor.execute("INSERT INTO modules_cache "
                           "  (url, document) VALUES (?, ?);",
                           (url, document,))

    def _invalidate_document(self, url):
        with self._cache_connection as cursor:
            cursor.execute("DELETE FROM modules_cache WHERE url = ?",
                           (url,))

    def to_url(self, mid, version='latest'):
        return "http://{}/content/{}/{}".format(self.host, mid, version)

    def to_source_url(self, mid, version):
        return "{}/source".format(self.to_url(mid, version))

    def __call__(self, mid):
        for version in self.get_versions(mid):
            # XXX Necessary to work around an issue on cnx.org where collection
            #     source for previous versions is inaccessible.
            if mid.startswith('c'):
                source_version = self.get_latest_version(mid)
            else:
                source_version = version
            url = self.to_source_url(mid, source_version)

            if self.is_cache_enabled:
                self.report_activity('retrieving cache', "for: {}".format(url))
                document = self._retrieve_document(url)
            else:
                self._invalidate_document(url)
                document = None
            if document is None:
                self.report_activity('requesting', url)
                resp = requests.get(url)
                document = unicode(resp.content, 'utf8')
                self._cache_document(url, document)

            try:
                metadata = parse_to_metadata(id_to_type(mid), document)
            except:
                self._invalidate_document(url)
                raise

            # XXX Part of the cnx.org collection source inaccessibility
            #     workaround. Fix the version contrary to what the document
            #     has in it.
            metadata['metadata']['version'] = version

            yield metadata, document
        raise StopIteration

    def get_module_resources(self, mid, version,
                             has_resource_callback=None):
        url = self.to_url(mid, version)
        contents_url = "{}/objectIds".format(url)
        resp = requests.get(contents_url)
        # XXX Evil but in a crunch.
        resources = eval(resp.text)
        resources.pop(resources.index('index.cnxml'))
        if has_resource_callback is not None:
            has_resources_callback = lambda filename: False
        for filename in resources:
            has_resource = has_resource_callback(filename)
            if has_resource:
                continue
            resource_url = "{}/{}".format(url, filename)
            resp = requests.get(resource_url)
            mimetype = resp.headers['content-type']
            file = BytesIO(resp.content)
            yield filename, mimetype, file
        raise StopIteration

    def get_latest_version(self, mid):
        """Retrieve the latest version of a module."""
        resp = requests.get("{}/getVersion".format(self.to_url(mid)))
        return resp.text.strip()

    def get_versions(self, mid):
        """Parse the html document to find the versions for this module."""
        # XXX cnx.org protects the method on modules for getting the version.
        #     So screw it and use 'latests' for this part.
        latest_version = mid.startswith('m') and 'latest' or self.get_latest_version(mid)
        url = "{}/content_info".format(self.to_url(mid, latest_version))
        if self.is_cache_enabled:
            self.report_activity('using cache', url)
            document = self._retrieve_document(url)
        else:
            self._invalidate_document(url)
            document = None
        if document is None:
            self.report_activity('requesting', url)
            resp = requests.get(url)
            document = unicode(resp.content, 'utf8')
            self.report_activity('caching', url)
            self._cache_document(url, document)

        try:
            doc = lxml.html.parse(BytesIO(document.encode('utf8')))
        except:
            self._invalidate_document(url)
            raise
        xpath_exp = '//div[@id="cnx_history_section"]//a[@class="cnxn"]/text()'
        versions = doc.xpath(xpath_exp)
        versions.reverse()
        self.report_activity('working', "versions for '{}': {}" \
                                 .format(mid, ', '.join(versions)))
        return versions


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
    if isinstance(file, unicode):
        file = file.encode('utf8')
    payload = (psycopg2.Binary(file),)
    md5 = hashlib.md5()
    md5.update(file)
    cursor.execute("SELECT fileid FROM files WHERE md5 = %s",
                   (md5.hexdigest(),))
    try:
        file_id = cursor.fetchone()[0]
    except TypeError:
        file_id = None

    if file_id is None:
        cursor.execute("INSERT INTO files (file) VALUES (%s) "
                       "RETURNING fileid;", payload)
        file_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO module_files "
                   "  (module_ident, fileid, filename, "
                   "   mimetype) "
                   "  VALUES (%s, %s, %s, %s) ",
                   (module_id, file_id, filename, mimetype,))
    return file_id

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

    def __init__(self, connection_string, source_host, use_cache=True):
        self.connection_string = connection_string
        self.source_host = source_host
        self.resolver = Resolver(self.source_host, enable_cache=use_cache)

    def __call__(self, mid):
        """Given the moduleid populate the database with all the versions
        of this module.
        """
        for metadata, content in self.resolver(mid):
            m_ident = self.get_module_ident_from_metadata(metadata['metadata'])
            if m_ident is None:
                m_ident, m_type = self.insert_module(metadata, content)
                self.report_activity_on_ident('inserted', m_ident)
            else:
                m_type = self.get_module_type_from_ident(m_ident)
                self.report_activity_on_ident('exists', m_ident)
            yield m_ident
            # Resolve modules connected to a module.
            if m_type == COLLECTION:
                for ext_mid in self._get_module_contents(m_ident):
                    for ext_ident in self(ext_mid):
                        yield ext_ident
            # Resolve resources for each module.
            elif m_type == MODULE:
                version = self._get_module_version(m_ident)
                has_resource_callback = self._generate_resource_callback(m_ident)
                resources = self.resolver.get_module_resources(
                        mid, version, has_resource_callback)
                for filename, mimetype, file in resources:
                    self.insert_module_file(m_ident, filename, mimetype, file)
                    self.report_activity('inserted',
                                         "resource '{}' for module with '{}' "
                                         "ident".format(filename, m_ident))
        raise StopIteration

    def get_module_ident_from_metadata(self, metadata):
        mid = metadata['moduleid']
        version = metadata['version']
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT module_ident FROM modules "
                               "  WHERE moduleid = %s AND version = %s",
                               (mid, version,))
                try:
                    ident = cursor.fetchone()[0]
                except TypeError:  # 'NoneType' object is not subscriptable
                    ident = None
        return ident

    def get_module_type_from_ident(self, ident):
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT portal_type AS type FROM modules "
                               "  WHERE module_ident = %s", (ident,))
                try:
                    type = cursor.fetchone()[0]
                except TypeError:  # 'NoneType' object is not subscriptable
                    raise ValueError("Module at '{}' probably doesn't "
                                     "exist.".format(ident))
        return type

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

    def insert_module_file(self, module_ident, filename, mimetype, file):
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                _insert_module_file(module_ident, filename, mimetype,
                                    file, cursor)
        return

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

    def _get_module_version(self, ident):
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT version from modules "
                               "  WHERE module_ident = %s;",
                               (ident,))
                version = cursor.fetchone()[0]
        return version

    def _generate_resource_callback(self, ident):
        def callback(filename):
            with psycopg2.connect(self.connection_string) as db_connection:
                with db_connection.cursor() as cursor:
                    cursor.execute("SELECT 'T'::bool FROM module_files "
                                   "  WHERE module_ident = %s "
                                   "        AND filename = %s;",
                                   (ident, filename))
                    is_found = cursor.fetchone()
            return is_found is not None and True or False
        return callback

    def report_activity(self, activity, message):
        """Print a statement about the activity."""
        print("-- {} -- {}".format(activity.upper(), message))

    def report_activity_on_ident(self, activity, ident):
        """Print a statement about the activity on the given ident."""
        with psycopg2.connect(self.connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT moduleid, version, portal_type "
                               "  FROM modules "
                               "  WHERE module_ident = %s", (ident,))
                id, version, type = cursor.fetchone()
        message = "ident={} id={} version={} type={}" \
                .format(ident, id, version, type)
        self.report_activity(activity, message)


def main(argv=None):
    """command interface to the utility"""
    parser = argparse.ArgumentParser("legacy database population utility")
    parser.add_argument('-s', '--source-host', default="cnx.org",
                        help="defaults to cnx.org")
    parser.add_argument('-c', '--connection-string',
                        default="dbname=rhaptos_dev_db",
                        help="database connection string passed to psycopg2")
    parser.add_argument('--disable-cache', dest="is_cache_enabled",
                        action="store_false",
                        help="Disable the source resolution cache")
    parser.add_argument('modules', nargs='+',
                        help="document ids (example, m42119)")
    args = parser.parse_args(argv)
    populator = Populator(args.connection_string, args.source_host,
                          use_cache=args.is_cache_enabled)

    idents = []
    for mid in args.modules:
        idents.extend([ident for ident in populator(mid)])
    print("Worked on {}.".format(', '.join([str(x) for x in idents])))


if __name__ == '__main__':
    main()
