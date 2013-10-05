# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
"""Connexions content parsers"""
import os
import lxml.etree


__all__ = (
    'parse_collection_xml', 'parse_collectionL_xml_contents',
    'parse_module_xml',
    )


def _generate_xpath_func(xml_doc, default_namespace_name='base'):
    """Generates an easy to work with xpath function."""
    nsmap = xml_doc.nsmap.copy()
    try:
        nsmap[default_namespace_name] = nsmap.pop(None)
    except KeyError:
        # There isn't a default namespace.
        pass
    if "http://cnx.rice.edu/mdml/" not in nsmap.values() \
       or "http://cnx.rice.edu/mdml/0.4" in nsmap.values():
        # Fixes an issue where the namespace is defined twice, once in the
        #   document tag and again in the metadata tag.
        nsmap['md4'] = "http://cnx.rice.edu/mdml/0.4"
        nsmap['md'] = "http://cnx.rice.edu/mdml"
    return lambda xpth: xml_doc.xpath(xpth, namespaces=nsmap)


def _parse_common_elements(xml_doc):
    """Parse the common elements between a ColXML and CnXML files."""
    xpath = _generate_xpath_func(xml_doc)

    # Pull the abstract
    try:
        abstract = xpath('//md:abstract/text()')[0]
    except IndexError:
        abstract = ''

    # Pull the license
    try:
        license = xpath('//md:license/@url')[0]
    except IndexError:
        raise ValueError("Missing license metadata.")

    # Pull the collection metadata
    metadata = {
        'moduleid': xpath('//md:content-id/text()')[0],
        'version': xpath('//md:version/text()')[0],
        'name': xpath('//md:title/text()')[0],
        # FIXME Don't feel like parsing the dates at the moment.
        'created': xpath('//md:created/text()')[0],
        'revised': xpath('//md:revised/text()')[0],
        'doctype': '',  # Can't be null, but appears unused.
        'submitter': '',
        'submitlog': '',
        'language': xpath('//md:language/text()')[0],
        'authors': xpath('//md:roles/md:role[type="author"]/text()')[:],
        'maintainers': xpath('//md:roles/md:role[type="maintainer"]/text()')[:],
        'licensors': xpath('//md:roles/md:role[type="licensor"]/text()')[:],
        # 'parentauthors': None,
        # 'portal_type': 'Collection' or 'Module',

        # Related on insert...
        # 'parent': 1,
        # 'stateid': 1,
        # 'licenseid': 1,
        # 'abstractid': 1,
        }

    keywords = [kw for kw in xpath('//md:keywordlist/md:keyword/text()')]
    subjects = [s for s in xpath('//md:subjectlist/md:subject/text()')]

    return [abstract, license, metadata, keywords, subjects]


def parse_collection_xml(fp):
    """Parse into the file into segments that will fit into the database.
    Returns the abstract content, license url, metadata dictionary,
    and a list of content ids that are part of this collection.
    """
    # Parse the document
    tree = lxml.etree.parse(fp)
    doc = tree.getroot()
    xpath = _generate_xpath_func(doc, 'colxml')

    data = _parse_common_elements(doc)
    data[2]['portal_type'] = 'Collection'
    return data


def parse_collection_xml_contents(fp):
    """Parse the file to find the collections contents."""
    # Parse the document
    tree = lxml.etree.parse(fp)
    doc = tree.getroot()
    xpath = _generate_xpath_func(doc, 'colxml')
    return xpath('//colxml:module/@document')[:]


def parse_module_xml(fp):
    """Parse the file into segments that will fit into the database.
    This works against the index_auto_generated.cnxml
    Returns the abstract content, license url, metadata dictionary,
    and a list of resource urls that are in the content.
    """
    # Parse the document
    tree = lxml.etree.parse(fp)
    doc = tree.getroot()
    xpath = _generate_xpath_func(doc, 'cnxml')

    data = _parse_common_elements(doc)
    data[2]['portal_type'] = 'Module'
    return data
