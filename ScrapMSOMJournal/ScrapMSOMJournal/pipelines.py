# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import sqlite3
from os import path

from scrapy import signals
from pydispatch import dispatcher
import sys
from datetime import datetime as dt
import time


class SQLiteStorePipelineInfos(object):

    filename = '/Users/pedroveronezi/BIA656_PaperProbability/PaperProbability.db'

    def __init__(self):
        self.conn = None
        dispatcher.connect(self.initialize, signals.engine_started)
        dispatcher.connect(self.finalize, signals.engine_stopped)
        self.cursor = None

    def process_item(self, item, domain):
        try:
            date_received_processed = dt.strptime(' '.join(str(item['date_received']).split()), '%B %d, %Y')
            date_received_processed = date_received_processed.strftime('%Y-%m-%d')
        except ValueError:
            date_received_processed = ''

        try:
            data_accepted_processed = dt.strptime(' '.join(str(item['date_accepted']).split()), '%B %d, %Y')
            data_accepted_processed = data_accepted_processed.strftime('%Y-%m-%d')
        except ValueError:
            data_accepted_processed = ''

        try:
            data_published_processed = dt.strptime(' '.join(str(item['date_published']).split()), '%B %d, %Y')
            data_published_processed = data_published_processed.strftime('%Y-%m-%d')
        except ValueError:
            data_published_processed = ''

        t = (str(item['link_id']), str(item['title']), str(item['authors']), str(item['affiliations']),
             str(item['keywords']), date_received_processed, data_accepted_processed, data_published_processed,
             str(item['abstract']))
        try:
            self.cursor.execute("insert into informations values(?,?,?,?,?,?,?,?,?)", t)
            self.conn.commit()
        except sqlite3.IntegrityError:
            print "Value to be inserted already exists in the table, review the primary key"
        except:
            print "Unexpected error:", sys.exc_info()[0]
            print 'Failed to insert item: ' + item['title']
        return item

    def initialize(self):
        if path.exists(self.filename):
            self.conn = sqlite3.connect(self.filename)
            self.cursor = self.conn.cursor()
            self.conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        else:
            self.conn = self.create_table(self.filename)

    def finalize(self):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def create_table(self, filename):
        conn = sqlite3.connect(filename)
        conn.execute("""create table IF NOT EXISTS infos
                     (link_id TEXT, PRIMARY KEY, title TEXT, author TEXT, affiliations TEXT, keywords TEXT,
         received_date TEXT, accepted_date TEXT, published_date TEXT, abstract TEXT)""")
        conn.commit()
        return conn


class SQLiteStorePipelineLinks(object):

    filename = '/Users/pedroveronezi/BIA656_PaperProbability/PaperProbability.db'

    def __init__(self):
        self.conn = None
        dispatcher.connect(self.initialize, signals.engine_started)
        dispatcher.connect(self.finalize, signals.engine_stopped)
        self.cursor = None

    def process_item(self, item, domain):
        try:
            for k, v in item.items():
                self.cursor.execute("insert into links_id values(?)", (str(v),))

            self.conn.commit()
        except sqlite3.IntegrityError:
            print "Value to be inserted already exists in the table, review the primary key"
        except:
            print "Unexpected error:", sys.exc_info()[0]
            print 'Failed to insert item: ' + item['link']
        return item

    def initialize(self):
        if path.exists(self.filename):
            self.conn = sqlite3.connect(self.filename)
            self.cursor = self.conn.cursor()
            self.conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        else:
            self.conn = self.create_table(self.filename)

    def finalize(self):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def create_table(self, filename):
        conn = sqlite3.connect(filename)
        conn.execute("""create table IF NOT EXISTS links_id
                     (id TEXT PRIMARY KEY)""")
        conn.commit()
        return conn