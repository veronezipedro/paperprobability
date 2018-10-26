import json
import requests
import sys
import time
from random import randint
from DataBase.SqliteDB import SqliteConnector


class DownloadPapersFromLinks(object):
    """
    Class responsible for download the
    """
    def __init__(self, html_base='', path_to_save=''):
        self.links_list = []
        self.html_base = html_base
        self.path_to_save = path_to_save

    @staticmethod
    def download_file(download_url, path_to_save, file_name):
        f = ''.join([path_to_save, file_name, '.pdf'])
        with open(f, 'wb') as book:
            a = requests.get(download_url, stream=True)
            for block in a.iter_content(512):
                if not block:
                    break
                book.write(block)
        print("Completed")

    def read_links(self, type='sqlite', **kwargs):
        if type == 'sqlite':
            if 'host_address' in kwargs.keys():
                host_address = kwargs['host_address']
            if 'database_name' in kwargs.keys():
                database_name = kwargs['database_name']

            try:
                links = self._read_links_sqlite(host_address, database_name)
            except:
                print 'To use type=sqlite the host_address (db file path) and the database name must be provided.'
        elif type == 'json':
            if 'link_file_path' in kwargs.keys():
                link_file_path = kwargs['link_file_path']
            else:
                print 'To use type=json a path for the file to be read must be provided.'
                raise
            links = self._read_links_json(link_file_path)
        else:
            print 'Type not supported, please entry sqlite or json.'
            raise
        self.links_list = links
        return links

    @staticmethod
    def _read_links_json(link_file_path):
        with open(link_file_path) as json_file:
            json_data = json.load(json_file)
        if len(json_data) > 0:
            links =json_data
            return links
        else:
            print 'Error on the links in the json file at %s' % link_file_path
            raise EnvironmentError

    @staticmethod
    def _read_links_sqlite(host_address, database_name):
        try:
            sqlite_conn = SqliteConnector(database_name=database_name, host_address=host_address)
            links = sqlite_conn.query('SELECT * FROM links_id')
            return links
        except:
            print "Unexpected error:", sys.exc_info()[0]
            print 'Failed to query links'

    @staticmethod
    def _read_links_with_titles(host_address, database_name):
        try:
            sqlite_conn = SqliteConnector(database_name=database_name, host_address=host_address)
            links_and_title = sqlite_conn.query('SELECT link_id, title FROM informations')
            return links_and_title
        except:
            print "Unexpected error:", sys.exc_info()[0]
            print 'Failed to query links'

    def download_pdfs(self, host_address, database_name, source_links='sqlite'):
        """
        Goes through a list of links and download them, saving it in the respective folder
        :return:
        """
        if self.links_list is None:
            self.read_links(source_links, host_address=host_address, database_name=database_name)

        links_withtitles = self._read_links_with_titles(host_address, database_name)
        count = 1
        max_ite = len(links_withtitles)
        for link_title in links_withtitles:
            title = link_title[1]
            b = title.split()
            title = ' '.join(b)
            title = title.replace('.', '')
            title = title.replace('"', '')
            title = title.replace('?', '')
            title = title.replace('/', '')
            title = title.replace('|', '')
            title = title.replace("'", '')
            title = title.replace('\\', '')
            title = title.replace('>', '')
            title = title.replace('<', '')
            title = title.replace(':', '')
            title = title.replace(';', '')
            title = title.replace('*', '')
            link_value = link_title[0]
            link_value = link_value.replace('.', '%2E')
            link_to_download = ''.join([self.html_base, link_value])
            print link_to_download
            self.download_file(link_to_download, self.path_to_save, title)
            print 'Download of file ' + title
            print 'Finished ' + str(count) + ' out of ' + str(max_ite)
            time.sleep(randint(60,120))
            count += 1

        print 'Process Finished, downloaded %s files.' % count

    def save_links_to_txt(self, path_to_save=None):
        if path_to_save is not None:
            path_with_file_name = ''.join([path_to_save, 'links_ids.txt'])
        else:
            path_with_file_name = '/Users/pedroveronezi/BIA656_PaperProbability/links_ids.txt'

        if len(self.links_list) > 0:
            with open(path_with_file_name, 'wb') as thefile:
                for item in self.links_list:
                    to_write = ''.join([self.html_base, item[0]])
                    thefile.write("%s\n" % to_write)
        else:
            links = self.read_links()
            if len(links) > 0:
                with open(path_with_file_name, 'wb') as thefile:
                    for item in links:
                        to_write = ''.join([self.html_base, item[0]])
                        thefile.write("%s\n" % to_write)
        print 'File successfully saved at' + path_with_file_name
        return path_with_file_name
