from FilesReader.FileReader import FileReader
from datetime import datetime
from nltk.corpus import stopwords
from FeatureGenerators.GoogleScholarWrapper import *
from DataBase.SqliteDB import SqliteConnector
import pandas as pd
__author__ = "Pedro Veronezi"


class FeatureGeneratorBase(object):
    """
    Base class for feature generation for the paper classification problem
    """

    def __init__(self, file_reader_instance, list_of_databases_address=[]):
        if isinstance(file_reader_instance, FileReader):
            self.f = file_reader_instance
        else:
            print 'Should use an instance of File Reader for the FeatureGeneratorBase Class'
            raise ValueError
        self._stop_words = set(stopwords.words('english'))
        self.google_scholar = GoogleScholarWrapper(pandasdf=False)
        self.db_instances = {}
        self.list_of_databases_address = list_of_databases_address
        for db in list_of_databases_address:
            self.db_instances[db] = SqliteConnector('database', db)
        self.db_dataframes = {}
        for conn_name, db_conn in self.db_instances.iteritems():
            self.db_dataframes[conn_name] = {}
            db_conn.query("PRAGMA synchronous = OFF")
            self.db_dataframes[conn_name]['informations'] = \
                pd.DataFrame(data=db_conn.query('SELECT * FROM informations'),
                             columns=['link_id', 'title', 'author', 'affiliations', 'keywords', 'received_date',
                                      'accepted_date', 'published_date', 'abstract'])
            self.db_dataframes[conn_name]['refs'] = pd.DataFrame(data=db_conn.query('SELECT * FROM refs'),
                                                                 columns=['link_id', 'titles', 'authors', 'journals'])
        for conn_name, db_conn in self.db_dataframes.iteritems():
            for table_name, df in self.db_dataframes[conn_name].iteritems():
                self.db_dataframes[conn_name][table_name] = self._clean_up_slqresults(df, table_name)

    @staticmethod
    def _clean_up_slqresults(dataframe, table_name):
        """

        :param dataframe:
        :param table_name:
        :return:
        """
        if table_name == 'informations':
            def transf_title(_title):
                return ' '.join(_title.split())
            dataframe['title'] = dataframe['title'].apply(transf_title)

            def transf_dates(_date):
                if _date != 'NA':
                    if len(_date.split(':')) == 1:
                        _date = ' '.join(_date.split())
                        try:
                            return datetime.strptime(_date, '%B %d, %Y')
                        except ValueError:
                            if _date.split()[0] == 'Feburary':
                                a = _date.split()
                                a[0] = 'February'
                                _date = ' '.join(a)
                                return datetime.strptime(_date, '%B %d, %Y')
                    else:
                        _date = _date.split(':')[1]
                        _date = ' '.join(_date.split())
                        return datetime.strptime(_date, '%B %d, %Y')
                else:
                    return datetime(1, 1, 1)

            dataframe['received_date'] = dataframe['received_date'].apply(transf_dates)
            dataframe['accepted_date'] = dataframe['accepted_date'].apply(transf_dates)
            dataframe['published_date'] = dataframe['published_date'].apply(transf_dates)

            def transf_keywords(_keywords):
                if _keywords != 'NA':
                    if len(_keywords.split(':')) > 1:
                        _keywords = _keywords.split(':')[1]
                        _keywords = '|'.join(_keywords.split(';'))
                        return _keywords
                    else:
                        _keywords = '|'.join(_keywords.split(';'))
                        return _keywords
                else:
                    return 'NA'
            dataframe['keywords'] = dataframe['keywords'].apply(transf_keywords)
            return dataframe
        elif table_name == 'refs':
            return dataframe
        else:
            print 'table_name not valid, please reconsider it.'
            raise ValueError

    def __find_title_informations_dataframes(self, title):
        """

        :param title:
        :return:
        """
        rtn_df = self.db_instances[self.list_of_databases_address[0]].\
            query("SELECT * FROM informations WHERE title LIKE '%% %s %%'" % (title,))
        if len(rtn_df) <= 0:
            rtn_df = self.db_instances[self.list_of_databases_address[1]]. \
                query("SELECT * FROM informations WHERE title LIKE '%% %s %%'" % (title,))
        return pd.DataFrame(data=rtn_df, columns=['link_id', 'title', 'authors', 'affiliations', 'keywords',
                                                  'received_date', 'accepted_date', 'published_date', 'abstract'])

    def __find_title_refs_dataframes(self, title):
        """

        :param title:
        :return:
        """
        temp_df = self.__find_title_informations_dataframes(title)
        rtn_df = self.db_instances[self.list_of_databases_address[0]]. \
            query("SELECT * FROM refs WHERE link_id = '%s' " % temp_df['link_id'].values[0])
        if len(rtn_df) <= 0:
            rtn_df = self.db_instances[self.list_of_databases_address[1]]. \
                query("SELECT * FROM refs WHERE link_id = '%s' " % str(temp_df['link_id'].values[0]))
        return pd.DataFrame(data=rtn_df, columns=['link_id', 'titles', 'authors', 'journals'])

    def gen_features_from_pdf_file(self, path_file):
        """

        :return:
        """
        size_path = len(path_file.split('/'))
        title_ = path_file.split('/')[size_path-1]
        title_ = title_.split('.pdf')[0]
        features = {'title': [title_]}
        raw, n_pages = self.f.read_file_to_raw_str(path_file)
        features['article_len'] = [len(raw)]
        features['filtered_article_len'] = \
            [len(filter(lambda w: not w in self._stop_words, raw.split()))]
        features['article_ratio'] = [features['filtered_article_len'][0]/float(features['article_len'][0])]
        features['n_pages'] = [self.f.n_pages]
        features['conclusion_len'] = [len(self._identify_conclusion(raw))]
        features['filtered_conclusion_len'] = \
            [len(filter(lambda w: not w in self._stop_words, self._identify_conclusion(raw).split()))]
        try:
            features['conclusion_ratio'] = [features['filtered_conclusion_len'][0]/float(features['conclusion_len'][0])]
        except:
            features['conclusion_ratio'] = [0.0]
        print 'Done ' + str(title_) + ' !!!!!'
        return pd.DataFrame(data=features)

    def gen_features_from_database(self, title):
        """

        :return:
        """

        features = {'title': title}
        df_title = self.__find_title_informations_dataframes(title)
        if len(df_title) > 0:
            features['abstract_len'] = [len(df_title['abstract'].values[0])]
            features['filtered_abstract_len'] = [len(filter(lambda w: not w in self._stop_words,
                                                           df_title['abstract'].values[0].split()))]
            features['abstract_ratio'] = [features['filtered_abstract_len'][0]/float(features['abstract_len'][0])]
            features['qty_authors'] = [len(str(df_title['authors']).split('|'))]
            features['keywords'] = [df_title['keywords'].values[0]]
            features['abstract'] = [df_title['abstract'].values[0]]
        else:
            features['abstract_len'] = ['NA']
            features['filtered_abstract_len'] = ['NA']
            features['abstract_ratio'] = ['NA']
            features['qty_authors'] = ['NA']
            features['keywords'] = ['NA']
            features['abstract'] = ['NA']
        return pd.DataFrame(data=features)

    def gen_features_from_google_scholar(self, title):
        """

        :param title:
        :return:
        """
        features = {'title': title}
        df_title = self.__find_title_informations_dataframes(title)
        if len(df_title) > 0:
            author_citation = 0
            for author in df_title['authors'].values[0].split('|'):
                author_citation += self._get_google_scholar_citations_stats_author(author)['sum_citations']
            features['author_citations'] = [author_citation]
            df_refs = self.__find_title_refs_dataframes(title)
            if len(df_refs) > 0:
                refs_citation = 0
                for reference in df_refs['titles'].values[0].split('|'):
                    refs_citation += self._get_google_scholar_citations_stats_title(reference)['sum_citations']
                features['refs_citation'] = [refs_citation]
        else:
            features['author_citations'] = ['NA']
            features['refs_citation'] = ['NA']
        return pd.DataFrame(data=features)

    @staticmethod
    def _identify_conclusion(raw):
        if raw.find('Conclusion') != -1:
            start_idx = raw.find('Conclusion')
        elif raw.find('Conclusions') != -1:
            start_idx = raw.find('Conclusion')
        elif raw.find('Concluding Remarks') != -1:
            start_idx = raw.find('Concluding Remarks')
        else:
            start_idx = -1
        if raw.find('References') != -1:
            end_idx = raw.find('References')
        else:
            end_idx = len(raw)
        conclusion = raw[start_idx:end_idx]
        return conclusion

    def _get_google_scholar_citations_stats_author(self, author):

        try:
            dict_author = self.google_scholar.get_query(author=author)
        except UnicodeDecodeError:
            dict_author = {}
        sum_citations = 0
        count_cit = 0
        for k, v in dict_author.items():
            try:
                sum_citations += v['Citations']
            except KeyError:
                pass
            count_cit += 1
        try:
            return {'sum_citations': sum_citations, 'count_citations': count_cit,
                    'average_citations': (sum_citations / count_cit)}
        except ZeroDivisionError:
            return {'sum_citations': sum_citations, 'count_citations': count_cit,
                    'average_citations': 0}

    def _get_google_scholar_citations_stats_title(self, title):
        try:
            dict_title = self.google_scholar.get_query(exact_phrase=title, title_only=True)
        except UnicodeDecodeError:
            dict_title = {}
        sum_citations = 0
        count_cit = 0
        for k, v in dict_title.items():
            try:
                sum_citations += v['Citations']
            except KeyError:
                pass
            count_cit += 1
        try:
            return {'sum_citations': sum_citations, 'count_citations': count_cit,
                    'average_citations': (sum_citations / count_cit)}
        except ZeroDivisionError:
            return {'sum_citations': sum_citations, 'count_citations': count_cit,
                    'average_citations': 0}

