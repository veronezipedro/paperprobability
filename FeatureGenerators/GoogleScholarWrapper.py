from FeatureGenerators.scholar import *
from pandas import DataFrame
from itertools import chain
from collections import defaultdict
__author__ = 'Pedro Veronezi'


class GoogleScholarWrapper(object):

    def __init__(self, pandasdf=True):
        """
        Class Constructor
        """
        self.asDF = pandasdf

    @staticmethod
    def _merge_dicts(dict_1, dict_2):
        """

        :param dict_A:
        :param dict_B:
        :return:
        """
        dict3 = defaultdict(list)
        for k, v in chain(dict_1.items(), dict_2.items()):
            dict3[k].append(v)
        for k, v in dict3.items():
            if len(v) > 1:
                v = [x for x in v if x is not None]
                if len(v) > 1:
                    dict3[k] = sum(v, [])
        return dict3

    def get_query(self, **kwargs):

        author = None
        allowed = None
        some = None
        none = None
        exact_phrase = None
        title_only = False
        publications = None
        after_year = None
        before_year = None
        patents = False
        cluster_id = None
        max_results = None
        save_csv = False
        citation = None

        ScholarConf.COOKIE_JAR_FILE = \
            "/Users/pedroveronezi/BIA656_PaperProbability/FeatureGenerators/cookies/cookie.jar"

        for key, value in kwargs.items():
            if key == 'author':
                author = kwargs[key]
            elif key == 'allowed':
                allowed = kwargs[key]
            elif key == 'some':
                some = kwargs[key]
            elif key == 'none':
                none = kwargs[key]
            elif key == 'exact_phrase':
                exact_phrase = kwargs[key]
            elif key == 'publications':
                publications = kwargs[key]
            elif key == 'after_year':
                after_year = kwargs[key]
            elif key == 'before_year':
                before_year = kwargs[key]
            elif key == 'patents':
                patents = kwargs[key]
            elif key == 'clusted_id':
                cluster_id = kwargs[key]
            elif key == 'max_results':
                max_results = kwargs[key]
            elif key == 'save_csv':
                save_csv = kwargs[key]
            elif key == 'citation':
                citation = kwargs[key]
            elif key == 'title_only':
                title_only = kwargs[key]

        print author
        print save_csv

        # Show help if we have neither keyword search nor author name
        if len(kwargs) == 0:
            raise ValueError

        querier = ScholarQuerier()
        settings = ScholarSettings()

        if citation == 'bt':
            settings.set_citation_format(ScholarSettings.CITFORM_BIBTEX)
        elif citation == 'en':
            settings.set_citation_format(ScholarSettings.CITFORM_ENDNOTE)
        elif citation == 'rm':
            settings.set_citation_format(ScholarSettings.CITFORM_REFMAN)
        elif citation == 'rw':
            settings.set_citation_format(ScholarSettings.CITFORM_REFWORKS)
        elif citation is not None:
            print('Invalid citation link format, must be one of "bt", "en", "rm", or "rw".')
            raise KeyError

        # Sanity-check the options: if they include a cluster ID query, it
        # makes no sense to have search arguments:
        if cluster_id is not None:
            if author or allowed or some or none \
               or exact_phrase or title_only or publications \
               or after_year or before_year is not None:
                print('Cluster ID queries do not allow additional search arguments.')
                raise KeyError

        if cluster_id is not None:
            query = ClusterScholarQuery(cluster=cluster_id)
        else:
            query = SearchScholarQuery()
            if author is not None:
                query.set_author(author)
            if allowed is not None:
                query.set_words(allowed)
            if some is not None:
                query.set_words_some(some)
            if none is not None:
                query.set_words_none(none)
            if exact_phrase is not None:
                query.set_phrase(exact_phrase)
            if title_only:
                query.set_scope(True)
            if publications is not None:
                query.set_pub(publications)
            if after_year or before_year is not None:
                query.set_timeframe(after_year, before_year)
            if patents:
                query.set_include_patents(False)
            if citation is None:
                query.set_include_citations(False)

        if max_results is not None:
            max_results = min(max_results, ScholarConf.MAX_PAGE_RESULTS)
            query.set_num_page_results(max_results)

        querier.apply_settings(settings)

        querier.send_query(query)

        if save_csv:
            csv(querier, header=True)

        dict_of_dict = python_dict(querier)
        if self.asDF:
            count = 0
            for key, value in dict_of_dict.items():
                if count == 0:
                    pandas_dict = dict.fromkeys(value.keys())
                self._merge_dicts(pandas_dict, value)
            return DataFrame(data=pandas_dict)
        else:
            rtn_dict = {}
            for key, value in dict_of_dict.items():
                try:
                    rtn_dict[value['Author']] = value
                except KeyError:
                    rtn_dict[value['Title']] = value
            return rtn_dict
