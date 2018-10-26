from os import listdir
import pandas as pd
import pathos.multiprocessing as mp
import pdb


class StructureData(object):
    """
    Class responsible to create the dataset organizing the features
    """

    def __init__(self, journal_paths_dict, feature_generation_instance, n_cores=-1):
        """
        Class constructor
        :param mypath: system path to the folder with the files to be be organized
        """
        if isinstance(journal_paths_dict, dict):
            self._mypaths = journal_paths_dict.keys()
            self._myjournals = journal_paths_dict.values()
            self._mydict = journal_paths_dict
        else:
            print 'Input must be a dictionary, with the keys being the journal and the values being the respective ' \
                  'paths'
            raise ValueError
        self._feat_gen = feature_generation_instance
        if n_cores == 1:
            self._n_cores = 1
            self._pool = None
        elif n_cores == -1:
            self._n_cores = mp.cpu_count()
            self._pool = mp.ProcessingPool(self._n_cores)
        else:
            self._n_cores = n_cores
            self._pool = mp.ProcessingPool(self._n_cores)

    @staticmethod
    def _identify_files_and_titles(my_path, journal=''):
        """

        :return:
        """
        onlyfiles = [f for f in listdir(my_path) if f.endswith('.pdf')]
        df = {'file_name': [], 'title': [], 'journal_published': [], 'file_complete_path': []}
        for f in onlyfiles:
            df['file_name'].append(f)
            if journal == 'MNSC':
                complete_path = '/Users/pedroveronezi/BIA656_PaperProbability/FilesPaperDataBase/Journal_MNSC/' + f
            elif journal == 'MSOM':
                complete_path = '/Users/pedroveronezi/BIA656_PaperProbability/FilesPaperDataBase/Journal_MSOM/' + f
            else:
                print 'Journals supported: MNSC, MSOM'
                raise KeyError
            df['file_complete_path'].append(complete_path)
            df['title'].append(f.split('.pdf')[0])
            df['journal_published'].append(journal)
        return pd.DataFrame(data=df)

    def run_preprocess(self, database_destination, table_destination):
        """

        :return:
        """
        list_of_dfs = []
        for journal, path in self._mydict.iteritems():
            list_of_dfs.append(self._identify_files_and_titles(path, journal))

        dataframe = pd.concat(list_of_dfs)

        if self._n_cores != 1:
            if self._n_cores == mp.cpu_count():
                print "Going to WARP SPEED!!!!"
            else:
                print "Meh, multiprocessing, but not good enough!"
            titles_to_process = dataframe['file_complete_path'].values
            list_features = self._pool.map(self._feat_gen.gen_features_from_pdf_file, titles_to_process)
        else:
            print "Why you didn't choose multiprocessing??? WHY???? WHY???????!"
            print ''
            list_features = []
            total_size = len(dataframe['file_complete_path'].values)
            count_ite = 1
            for t in dataframe['file_complete_path'].values:
                try:
                    list_features.append(self._feat_gen.gen_features_from_pdf_file(t))
                    count_ite += 1
                    print 'Done ' + str(t)
                    print str(count_ite) + ' of ' + str(total_size)
                except:
                    print ' '
                    print 'Problems with pdf: ' + str(t)
                    print 'Iteration: ' + str(count_ite)
                    print ' '

        file_features = pd.concat(list_features)

        print "Sorry, no multiprocessing implemented for this part :/ "
        list_of_features_from_db = []
        total_size = len(dataframe['title'].values)
        count_ite = 1
        for t in dataframe['title'].values:
            list_of_features_from_db.append(self._feat_gen.gen_features_from_database(t))
            count_ite += 1
            print 'Done ' + str(t)
            print str(count_ite) + ' of ' + str(total_size)

        db_features = pd.concat(list_of_features_from_db)

        rtn_dataframe = file_features.set_index('title').join(db_features.set_index('title'))

        rtn_dataframe = rtn_dataframe.join(dataframe.set_index('title'))
        rtn_dataframe.reset_index(level=0, inplace=True)

        return rtn_dataframe

    def run_preprocessing_singlecore_stepwise(self, database_destination, table_destination):
        """

        :param database_destination:
        :param table_destination:
        :return:
        """
        list_of_dfs = []
        for journal, path in self._mydict.iteritems():
            list_of_dfs.append(self._identify_files_and_titles(path, journal))

        dataframe = pd.concat(list_of_dfs)

        total_size = len(dataframe['file_complete_path'].values)
        count_ite = 1
        for t in dataframe['file_complete_path'].values:
            try:
                list_features_pdf = self._feat_gen.gen_features_from_pdf_file(t)
                list_of_features_from_db = \
                    self._feat_gen.gen_features_from_database(dataframe[dataframe['file_complete_path'] == t]['title'].
                                                              values[0])
                rtn_dataframe = list_features_pdf.set_index('title').join(list_of_features_from_db.set_index('title'))

                rtn_dataframe = rtn_dataframe.join(dataframe.set_index('title'))
                rtn_dataframe.reset_index(level=0, inplace=True)
                rtn_dataframe.to_sql(name=table_destination, con=database_destination.conn, flavor='sqlite',
                                     index=False, if_exists='append')
                print 'Done ' + str(t)
                print str(count_ite) + ' of ' + str(total_size)
                count_ite += 1
            except Exception as e:
                print(e)
                print ' '
                print 'Problems with pdf: ' + str(t)
                print 'Iteration: ' + str(count_ite)
                count_ite += 1
                import time
                time.sleep(5)
                print ' '

        return None




