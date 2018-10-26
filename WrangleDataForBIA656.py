from FilesReader.FileReader import FileReader
from FeatureGenerators.FeatureGeneratorBase import FeatureGeneratorBase
from DataWragling.StructuredData import StructureData
from DataBase.SqliteDB import SqliteConnector

file_reader = FileReader()

feat_gen_instance = FeatureGeneratorBase(file_reader, ['PaperProbability.db', 'MNSCPaperProbability.db'])
dict_of_journals = {'MNSC': '/Users/pedroveronezi/BIA656_PaperProbability/FilesPaperDataBase/Journal_MNSC/',
                    'MSOM': '/Users/pedroveronezi/BIA656_PaperProbability/FilesPaperDataBase/Journal_MSOM/'}

wragling = StructureData(dict_of_journals, feat_gen_instance, n_cores=1)
conn = SqliteConnector('database', '/Users/pedroveronezi/BIA656_PaperProbability/DataWrangled.db')

dataframe = wragling.run_preprocessing_singlecore_stepwise(conn, 'input')

