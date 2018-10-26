from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from DataBase.SqliteDB import SqliteConnector
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle

conn = SqliteConnector('database', '/Users/pedroveronezi/BIA656_PaperProbability/DataWrangled.db')

df_col_names = []

table_structure = conn.query('pragma table_info(input)')

for col in table_structure:
    df_col_names.append(col[1])

dataset = pd.DataFrame(data=conn.query('SELECT * from input'), columns=df_col_names)

dataset = dataset[dataset['abstract'] != 'NA']

dataset.reset_index(inplace=True)

dataset = shuffle(dataset)

train, test = train_test_split(dataset, test_size=0.2)

train_2 = train[train['journal_published'] == 'MSOM']

train = pd.concat([train, train_2])

train = shuffle(train)

X_train = train['abstract']
Y_train = train['journal_published']

X_test = test['abstract']
Y_test = test['journal_published']

text_clf = Pipeline([('vect', CountVectorizer(stop_words=set(stopwords.words('english')))),
                     ('tfidf', TfidfTransformer()),
                     ('clf', SGDClassifier(loss='hinge', penalty='l1', alpha=1e-3, n_iter=50, random_state=42)),
                     ])

text_clf = text_clf.fit(X_train, Y_train)

predicted = text_clf.predict(X_test)
np.mean(predicted == Y_test)

parameters = {'vect__ngram_range': [(1, 1), (1, 2), (3, 4), (10, 10)],
              'tfidf__use_idf': (True, False),
              'clf__alpha': (1e-2, 1e-3),
              }

gs_clf = GridSearchCV(text_clf, parameters, n_jobs=-1)
gs_clf = gs_clf.fit(X_train, Y_train)
predicted = text_clf.predict(X_test)
np.mean(predicted == Y_test)

print metrics.classification_report(Y_test, predicted)
print metrics.confusion_matrix(Y_test, predicted)
