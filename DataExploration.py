# encoding: utf-8
import sqlite3 as sql
import pandas as pd
import datetime
import numpy as np
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import itertools
import networkx as nx
import sys
import re
reload(sys)
sys.setdefaultencoding('utf-8')


columns = ['link_id', 'title', 'author', 'affiliations', 'keywords', 'received_date', 'accepted_date',
           'published_date', 'abstract']

conn = sql.connect("MSOM.db")
cursor = conn.cursor()
MSOM = cursor.execute("SELECT * FROM informations;")
df_msom = pd.DataFrame.from_records(MSOM.fetchall(), columns=columns)

conn = sql.connect("MNSC.db")
cursor = conn.cursor()
MNSC = cursor.execute("SELECT * FROM informations;")
df_mnsc = pd.DataFrame.from_records(MNSC.fetchall(), columns=columns)
conn.close()

df_msom.dropna(inplace=True, subset=['received_date', 'accepted_date', 'published_date'])
df_mnsc.dropna(inplace=True, subset=['received_date', 'accepted_date', 'published_date'])


def convert_date(date_string):
    """
    Function to convert string to datetime
    :param date_string:
    :return:
    """
    try:
        date_string = date_string.strip()
        date_string = date_string.replace('Published Online:', "")
        date_string = date_string.strip()
        date = datetime.datetime.strptime(date_string, "%B %d, %Y")
    except:
        date = np.nan

    return date


def generate_wordcloud(strings):
    """
    Functiokn to generate wordcloud
    :param strings:
    :return: plot wordcloud
    """

    # Create empty stopwords list
    stopwords = set(STOPWORDS)
    stopwords.add('Keywords')
    stopwords.add('Keyword')
    stopwords.add('model')
    stopwords.add('models')

    # Plot word cloud
    wc = WordCloud(background_color="white", max_words=2000, stopwords=stopwords)
    wc.generate(strings)
    plt.imshow(wc)
    plt.axis('off')
    plt.show()


def create_authors_by_year(data):
    """

    :param data:
    :return:
    """

    authors = []
    year_publish = []
    time_publish = []

    for i in range(0, data.shape[0]):
        for j in range(0, len(data['author'][i])):
            authors.append(data['author'][i][j])
            year_publish.append(data['year_publish'][i])
            time_publish.append(data['time_publish'][i])

    return pd.DataFrame(data={'author': authors, 'year_publish': year_publish, 'time_publish': time_publish})


def create_network_data(data):
    """
    Function to create data necessary to generate the network plot
    :param data:
    :return:
    """

    # Create empty data frame
    return_df = pd.DataFrame()

    # For each row of the data read the authors and create every possible combination
    for i in range(0, data.shape[0]):
        return_df = pd.concat([return_df, pd.DataFrame(list(itertools.combinations(data['author'][i], 2)))], axis=0)

    return_df.columns = ['from', 'to']

    return return_df


def create_network_data_university(data):
    """
    Function to create data necessary to generate the network plot
    :param data:
    :return:
    """

    # Create empty data frame
    return_df = pd.DataFrame()

    # For each row of the data read the authors and create every possible combination
    for i in range(0, data.shape[0]):
        return_df = pd.concat([return_df, pd.DataFrame(list(itertools.combinations(data['affiliations'][i], 2)))], axis=0)

    return_df.columns = ['from', 'to']

    return return_df


def generate_network_plot(df_journal, label):
    """

    :param df_journal:
    :return:
    """

    # Create network dataframe
    network_data = create_network_data(df_journal)

    # Create new column with number of publications by authors
    network_data['count'] = ''

    n_publications = network_data['from'].value_counts()

    # Set the number of publications to each author
    for name in n_publications.index:
        network_data.loc[(network_data['from'] == name), 'count'] = int(
            n_publications[n_publications.index == name].values)

    # Build your graph
    G = nx.from_pandas_dataframe(network_data, 'from', 'to')

    # labels = {}
    #
    # for node in G.nodes():
    #     if node in list(network_data['label'].dropna().values):
    #         labels[node] = node

    # Plot it
    nx.draw(G, with_labels=label, node_size=list(network_data['count'].values * 30))
    plt.show()

    # g = nx.Graph()
    # g.add_nodes_from(network_data['from'].unique()[0:10])
    # g.add_edges_from([tuple(x) for x in list(network_data[['from', 'to']].values)])
    # node_sizes = list(network_data['from'].value_counts()[0:10])
    #
    # nx.draw_circular(g, node_size=node_sizes, with_labels=False)


def generate_network_plot_univ(network_data, label):
    """

    :param df_journal:
    :return:
    """

    # Create new column with number of publications by authors
    network_data['count'] = ''

    n_publications = network_data['from'].value_counts()

    # Set the number of publications to each author
    for name in n_publications.index:
        network_data.loc[(network_data['from'] == name), 'count'] = int(
            n_publications[n_publications.index == name].values)

    # Build your graph
    G = nx.from_pandas_dataframe(network_data, 'from', 'to')

    # labels = {}
    #
    # for node in G.nodes():
    #     if node in list(network_data['label'].dropna().values):
    #         labels[node] = node

    # Plot it
    nx.draw(G, with_labels=label, node_size=list(network_data['count'].values * 10))
    plt.show()

    # g = nx.Graph()
    # g.add_nodes_from(network_data['from'].unique()[0:10])
    # g.add_edges_from([tuple(x) for x in list(network_data[['from', 'to']].values)])
    # node_sizes = list(network_data['from'].value_counts()[0:10])
    #
    # nx.draw_circular(g, node_size=node_sizes, with_labels=False)


def create_university_by_year(data):
    """

    :param data:
    :return:
    """

    university = []
    year_publish = []
    time_publish = []

    for i in range(0, data.shape[0]):
        for j in range(0, len(data['affiliations'][i])):
            print data['affiliations'][i][j]
            affiliations = data['affiliations'][i][j].split(',')
            b = [x if re.search('(University|College|Institute)', x, re.IGNORECASE) else None for x in affiliations]
            try:
                c = filter(None, b)
                c = c[len(c) - 1]
                university.append(c.strip())
                year_publish.append(data['year_publish'][i])
                time_publish.append(data['time_publish'][i])
            except:
                continue

    return pd.DataFrame(data={'university': university, 'year_publish': year_publish, 'time_publish': time_publish})


def change_affiliation(affiliations):
    """

    :param affiliations: data['affiliations'][0]
    :return:
    """

    new_affiliation = []
    for affiliation in affiliations:
        b = [x if re.search('(University|College|Institute)', x, re.IGNORECASE) else None for x in affiliation.split(',')]

        try:
            c = filter(None, b)
            c = c[len(c) - 1].strip()
            new_affiliation.append(c)
        except:
            continue

    return new_affiliation


# Convert received, accepted, and published dates to datetime
# MSOM journal
df_msom['received_date'] = df_msom['received_date'].apply(convert_date)
df_msom['accepted_date'] = df_msom['accepted_date'].apply(convert_date)
df_msom['published_date'] = df_msom['published_date'].apply(convert_date)

# MNSC journal
df_mnsc['received_date'] = df_mnsc['received_date'].apply(convert_date)
df_mnsc['accepted_date'] = df_mnsc['accepted_date'].apply(convert_date)
df_mnsc['published_date'] = df_mnsc['published_date'].apply(convert_date)

# Create new attribute total time to publish
df_msom['time_publish'] = df_msom['published_date'] - df_msom['received_date']
df_mnsc['time_publish'] = df_mnsc['published_date'] - df_mnsc['received_date']

# Create new attribute with the year of publication
df_msom['year_publish'] = df_msom['published_date'].dt.year
df_mnsc['year_publish'] = df_mnsc['published_date'].dt.year

# Fill missing values for the year of publication with 1
df_msom['year_publish'].fillna(1, inplace=True)
df_mnsc['year_publish'].fillna(1, inplace=True)

# Convert year of publication to integer
df_msom['year_publish'] = df_msom['year_publish'].astype(int)
df_mnsc['year_publish'] = df_mnsc['year_publish'].astype(int)

# Get number of days for publication
df_msom['time_publish'] = df_msom['time_publish'].dt.days
df_mnsc['time_publish'] = df_mnsc['time_publish'].dt.days

# Get the average time for publication by
df_msom.dropna().groupby(['year_publish'])['time_publish'].mean().sort_index()
df_mnsc.dropna().groupby(['year_publish'])['time_publish'].mean().sort_index()

# Number of publications by years of publication
df_msom['year_publish'].value_counts()
df_mnsc['year_publish'].value_counts()

# Generate word cloud using text from KEYWORDS
strings = ' '.join(df_msom['keywords'])
generate_wordcloud(strings)
strings = ' '.join(df_mnsc['keywords'])
generate_wordcloud(strings)
strings = ' '.join(df_msom['abstract'])
generate_wordcloud(strings)
strings = ' '.join(df_mnsc['abstract'])
generate_wordcloud(strings)

# Get name of th
df_msom['author'] = df_msom['author'].apply(lambda x: x.encode("utf-8").split("|"))
df_mnsc['author'] = df_mnsc['author'].apply(lambda x: x.encode("utf-8").split("|"))

df_msom['affiliations'] = df_msom['affiliations'].apply(lambda x: x.encode("utf-8").split("|"))
df_mnsc['affiliations'] = df_mnsc['affiliations'].apply(lambda x: x.encode("utf-8").split("|"))

df_msom['affiliations'] = df_msom['affiliations'].apply(lambda x: change_affiliation(x))
df_mnsc['affiliations'] = df_mnsc['affiliations'].apply(lambda x: change_affiliation(x))

df_msom['n_authors'] = df_msom['author'].apply(lambda x: len(x))
df_mnsc['n_authors'] = df_mnsc['author'].apply(lambda x: len(x))

df_msom.groupby(['year_publish'])['n_authors'].sum().sort_index(ascending=True)
df_mnsc.groupby(['year_publish'])['n_authors'].sum().sort_index(ascending=True)

df_msom.groupby(['year_publish'])['time_publish'].mean().sort_index(ascending=True)
df_mnsc.groupby(['year_publish'])['time_publish'].mean().sort_index(ascending=True)

df_msom.groupby(['year_publish'])['time_publish'].max().sort_index(ascending=True)
df_mnsc.groupby(['year_publish'])['time_publish'].max().sort_index(ascending=True)

df_msom.groupby(['year_publish'])['time_publish'].min().sort_index(ascending=True)
df_mnsc.groupby(['year_publish'])['time_publish'].min().sort_index(ascending=True)


df_msom.groupby(['university'])['time_publish'].mean().sort_values(ascending=False)
df_msom[df_msom.affiliations.isin(top_unis.uniss.unique())].groupby(['university'])['time_publish'].mean().sort_values(ascending=False)

df_mnsc.groupby(['university'])['time_publish'].mean().sort_values(ascending=False)


# Create network dataframe
network_data = create_network_data_university(df_msom)
generate_network_plot_univ(network_data, False)

network_data = create_network_data_university(df_mnsc)
generate_network_plot_univ(network_data, False)

generate_network_plot(df_msom, False)
generate_network_plot(df_mnsc, False)

a = create_university_by_year(df_msom)
b = create_university_by_year(df_mnsc)

a.groupby(['university','year_publish'])['time_publish'].mean().sort_values(ascending=False)

a.university.value_counts()


b.groupby(['university'])['time_publish'].mean().sort_values(ascending=False).to_csv('b_uni_mean.csv')
a.groupby(['university'])['time_publish'].mean().sort_values(ascending=False).to_csv('a_uni_mean.csv')

b.university.value_counts()


from bokeh.plotting import figure
from bokeh.embed import notebook_div
from bokeh.plotting import output_file, output_notebook

plot = figure()
plot.circle([1,2], [3,4])

output_file()
