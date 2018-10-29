# encoding: utf-8
import sqlite3 as sql
import pandas as pd
import datetime
import numpy as np
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import itertools
import sys
import re
from collections import Counter
import plotly.plotly as py
import plotly.graph_objs as go
import networkx as nx
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
        b = [x if re.search('(University|College|Institute)', x, re.IGNORECASE) else None for x in
             affiliation.split(',')]

        try:
            c = filter(None, b)
            c = c[len(c) - 1].strip()
            new_affiliation.append(c)
        except:
            continue

    return new_affiliation


def get_word_count(astring):
    """

    :param astring: 
    :return: 
    """
    stopwords = set(STOPWORDS)
    stopwords.add('Keywords')
    stopwords.add('Keyword')
    stopwords.add('model')
    stopwords.add('models')
    stopwords = [x.lower() for x in stopwords]
    stopwords = list(stopwords)
    alist = astring.lower().strip().split()
    alist = [x for x in alist if x not in stopwords]
    return Counter(alist).most_common(50)


def func1(alist_of_tuples, item_to_check):
    try:
        return [x[1] for x in alist_of_tuples if x[0] == item_to_check][0]
    except IndexError:
        return np.nan


def flatten_list_of_tuples(list_of_tuples):
    list_fixed_words = []
    for item in list_of_tuples:
        list_fixed_words.append(item[0])
    return list_fixed_words


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

    # Plot it
    nx.draw(G, with_labels=label, node_size=list(network_data['count'].values * 30))
    plt.show()


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

    # Plot it
    nx.draw(G, with_labels=label, node_size=list(network_data['count'].values * 10))
    plt.show()


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


# Get name of th
df_msom['author'] = df_msom['author'].apply(lambda x: x.encode("utf-8").split("|"))
df_mnsc['author'] = df_mnsc['author'].apply(lambda x: x.encode("utf-8").split("|"))

df_msom['affiliations'] = df_msom['affiliations'].apply(lambda x: x.encode("utf-8").split("|"))
df_mnsc['affiliations'] = df_mnsc['affiliations'].apply(lambda x: x.encode("utf-8").split("|"))

df_msom['affiliations'] = df_msom['affiliations'].apply(lambda x: change_affiliation(x))
df_mnsc['affiliations'] = df_mnsc['affiliations'].apply(lambda x: change_affiliation(x))

df_msom['n_authors'] = df_msom['author'].apply(lambda x: len(x))
df_mnsc['n_authors'] = df_mnsc['author'].apply(lambda x: len(x))



# Create network dataframe
network_data = create_network_data_university(df_mnsc)

# Create new column with number of publications by authors
network_data['count'] = ''

n_publications = network_data['from'].value_counts()

# Set the number of publications to each author
for name in n_publications.index:
    network_data.loc[(network_data['from'] == name), 'count'] = int(
        n_publications[n_publications.index == name].values)

# Build your graph
G = nx.from_pandas_dataframe(network_data, 'from', 'to', 'count')
# G = nx.random_geometric_graph(len(G.nodes()), 0.125)
pos = nx.random_layout(G)
# G = nx.from_pandas_dataframe(network_data, 'from', 'to', 'count')

for node in G.nodes():
    G.node[node]['pos'] = list(pos[node])


dmin = 1
ncenter = 0
for n in pos:
    x , y = pos[n]
    d = (x-0.5)**2+(y-0.5)**2
    if d < dmin:
        ncenter = n
        dmin = d

p = nx.single_source_shortest_path_length(G, ncenter)


edge_trace = go.Scatter(
    x=[],
    y=[],
    line=dict(width=0.5,color='#888'),
    hoverinfo='none',
    mode='lines')

for edge in G.edges():
    x0, y0 = G.node[edge[0]]['pos']
    x1, y1 = G.node[edge[1]]['pos']
    edge_trace['x'] += tuple([x0, x1, None])
    edge_trace['y'] += tuple([y0, y1, None])

node_trace = go.Scatter(
    x=[],
    y=[],
    text=[],
    mode='markers',
    hoverinfo='text',
    marker=dict(
        showscale=True,
        # colorscale options
        #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
        #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
        #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
        colorscale='YlGnBu',
        reversescale=True,
        color=[],
        size=10,
        colorbar=dict(
            thickness=15,
            title='Node Connections',
            xanchor='left',
            titleside='right'
        ),
        line=dict(width=2)))

for node in G.nodes():
    x, y = G.node[node]['pos']
    node_trace['x'] += tuple([x])
    node_trace['y'] += tuple([y])

for node, adjacencies in enumerate(G.adjacency()):
    node_trace['marker']['color']+=tuple([len(adjacencies[1])])
    node_info = '# of connections: '+str(len(adjacencies[1]))
    node_trace['marker']['size'].append(len(adjacencies))

fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title='<br>The Data Incubator',
                titlefont=dict(size=16),
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[ dict(
                    text="By University",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

py.iplot(fig, filename='networkx')