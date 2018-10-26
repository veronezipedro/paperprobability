"""

Created by veronezi.pedro

6th February 2017


"""

from DataBase import DataBaseInterface
import sqlite3 as sq


class SqliteConnector(DataBaseInterface):
    """
    Class responsible to connect with the Sqlite database
    """

    def __init__(self, database_name, host_address, user=None, password=None, port=None):
        """
        Class constructor
        :param user: Username to access the database
        :type user: String
        :param pwd: Password to access the database
        :type pwd: String
        :param database_name: Database name
        :type database_name: String
        :param host_address: IP address or local
        :type host_address: String
        """
        DataBaseInterface.__init__(self, user, password, database_name, host_address, port)

    def _connect_to_db(self):
        self.conn = sq.connect(self.address)
        self.pointer = self.conn.cursor()

    def _query(self, statement):
        self.pointer.execute(statement)
        return self.pointer.fetchall()

    def _insert(self, DataFrame, table, flavor):
        """

        :param DataFrame:
        :type pandas.DataFrame
        :param table:
        :param flavor:
        :return:
        """
        try:
            DataFrame.to_sql(table, self.conn, flavor=flavor, index=False, chunksize=50, if_exists='replace')
            return 'success'
        except:
            raise ValueError("Trying to insert a different flavor using the SqLite connection!")

    def _querymany(self, statement_list):
        list_of_returns = []
        for qry in statement_list:
            list_of_returns.append(self._query(qry))
        return list_of_returns

    def _create_table(self, dict_fields_types, table_name, primary_key):
        row_statement = []
        last_item_count = 1
        last_item = len(dict_fields_types)
        for k, v in dict_fields_types.items():
            if primary_key is not None and primary_key == k:
                if last_item_count == last_item:
                    row_statement.append(str(k) + ' ' + str(v) + ' ' + 'PRIMARY KEY ')
                else:
                    row_statement.append(str(k) + ' ' + str(v) + ' ' + 'PRIMARY KEY, ')
            else:
                if last_item_count == last_item:
                    row_statement.append(str(k) + ' ' + str(v))
                else:
                    row_statement.append(str(k) + ' ' + str(v) + ', ')
            last_item_count += 1

        first_statement = 'CREATE TABLE IF NOT EXISTS ' + str(table_name) + ' ('
        middle_statement = ''.join(row_statement)
        last_statement = ');'

        create_table_query = ''.join([first_statement, middle_statement, last_statement])

        return self._query(create_table_query)
