"""

Created by veronezi.pedro

6th February 2017


"""
from abc import abstractmethod, ABCMeta


class DataBaseInterface(object):
    """
    This is a base class to develop the connections with several different databases implementations
    """

    __metaclass__ = ABCMeta

    def __init__(self, user, password,  database_name, host_address=None, port=None):
        """

        :param user: Username to access the database
        :type user: String
        :param pwd: Password to access the database
        :type pwd: String
        :param database_name: Database name
        :type database_name: String
        :param host_address: IP address or local
        :type host_address: String
        """
        # Stores the initialized variables
        self.user = user
        self.pwd = password
        self.dbname = database_name
        self.address = host_address
        self.port = port

        # Initiates the variables to later use

        # Instantiates the pointer for the database
        self.pointer = None
        # Instantiates the connection variable
        self.conn = None
        # Calls the function to start a connection with the database
        self._connect_to_db()

    def connect_to_db(self):
        self._connect_to_db()

    @abstractmethod
    def _connect_to_db(self):
        pass

    def query(self, statement):
        if self.conn is None:
            try:
                self._connect_to_db()
                return self._query(statement)
            except:
                raise ValueError("Not possible to connect to the database: " + self.dbname)
                return None
        else:
            return self._query(statement)

    @abstractmethod
    def _query(self, statement):
        pass

    def insert(self, DataFrame, table, flavor="sqlite"):
        if self.conn is None:
            try:
                self._connect_to_db()
                rtn = self._insert(DataFrame, table, flavor)
            except:
                raise ValueError("Not possible to connect to the database: " + self.dbname)
        else:
            rtn = self._insert(DataFrame, table, flavor)
        if rtn != 'success':
            raise EnvironmentError("Problem into inserting into the database")

    @abstractmethod
    def _insert(self, DataFrame, table, flavor):
        pass

    def querymany(self, statement_list):
        if self.conn is None:
            try:
                self._connect_to_db()
                return self._querymany(statement_list)
            except:
                raise ValueError("Not possible to connect to the database: " + self.dbname)
                return None
        else:
            return self._querymany(statement_list)

    @abstractmethod
    def _querymany(self, statement_list):
        pass

    def create_table(self, dict_fields_types, table_name, primary_key=None):
        self._create_table(dict_fields_types, table_name, primary_key)

    @abstractmethod
    def _create_table(self, dict_fields_types, table_name, primary_key):
        pass








