__author__ = 'Julien'

import MySQLdb
import traceback
import sys
import re
import imp
try:
    imp.find_module('sshtunnel')
    import sshtunnel
    has_sshtunnel = True
except ImportError:
    has_sshtunnel = False

class Helper:
    __BACKUP_TABLE_NAME_FORMAT = '`%s.bak.%d`'
    __TABLE_NAME_PATTERN       = re.compile('`?((?:(?!`|\\.bak).)+)(?:\\.bak\\.(\d+))?`?')

    @staticmethod
    def print_warning(warning):
        print '\n************************ Database Warning ************************\n\n%s\n' % warning \
              + '\n******************************************************************\n'

    @staticmethod
    def print_error(error, limit=-1):
        try:
            raise RuntimeError()
        except RuntimeError:
            print '\n************************* Database Error *************************\n\n%s' \
                  '\n\nTraceback (most recent call last):\n' % error \
                  + ''.join(traceback.format_stack()[:limit]) \
                  + '\n******************************************************************\n'

    @staticmethod
    def print_database_error(error_description, sql, values, e):
        Helper.print_error('Database error when %s!\n   -> Here is the sql: %s'
                           '\n   -> The values to bind: %s\n   -> Error: %s' %
                           (error_description, sql, str(values), str(e.args)), -2)

    @staticmethod
    def init_list(values):
        if not isinstance(values, list):
            return [values]
        return values

    @staticmethod
    def init_double_list(values):
        if not isinstance(values, list):
            return [[values]]
        if len(values) > 0 and not isinstance(values[0], list):
            return [values]
        return values

    @staticmethod
    def check_double_list(values, name):
        for row in values:
            if len(row) == 2:
                if not isinstance(row[0], basestring):
                    Helper.print_error('First column of `%s` must be a column name!' % name)
                    return False
                if isinstance(row[1], TableQuery):
                    Helper.print_error('Conditional statement in `%s.%s` require a special comparator!' % (name, row[0]))
                    return False
            elif len(row) == 3:
                if not isinstance(row[0], basestring):
                    Helper.print_error('First column of `%s` must be a column name!' % name)
                    return False
                if not isinstance(row[1], basestring):
                    Helper.print_error('`%s` is not a valid comparator for `%s.%s`!' % (str(row[1]), name, row[0]))
                    return False
                if isinstance(row[2], TableQuery) and not row[2].is_valid(name):
                    return False
            else:
                print Helper.print_error('`%s` rows have to be [column_name, value] '
                                         'or [column_name, comparator, value]!' % name)
                return False
        return True

    @staticmethod
    def check_order_by_list(values, name):
        for row in values:
            if len(row) != 2 or not isinstance(row[0], basestring) or (row[1] != 'ASC' and row[1] != 'DESC'):
                Helper.print_error('`%s` rows must contain [column_name, \'ASC\'|\'DESC\']!' % name)
                return False
        return True

    @staticmethod
    def get_list_values(values):
        row_values = lambda(row_value): row_value.get_values() if isinstance(row_value, TableQuery) else [row_value]
        return [value for row in values for value in row_values(row[len(row)-1])]

    @staticmethod
    def __format_name(name):
        if name[0] == '`':
            name = name[1:]
        if name[-1] == '`':
            name = name[:-1]
        return '`%s`' % name

    @staticmethod
    def format_database_name(name):
        return Helper.__format_name(name.lower())

    @staticmethod
    def format_table_name(name):
        return Helper.__format_name(name)

    @staticmethod
    def format_backup_table_name(name):
        match = Helper.__TABLE_NAME_PATTERN.match(name)
        if match:
            name_root = match.group(1)
            version   = match.group(2)
            if version is None:
                version = 0
            else:
                version = int(version) + 1
        else:
            name_root = name
            if name_root[0] == '`':
                name_root = name_root[1:]
            if name_root[-1] == '`':
                name_root = name_root[:-1]
            version = 0
        return Helper.__BACKUP_TABLE_NAME_FORMAT % (name_root, version)

class TableQuery:
    def __init__(self, database_name, table_name, to_fetch=[], to_fetch_modifiers=[], where=[], order_by=[],
                 limit=None, join=[], join_database_name=None, join_table_name=None, join_where=[], join_to_fetch=[],
                 join_to_fetch_modifiers=[], to_compare=[], join_order_by=[], join_limit=None):
        """
        Initialize a table query.
        :param database_name: Name of the database to look into.
        :param table_name: Name of the table to look into
        :param to_fetch: Columns to fetch. Can be a string or a list of string. Can be empty or '*'.
        :param to_fetch_modifiers: Modifiers to apply (ex: count, distinct, ...). Can be a string or a list of string.
            If both to_fetch and to_fetch modifiers are provided, they must have the same size. Can be empty.
        :param where: List (or singleton) of lists [column_name, comparator, value] clauses. The comparator is optional,
            by default it's '='. The value can be a string, a number, a boolean or another TableQuery object with a non
            empty to_fetch attribute. Can be empty.
        :param order_by: List (or singleton) of pairs [column_name, 'ASC'/'DESC'] specifying the order in which entries
            are fetched. Can be empty, in this case the order is arbitrary.
        :param limit: Indicates the maximal number of entries to fetch.
        :param join: List (or singleton) of list [TableQuery object, [([)column_name, (comparator,) join_column_name(])]
            to (inner) join with. Can be empty.
        :param join_database_name: Name of database of the created join TableQuery object. If empty, database_name
            is used instead.
        :param join_table_name: Name of table of the created join TableQuery object. Cannot be None if a join table
            needs to be created.
        :param join_where: Where clauses for the created join TableQuery object.
        :param join_to_fetch: Fetch columns for the created join TableQuery object.
        :param join_to_fetch_modifiers: Fetch modifiers for the created join TableQuery object.
        :param to_compare: List (or singleton) of pairs [column_name, (comparator,) join_column_names] specifying which
            columns have to be compared for the join operation
        :param join_order_by: Order by clauses for the created join TableQuery object.
        :param join_limit: Limit parameter for the created join TableQuery object.
        """

        self.database_name = Helper.format_database_name(database_name)
        if len(join) == 0 and join_table_name is not None:
            if join_database_name is None:
                join_database_name = self.database_name
            join = [TableQuery(database_name      = join_database_name,
                               table_name         = join_table_name,
                               to_fetch           = join_to_fetch,
                               to_fetch_modifiers = join_to_fetch_modifiers,
                               where              = join_where,
                               order_by           = join_order_by,
                               limit              = join_limit), to_compare]

        self.table_name         = Helper.format_table_name(table_name)
        self.to_fetch           = Helper.init_list(to_fetch)
        self.to_fetch_modifiers = Helper.init_list(to_fetch_modifiers)
        self.where              = Helper.init_double_list(where)
        self.order_by           = Helper.init_double_list(order_by)
        self.limit              = limit
        self.join               = Helper.init_double_list(join)
        for join_entry in self.join:
            join_entry[1] = Helper.init_double_list(join_entry[1])

    def is_valid(self, parent_name='sql'):
        """ Returns if the query parameters are valid. """
        if self.database_name is None:
            Helper.print_error('%s.database_name not provided!' % parent_name)
            return False
        if self.table_name is None:
            Helper.print_error('%s.table_name not provided!' % parent_name)
            return False
        if (len(self.to_fetch) > 0) and (len(self.to_fetch_modifiers) > 0) \
                and (len(self.to_fetch) != len(self.to_fetch_modifiers)):
            Helper.print_error('%s.to_fetch and %s.to_fetch_modifiers lists don\'t have the same size!' %
                               (parent_name, parent_name))
            return False
        for j, join_entry in enumerate(self.join):
            if not join_entry[0].is_valid('%s.join[%d]' % (parent_name, j)):
                return False
        return Helper.check_double_list(self.where, '%s.where' % parent_name) and \
               Helper.check_order_by_list(self.order_by, '%s.order_by' % parent_name)

    def get_values(self):
        values = Helper.get_list_values(self.where)
        for join_entry in self.join:
            values += join_entry[0].get_values()
        return values

    def format_join_clauses(self, t_i, index, depth, prefix):
        sql = ''
        for j, join_entry in enumerate(self.join):
            t_j    = '%s_%d_%d' % (prefix, index+j, depth+1)
            query  = join_entry[0]
            sql   += ' INNER JOIN %s.%s AS %s ON ' % (query.database_name, query.table_name, t_j)
            comp = join_entry[1]
            for comp_entry in comp:
                if len(comp_entry) == 3:
                    sql += '%s.%s %s %s.%s AND ' % (t_i, comp_entry[0], comp_entry[1], t_j, comp_entry[2])
                else:
                    sql += '%s.%s=%s.%s AND ' % (t_i, comp_entry[0], t_j, comp_entry[1])
            sql = sql[0:-5] + query.format_join_clauses(t_j, index+j, depth+1, prefix)
        return sql

    def has_where_clauses(self):
        if len(self.where) > 0:
            return True
        for join_entry in self.join:
            if join_entry[0].has_where_clauses():
                return True
        return False

    def format_where_clauses(self, t_i, index, depth, prefix):
        sql = ''
        for k, clause in enumerate(self.where):
            t_k = '%s_%d_%d' % (prefix, index+k+len(self.join), depth+1)
            if len(clause) == 3:
                if isinstance(clause[2], TableQuery):
                    sql += '%s.%s %s (%s) AND ' % (t_i, clause[0], clause[1],
                                                   clause[2].format_fetch_statement(t_k, index+k+len(self.join), depth+1, prefix))
                else:
                    sql += '%s.%s %s %%s AND ' % (t_i, clause[0], clause[1])
            else:
                sql += '%s.%s=%%s AND ' % (t_i, clause[0])
        for j, join_entry in enumerate(self.join):
            t_j = '%s_%d_%d' % (prefix, index+j, depth+1)
            table_query = join_entry[0]
            if table_query.has_where_clauses():
                sql += '%s AND ' % table_query.format_where_clauses(t_j, index+j, depth+1, prefix)
        return sql[0: -5]

    def has_order_by_clauses(self):
        if len(self.order_by) > 0:
            return True
        for join_entry in self.join:
            if join_entry[0].has_order_by_clauses():
                return True
        return False

    def format_order_by_clauses(self, t_i, index, depth, prefix):
        sql = ''
        for clause in self.order_by:
            sql += '%s.%s %s, ' % (t_i, clause[0], clause[1])
        for j, join_entry in enumerate(self.join):
            t_j = 't_%d_%d' % (index+j, depth+1)
            table_query = join_entry[0]
            if table_query.has_order_by_clauses():
                sql += '%s, ' % table_query.format_order_by_clauses(t_j, index+j, depth+1, prefix)
        return sql[0: -2]

    def has_fetch_clauses(self):
        if len(self.to_fetch) > 0 or len(self.to_fetch_modifiers) > 0:
            return True
        for join_entry in self.join:
            if join_entry[0].has_fetch_clauses():
                return True
        return False

    def format_fetch_clauses(self, t_i, index, depth, prefix):
        sql = ''
        if len(self.to_fetch) > 0 and len(self.to_fetch_modifiers) == 0:
            for clause in self.to_fetch:
                sql += '%s.%s, ' % (t_i, clause)
        elif len(self.to_fetch) > 0 and len(self.to_fetch_modifiers) > 0:
            for clause, modifier in zip(self.to_fetch, self.to_fetch_modifiers):
                if clause == '*':
                    sql += '%s(*), ' % modifier
                else:
                    sql += '%s(%s.%s), ' % (modifier, t_i, clause)
        if len(self.to_fetch) == 0 and len(self.to_fetch_modifiers) > 0:
            for modifier in self.to_fetch_modifiers:
                sql += '%s(*), ' % modifier
        for j, join_entry in enumerate(self.join):
            t_j = 't_%d_%d' % (index+j, depth+1)
            table_query = join_entry[0]
            if table_query.has_fetch_clauses():
                sql += '%s, ' % table_query.format_fetch_clauses(t_j, index+j, depth+1, prefix)
        return sql[0: -2]

    def format_condition_statement(self, t_i='t_0_0', index=0, depth=0, prefix='t'):
        # Declaration of join tables
        sql = self.format_join_clauses(t_i, index, depth, prefix)

        # Where clauses
        if self.has_where_clauses():
            sql += ' WHERE %s' % self.format_where_clauses(t_i, index, depth, prefix)

        # Order by clauses
        if self.has_order_by_clauses():
            sql += ' ORDER BY %s' % self.format_order_by_clauses(t_i, index, depth, prefix)

        # Limit
        if self.limit is not None:
            sql += ' LIMIT %s' % str(self.limit)

        return sql

    def format_fetch_statement(self, t_i='t_0_0', index=0, depth=0, prefix='t'):
        sql = 'SELECT '
        if self.has_fetch_clauses():
            sql += self.format_fetch_clauses(t_i,index, depth, prefix)
        else:
            sql += '*'
        return sql + ' FROM %s.%s AS %s%s' % (self.database_name, self.table_name, t_i,
                                              self.format_condition_statement(t_i, index, depth, prefix))

class DatabaseWrapper:
    __CONNECTION_ERRORS__ = { 0, 2006 }

    def __init__(self, database=None, host='127.0.0.1', port=3306, user='', password='', remote_host='', remote_port=22,
                 forward_host='127.0.0.1', forward_port=-1, remote_user='', remote_password=''):
        """
        Initialize a database wrapper. Can be open locally (ex: DatabaseWrapper(user='brad', password='moxie100'))
        or remotely (ex: DatabaseWrapper(user='my_username', password='my_password', remote_host='123.456.678.901',
        remote_port=1234)).
        :param database: Another DatabaseWrapper object or a tuple (host, user, password). If not null, the database
            object will be used to initialize the new object attributes, unless those attributes are provided in
            the constructor.
        :param host: The host name of the MySql database. Default is localhost.
        :param port: The port used to connect to the database. Default is 3306.
        :param user: The username used to connect to the database.
        :param password: The password used to connect the database.
        :param remote_host: The host address of the remote server in which the database is running.
        :param remote_port: The port used to connect to the remote server.
        :param forward_host: The host name of the MySql database on the server. By default it's the same than host.
        :param forward_port: The port used to connect to the database on the server. By default it's the same than port.
        :param remote_user: The username used to connect to the remote server. By default it's the same than user.
        :param remote_password: The password used to connect to the remote server. By default it's the same than password.
        """
        self.__reconnect_iter = 0
        if database is None:
            database = (host, user, password)

        if isinstance(database, DatabaseWrapper):
            self.__host            = self.__copy_or_init_attribute(database.__host           , host           , '127.0.0.1')
            self.__port            = self.__copy_or_init_attribute(database.__port           , port           , 3306)
            self.__user            = self.__copy_or_init_attribute(database.__user           , user           , '')
            self.__password        = self.__copy_or_init_attribute(database.__password       , password       , '')
            self.__remote_host     = self.__copy_or_init_attribute(database.__remote_host    , remote_host    , '')
            self.__remote_port     = self.__copy_or_init_attribute(database.__remote_port    , remote_port    , 22)
            self.__forward_host    = self.__copy_or_init_attribute(database.__forward_host   , forward_host   , '127.0.0.1')
            self.__forward_port    = self.__copy_or_init_attribute(database.__forward_port   , forward_port   , -1)
            self.__remote_user     = self.__copy_or_init_attribute(database.__remote_user    , remote_user    , '')
            self.__remote_password = self.__copy_or_init_attribute(database.__remote_password, remote_password, '')
        else:
            self.__host            = self.__copy_or_init_attribute(database[0]    , host           , '')
            if self.__host == 'localhost':
                self.__host = '127.0.0.1'
            self.__port            = port
            self.__user            = self.__copy_or_init_attribute(database[1]    , user           , '')
            self.__password        = self.__copy_or_init_attribute(database[2]    , password       , '')
            self.__remote_host     = remote_host
            self.__remote_port     = remote_port
            self.__forward_host    = self.__copy_or_init_attribute(self.__host    , forward_host   , '127.0.0.1')
            self.__forward_port    = self.__copy_or_init_attribute(self.__port    , forward_port   , -1)
            self.__remote_user     = self.__copy_or_init_attribute(self.__user    , remote_user    , '')
            self.__remote_password = self.__copy_or_init_attribute(self.__password, remote_password, '')

        if remote_host != '':  # Remote connection
            if not has_sshtunnel:
                Helper.print_error('Package sshtunnel is not installed. Cannot open a remote connection!')
                sys.exit(-1)

            self.__server = sshtunnel.SSHTunnelForwarder((self.__remote_host, self.__remote_port),
                                                         ssh_username       = self.__remote_user,
                                                         ssh_password       = self.__remote_password,
                                                         remote_bind_address=(self.__forward_host, self.__forward_port),
                                                         local_bind_address =(self.__host, self.__port))
            self.__server.start()
        else:
            self.__server = None

        self.__database_connector = MySQLdb.connect(host   = self.__host,
                                                    user   = self.__user,
                                                    passwd = self.__password,
                                                    port   = self.__port)
        self.__cur = self.__database_connector.cursor()

    def __del__(self):
        if self.__server is not None:
            self.__server.stop()

    def __iter__(self):
        return iter(self.__cur)

    @staticmethod
    def __copy_or_init_attribute(database_value, init_value, default_value):
        if init_value == default_value:
            return database_value
        return init_value

    def __reconnect_and_retry(self, e, method, *args, **kwargs):
        if self.__reconnect_iter > 10:
            self.__reconnect_iter = 0
            Helper.print_error('Failed to connect to the database to many times:\n   -> Error: %s' % str(e.args))
            return False
        self.__reconnect_iter += 1
        self.__database_connector = MySQLdb.connect(host   = self.__host,
                                                    user   = self.__user,
                                                    passwd = self.__password,
                                                    port   = self.__port)

        self.__cur = self.__database_connector.cursor()
        success    = method(*args, **kwargs)
        self.__reconnect_iter = 0
        return success

    def database_exists(self, database_name):
        """
        Returns if the database with the given name exists.
        :param database_name: Name of the database to check
        """
        database_name = Helper.format_database_name(database_name)
        sql           = 'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s' % database_name
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return self.__cur.fetchone()[0] > 0
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.database_exists, database_name)
            Helper.print_database_error('looking for database %s' % database_name, sql, [], e)
            return False

    def create_database(self, database_name):
        """
        Creates a database using the given name.
        :param database_name: Name of the database to create
        :return: Returns True if the operation is successful, False otherwise
        """
        database_name = Helper.format_database_name(database_name)
        sql           = 'CREATE DATABASE IF NOT EXISTS %s' % database_name
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.create_database, database_name)
            Helper.print_database_error('creating database %s' % database_name, sql, [], e)
            return False

    def drop_database(self, database_name):
        """
        Drop the given database. Be careful, all the data contained in the database will be lost!
        :param database_name: Name of teh database to drop.
        :return: Returns True if the operation is successful, False otherwise
        """
        database_name = Helper.format_database_name(database_name)
        sql           = 'DROP DATABASE %s' % database_name
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.drop_database, database_name)
            Helper.print_database_error('dropping database %s' % (database_name), sql, [], e)
            return False

    def tables_list(self, database_name):
        """
        Returns the list of the tables in the given database
        :param database_name: Name of the database to look into
        :return: The list of the tables in the given database or an empty list if an error occurs
        """
        database_name = Helper.format_database_name(database_name)
        sql           = 'SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema=\'%s\'' % database_name[1:-1]
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return [row[0] for row in self]
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.tables_list, database_name)
            Helper.print_database_error('looking for tables list in %s' % (database_name), sql, [], e)
            return []

    def table_exists(self, database_name, table_name):
        """
        Returns if a table with the given name in the given database exists.
        :param database_name: Name of the database in which to look for
        :param table_name: Name of the desired table
        """
        database_name = Helper.format_database_name(database_name)
        table_name    = Helper.format_table_name(table_name)
        sql           = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\'%s\' AND table_name=\'%s\'' % \
                        (database_name[1:-1], table_name[1:-1])
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return self.__cur.fetchone()[0] > 0
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.table_exists, database_name, table_name)
            Helper.print_database_error('looking for table %s.%s' % (database_name, table_name), sql, [], e)
            return False

    def create_table(self, database_name, table_name, column_names, column_types):
        """
        Creates, if not exists, a table using the given name in the given database. If the table already exists and
        have different columns, creates a backup and drops it before creating the new one.
        :param database_name: Name of the database in which create the table
        :param table_name: Name of the database to create
        :param column_names: List of the columns to insert in the table
        :param column_types: List of the types corresponding to the given columns
        :return: Returns True if the operation is successful, False otherwise
        """

        # Check that both lists have the same size
        database_name = Helper.format_database_name(database_name)
        table_name    = Helper.format_table_name(table_name)
        column_names  = Helper.init_list(column_names)
        column_types  = Helper.init_list(column_types)
        if len(column_names) != len(column_types):
            Helper.print_error('Table column_names size and column_types size mismatch!\n   column_names: %s'
                               '\n   column_types: %s' % (str(column_names), str(column_types)))
            return False

        # Check if the table already exists
        if self.table_exists(database_name, table_name):
            # Check if the columns are the same
            old_column_names = self.table_columns(database_name, table_name)
            if set(column_names) != set(old_column_names): # Backup the table
                backup_name = Helper.format_backup_table_name(table_name)
                Helper.print_warning('Table %s.%s already exists but column names mismatch (new:%s != old:%s). '
                                     'Previous table saved to %s.%s' %
                                     (database_name, table_name, str(set(column_names)), str(set(old_column_names)),
                                      database_name, backup_name))
                self.rename_table(database_name, table_name, backup_name)
            else:
                return True

        # Format the sql
        sql = 'CREATE TABLE IF NOT EXISTS %s.%s (' % (database_name, table_name)
        for name, type in zip(column_names, column_types):
            sql += '%s %s, ' % (name, type)
        sql = sql[0:-2] + ')'

        # Execute the query
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.create_table, database_name, table_name, column_names, column_types)
            Helper.print_database_error('creating table %s.%s' % (database_name, table_name), sql, [], e)
            return False

    def rename_table(self, database_name, old_table_name, new_table_name, new_database_name=None):
        """
        Rename the given table.
        :param database_name:       Name of the database to which the table belong
        :param old_table_name:      Name of the table to rename
        :param new_table_name:      New name of the table
        :param new_database_name:   Name of the destination database. By default is the same as database_name.
                                    If different, the table is actually moved from the first to new one.
        :return: Returns True if the operation is successful, False otherwise
        """
        database_name  = Helper.format_database_name(database_name)
        old_table_name = Helper.format_table_name(old_table_name)
        new_table_name = Helper.format_table_name(new_table_name)
        if new_database_name is None:
            new_database_name = database_name
        else:
            new_database_name = Helper.format_database_name(new_database_name)

        if not self.table_exists(database_name, old_table_name):
            Helper.print_error('Table %s.%s doesn\'t exist!' % (database_name, old_table_name))
            return False

        if self.table_exists(new_database_name, new_table_name):
            backup_name = Helper.format_backup_table_name(new_table_name)
            Helper.print_warning('Table %s.%s already exists. Previous table saved to %s.%s' % \
                                 (new_database_name, new_table_name, new_database_name, backup_name))
            self.rename_table(new_database_name, new_table_name, backup_name)

        sql = 'RENAME TABLE %s.%s TO %s.%s' % (database_name, old_table_name, new_database_name, new_table_name)
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.rename_table, database_name, old_table_name, new_table_name, new_database_name)
            Helper.print_database_error('renaming table %s.%s to %s.%s' % \
                                        (database_name, old_table_name, new_database_name, new_table_name), sql, [], e)
            return False

    def drop_table(self, database_name, table_name):
        """
        Drop the table with the given name from the given database.
        :param database_name: Name of the database to modify.
        :param table_name: Name of the table to drop
        :return: Returns True if the operation is successful, False otherwise
        """
        database_name = Helper.format_database_name(database_name)
        table_name    = Helper.format_table_name(table_name)
        sql           = 'DROP TABLE %s.%s' % (database_name, table_name)
        try:
            self.__cur.execute(sql,)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.drop_table, database_name, table_name)
            Helper.print_database_error('dropping table %s.%s' % (database_name, table_name), sql, [], e)
            return False

    def table_columns(self, database_name, table_name):
        """
        Get the names of the columns in the given table.
        :param database_name: Name of the database to look into.
        :param table_name:    Name of the table to look into.
        :return: The column names or an empty list if an error occurs.
        """
        database_name = Helper.format_database_name(database_name)
        table_name    = Helper.format_table_name(table_name)
        sql           = 'SELECT column_name FROM information_schema.columns WHERE table_schema=\'%s\' AND table_name=\'%s\'' % \
                        (database_name[1:-1], table_name[1:-1])
        try:
            self.__cur.execute(sql)
            self.__database_connector.commit()
            return [row[0] for row in self]
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.table_columns, database_name, table_name)
            Helper.print_database_error('looking for columns list in %s.%s' % (database_name, table_name), sql, [], e)
            return []

    def insert_into_database(self, database_name, table_name, column_names, values, on_duplicate_update=[]):
        """
        Insert the given values into the database. If the table contains a unique key and the corresponding value
        already exists in the table, then the query is ignored.
        :param database_name: Name of the database in which insert entry
        :param table_name: Name of the database in which insert entry
        :param column_names: Name of the columns in which values will be inserted
        :param values: List (or singleton) of values to insert. Each row must have the same size than column_names.
        :param on_duplicate_update: If not empty and if the table already contains an entry with the same unique values
            than the provided ones, the columns defined by the indexes contained in on_duplicate_update will be updated.
        :return: Returns True if the operation is successful, False otherwise
        """

        # Check that both lists have the same size
        database_name       = Helper.format_database_name(database_name)
        table_name          = Helper.format_table_name(table_name)
        column_names        = Helper.init_list(column_names)
        values              = Helper.init_double_list(values)
        on_duplicate_update = Helper.init_list(on_duplicate_update)
        if len(column_names) != len(values[0]):
            Helper.print_error('column_names size and values size mismatch!\n   column_names: %s'
                               '\n   values      : %s' % (str(column_names), str(values[0])))
            return False

        # Format the sql
        if len(on_duplicate_update) > 0:
            sql = 'INSERT INTO %s.%s SET ' % (database_name, table_name)
            for name in column_names:
                sql += name + '=%s, '
            sql = sql[0:-2] + ' ON DUPLICATE KEY UPDATE '
            for col in on_duplicate_update:
                sql += column_names[col] + '=%s, '
            sql = sql[0:-2]
        else:
            sql = 'INSERT IGNORE INTO %s.%s SET ' % (database_name, table_name)
            for name in column_names:
                sql += name + '=%s, '
            sql = sql[0:-2]

        # Execute the query
        try:
            if len(on_duplicate_update) > 0:
                for entry in values:
                    self.__cur.execute(sql, entry + [entry[col] for col in on_duplicate_update])
            else:
                for entry in values:
                    self.__cur.execute(sql, entry)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.insert_into_database, database_name, table_name, column_names, values)
            Helper.print_database_error('inserting into %s.%s' % (database_name, table_name), sql, values, e)
            return False

    def update_database(self, to_update, table_query=None, **kwargs):
        """
        Update entries matching the given table_query (or the one created using the given arguments)
        :param to_update: List of pair (or singleton) ['column_name', new_value]
        :param table_query: A TableQuery object specifying the entries to update
        :param kwargs: Parameters to create a TableQuery if non is provided (see DatabaseWrapper.create_table_query)
        :return: Returns if the operation is successful
        """

        if table_query is None:
            table_query = TableQuery(**kwargs)

        # Check that the parameters are valid
        to_update = Helper.init_double_list(to_update)
        for row in to_update:
            if len(row) != 2 or not isinstance(row[0], basestring):
                print Helper.print_error('sql.to_update rows have to be [column_name, new_value]!')
                return False
        if not table_query.is_valid():
            return False

        # Prepare sql query
        sql = 'UPDATE %s.%s AS t_0_0 SET ' % (table_query.database_name, table_query.table_name)
        for row in to_update:
            sql += 't_0_0.%s=%%s, ' % row[0]
        sql = sql[0: -2] + table_query.format_condition_statement()

        # Execute the query
        values = Helper.get_list_values(to_update) + table_query.get_values()
        try:
            self.__cur.execute(sql, values)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.update_database, to_update, table_query)
            Helper.print_database_error('updating %s.%s' % (table_query.database_name, table_query.table_name), sql, values, e)
            return False

    def delete_from_database(self, table_query=None, **kwargs):
        """
        Removes entries matching the given table_query (or the one created using the given arguments)
        :param table_query: A TableQuery object specifying the entries to delete
        :param kwargs: Parameters to create a TableQuery if non is provided (see DatabaseWrapper.create_table_query)
        :return: Returns if the operation is successful
        """

        if table_query is None:
            table_query = TableQuery(**kwargs)

        # Check that the parameters are valid
        if not table_query.is_valid():
            return False

        # Prepare sql query
        sql = 'DELETE t_0_0'
        for j, join_entry in enumerate(table_query.join):
            sql += ', t_%d_1' % j
        sql += ' FROM %s.%s AS t_0_0%s' % (table_query.database_name, table_query.table_name,
                                           table_query.format_condition_statement())

        # Execute the query
        values = table_query.get_values()
        try:
            self.__cur.execute('USE ' + table_query.database_name)
            self.__cur.execute(sql, values)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.delete_from_database, table_query)
            Helper.print_database_error('deleting from %s.%s' % (table_query.database_name, table_query.table_name), sql, values, e)
            return False

    def fetch_from_database(self, table_query=None, **kwargs):
        """
        Fetch entries matching the given table_query (or the one created using the given arguments)
        :param table_query: A TableQuery object specifying the entries to fetch
        :param kwargs: Parameters to create a TableQuery if non is provided (see DatabaseWrapper.create_table_query)
        :return: Returns if the operation is successful
        """

        if table_query is None:
            table_query = TableQuery(**kwargs)

        # Check that the parameters are valid
        if not table_query.is_valid():
            return False

        # Execute the query
        sql    = table_query.format_fetch_statement()
        values = table_query.get_values()
        try:
            self.__cur.execute(sql, values)
            self.__database_connector.commit()
            return True
        except MySQLdb.Error as e:
            if e.args[0] in DatabaseWrapper.__CONNECTION_ERRORS__:
                return self.__reconnect_and_retry(e, self.fetch_from_database, table_query)
            Helper.print_database_error('fetching from %s.%s' % (table_query.database_name, table_query.table_name),
                                        sql, values, e)
            return False

    def fetchone(self):
        return self.__cur.fetchone()

    def fetchall(self):
        return self.__cur.fetchall()

    def row_count(self):
        return self.__cur.rowcount()

