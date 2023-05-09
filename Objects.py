import mysql.connector
from credentials import env
from DatabaseExceptions import *


class DbInteractor:
    connection = mysql.connector.connect(host=env['Host'],
                               database=env['Database'],
                               user=env['User'],
                               password=env['Password'])
    cursor = connection.cursor()


class Utitliy:
    @staticmethod
    def getNotNullAbsenceExceptionMessage(tableName: str, columnName : str) -> str:
        """Generates the message to display at exception for not passing `not null` columns
        """
        return f"Table : {tableName}, Column: {columnName} is `not null`."

    def get_user_count(conn, tableName: str) -> int:
        """Returns count of rows in the `tableName`
        """
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {tableName}")
        return cursor.fetchone()[0]

class SqlGenerator:
    """Generated the SQL Generator
    """
    @staticmethod
    def insert(tableName: str, columnNames: tuple, values : tuple):
        """Returns `insert` query for given `table`, `columns` and `values'
        """
        if len(columnNames) != len(values):
            raise ColumnValueMisMatchException("Bad SQL Generate Request")
        return f'INSERT INTO {tableName} ({", ".join(columnNames)}) VALUES {values};'
    
    def delete_(tableName: str, columnNames: tuple, values : tuple):
        """Returns `delete` query for given `table`, `columns` and `values'
        """
        if len(columnNames) != len(values):
            raise ColumnValueMisMatchException("Bad SQL Generate Request")
        
        condition = list(f"{col} = '{val}'" for col, val in zip(columnNames, values))
        
        return f'DELETE FROM {tableName} WHERE {" and ".join(condition)};'
    
    def selectAll(tableName: str, columnNames: tuple):
        """Returns `select` query for given `table` displaying given `columns`"""
        return f'SELECT {", ".join(columnNames)} FROM {tableName};'
    
    def select(tableName: str, columnNames: list, values : list, selectColumns : list):
        """Returns `select` query for given `table`, `columns` and `values'
        """
        if len(columnNames) != len(values):
            raise ColumnValueMisMatchException("Bad SQL Generate Request")
        
        toSelect = '*' if len(selectColumns) == 0 else ", ".join(selectColumns)



        condition = list(f"{col} = '{val}'" for col, val in zip(columnNames, values))



        theCondition = f' WHERE {" and ".join(list(condition))}' if len(list(condition)) > 0 else ""



        return f'SELECT {toSelect} FROM {tableName} {theCondition};'

    def update(tableName: str, id: int,  columnNames: tuple, values : tuple):
        """Generates an update SQL query"""
        if len(columnNames) != len(values):
            raise ColumnValueMisMatchException("Bad SQL Generate Request")
        
        condition = list(f"{col} = '{val}'" for col, val in zip(columnNames, values))
        
        return f'UPDATE {tableName} SET {", ".join(condition)} WHERE CustomerId = {id};'


class Customer:
    """
The object that defines the customer class and contains the methods to `CRUD` customers.
    """
    columns = (
        'customerid',
        'name',
        'address',
        'email',
        'phone'
    )

    notNullFields = (
        'name',
        'email',
        'phone'
    )

    

    @staticmethod
    def createSingleRecord(**kwargs : str) -> str:
        """Creates a new Customer and commits into the database, requires `name`, `email`, `phone` and `address`. Avoids commiting `duplicate records`
         
 Returns `values` committed or avoided"""
        if len(kwargs) == 0:
            raise NoRequestException("No request Found !")
    
        for col in Customer.notNullFields:
            if (not kwargs.get(col)):
                raise NotEnoughFieldException(Utitliy.getNotNullAbsenceExceptionMessage(col, 'Name'))

        values = tuple(kwargs[key] for key in kwargs if key.lower() in Customer.columns)
        columns = tuple(filter(lambda x : x in Customer.columns, kwargs.keys()))



        DbInteractor.cursor.execute(SqlGenerator.selectAll("Customer", columns))
        rows = DbInteractor.cursor.fetchall()

        if (values in rows):
            # print(vars(Customer).keys())
            print("\nWarning : Data already in the table\nNothing Comitted !\n")
            return values

        sql = SqlGenerator.insert('Customer', columns, values)

         
        rowCountBefore = Utitliy.get_user_count(DbInteractor.connection, 'Customer')
        DbInteractor.cursor.execute(sql)

        DbInteractor.connection.commit()
        rowCountAfter = Utitliy.get_user_count(DbInteractor.connection, 'Customer')

        assert rowCountAfter == rowCountBefore + 1, "User count did not increase as expected !"



        print(f'Inserted : {values} into Customers')

        return values

    @staticmethod
    def deleteRecord(**kwargs : str) -> str:
        """Deletes all records on `Customer` and commits into the database
        """
        if len(kwargs) == 0:
            raise NoRequestException("No request Found !")
        

        values = tuple(kwargs[key] for key in kwargs if key.lower() in Customer.columns)
        columns = tuple(filter(lambda x : x in Customer.columns, kwargs.keys()))



        DbInteractor.cursor.execute(SqlGenerator.selectAll("Customer", columns))
        rows = DbInteractor.cursor.fetchall()

        if (values not in rows):
            print("\nWarning : Data not in the table\nNothing Comitted !\n")
            return values

        sql = SqlGenerator.delete_('Customer', columns, values)

         
        # print(sql)
        DbInteractor.cursor.execute(sql)
        DbInteractor.connection.commit()

        # print(sql)
        print(f'Deleted : {values} from Customers')


        return values
    

    @staticmethod
    def readRecord(selectColumns : list = [], **kwargs : str) -> str:
        """Retrives all records on `Customer` and commits into the database
specify `selectColumns` to select selective columns
        """

        
        values = tuple(kwargs[key] for key in kwargs if key.lower() in Customer.columns)
        columns = tuple(filter(lambda x : x in Customer.columns, kwargs.keys()))


        # print(SqlGenerator.select("Customer", columns, values, selectColumns))

        DbInteractor.cursor.execute(SqlGenerator.select("Customer", columns, values, selectColumns))
        rows = DbInteractor.cursor.fetchall()

        print("\nRead Result:")
        for i in rows:
            print(i)

        # print(sql)
        # print(f'Deleted : {values} from Customers')


        return values


    @staticmethod
    def updateRecord(id : int, **kwargs : str) -> str:
        """Updates record on `Customer` with id = `id` and commits into the database
        """
        if len(kwargs) == 0:
            raise NoRequestException("No request Found !")
        

        values = tuple(kwargs[key] for key in kwargs if key.lower() in Customer.columns)
        columns = tuple(filter(lambda x : x in Customer.columns, kwargs.keys()))


        # print(SqlGenerator.select('Customer', ['CustomerID'], [id], []))

        DbInteractor.cursor.execute(SqlGenerator.select('Customer', ['CustomerID'], [id], []))
        rows = DbInteractor.cursor.fetchall()

        if (len(rows) == 0):
            print("\nWarning : Data not in the table\nNothing Comitted !\n")
            return values

        sql = SqlGenerator.update('Customer', id, columns, values)

         
        # print(sql)
        DbInteractor.cursor.execute(sql)
        DbInteractor.connection.commit()

        # print(sql)
        print(f'Updated : {values} from Customers with ID = {id}')


        return values




# assert 1 == 1 + 1, "User count did not increase as expected"


