import wrds
from typing import List

class WRDSClientManager():
    def __init__(cls, username, password) -> None:
        cls.username = username
        cls.password = password


    def create_connection(cls):
        conn = wrds.Connection(wrds_username = cls.username,
                               wrds_password = cls.password)
        return conn



    def read_corporate_bond_data(cls, conn: wrds.sql.Connection,
                                 corporate_bond_variable_list: List):

        """
        *conn: WRDS Database Connection
        *corporate_bond_variable_list: List of variables to query from WRDS bondret table
        """

        try:
            corporate_bond = conn.get_table(library='wrdsapps_bondret',
                                        table = 'bondret',
                                        columns = corporate_bond_variable_list)
            return corporate_bond
        except Exception as e:
            raise Exception(f"Could not generate corporate bond returns table: \n{e}")



    def read_otc_data(cls, conn: wrds.sql.Connection,
                      otc_variable_list: List):

        """
        *conn: WRDS Database Connection
        *otc_variable_list: List of variables to query from WRDS OTC EOD table
        """

        try:
            corporate_bond = conn.get_table(library='otc_endofday',
                                        table = 'endofday',
                                        columns = otc_variable_list)
            return corporate_bond
        except Exception as e:
            raise Exception(f"Could not generate OTC table: \n{e}")












