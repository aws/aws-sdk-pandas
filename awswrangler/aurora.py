from typing import Union
import logging

import pg8000  # type: ignore
import pymysql  # type: ignore

from awswrangler.exceptions import InvalidEngine

logger = logging.getLogger(__name__)


class Aurora:
    def __init__(self, session):
        self._session = session

    @staticmethod
    def _validate_connection(database: str,
                             host: str,
                             port: Union[str, int],
                             user: str,
                             password: str,
                             engine: str = "mysql",
                             tcp_keepalive: bool = True,
                             application_name: str = "aws-data-wrangler-validation",
                             validation_timeout: int = 5) -> None:
        if "postgres" in engine.lower():
            conn = pg8000.connect(database=database,
                                  host=host,
                                  port=int(port),
                                  user=user,
                                  password=password,
                                  ssl=True,
                                  application_name=application_name,
                                  tcp_keepalive=tcp_keepalive,
                                  timeout=validation_timeout)
        elif "mysql" in engine.lower():
            conn = pymysql.connect(database=database,
                                   host=host,
                                   port=int(port),
                                   user=user,
                                   password=password,
                                   program_name=application_name,
                                   connect_timeout=validation_timeout)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        conn.close()

    @staticmethod
    def generate_connection(database: str,
                            host: str,
                            port: Union[str, int],
                            user: str,
                            password: str,
                            engine: str = "mysql",
                            tcp_keepalive: bool = True,
                            application_name: str = "aws-data-wrangler",
                            connection_timeout: int = 1_200_000,
                            validation_timeout: int = 5):
        """
        Generates a valid connection object

        :param database: The name of the database instance to connect with.
        :param host: The hostname of the Aurora server to connect with.
        :param port: The TCP/IP port of the Aurora server instance.
        :param user: The username to connect to the Aurora database with.
        :param password: The user password to connect to the server with.
        :param engine: "mysql" or "postgres"
        :param tcp_keepalive: If True then use TCP keepalive
        :param application_name: Application name
        :param connection_timeout: Connection Timeout
        :param validation_timeout: Timeout to try to validate the connection
        :return: PEP 249 compatible connection
        """
        Aurora._validate_connection(database=database,
                                    host=host,
                                    port=port,
                                    user=user,
                                    password=password,
                                    engine=engine,
                                    tcp_keepalive=tcp_keepalive,
                                    application_name=application_name,
                                    validation_timeout=validation_timeout)
        if "postgres" in engine.lower():
            conn = pg8000.connect(database=database,
                                  host=host,
                                  port=int(port),
                                  user=user,
                                  password=password,
                                  ssl=True,
                                  application_name=application_name,
                                  tcp_keepalive=tcp_keepalive,
                                  timeout=connection_timeout)
        elif "mysql" in engine.lower():
            conn = pymysql.connect(database=database,
                                   host=host,
                                   port=int(port),
                                   user=user,
                                   password=password,
                                   program_name=application_name,
                                   connect_timeout=connection_timeout)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        return conn
