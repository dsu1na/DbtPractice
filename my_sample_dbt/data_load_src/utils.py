import psycopg2
import logging
from typing import List

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DatabaseConnection:
    """
    A class to manage database connections.
    """
    def __init__(self,
                 host: str,
                 database: str,
                 user: str,
                 password: str,
                 logger: logging = logger,
                 **kwargs
                 ) -> None:
        """
        Initialize the DatabaseConnection instance.
        Args:
            host (str): The hostname of the PostgreSQL server.
            database (str): The name of the database to connect to.
            user (str): The username to authenticate with.
            password (str): The password to authenticate with.
            logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
            **kwargs: Additional keyword arguments for future extensions.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.logger = logger
        
    def __enter__(self):
        """
        Enter the runtime context related to this object.
        Returns:
            psycopg2.extensions.connection: A connection object to the PostgreSQL database.
        """
        self.connection = self.create_connection_postgres()
        self.logger.info("Connection to PostgreSQL database established successfully.")
        return self.connection
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and close the database connection.
        Args:
            exc_type: The exception type.
            exc_value: The exception value.
            traceback: The traceback object.
        """
        self.logger.info("Closing connection to PostgreSQL database...")
        self.connection.close()
    
    def create_connection_postgres(self)-> psycopg2.extensions.connection:
        """
        Create a connection to a PostgreSQL database.
        Returns:
            psycopg2.extensions.connection: A connection object to the PostgreSQL database.
        """
        connection = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        return connection

class DDLQueries:
    """
    A class to hold DDL queries for table creation.
    """
    kaggle_queries = {
        "IPL": [
            {
                "table_name": "teams",
                "ddl_query": """
                    Team_Id INT PRIMARY KEY,
                    Team_Name VARCHAR(50) NOT NULL,
                    Team_Short_Code VARCHAR(10) NOT NULL
                """,
                "csv_path": "./database_data_csv/Team.csv"
            },
            {
                "table_name": "seasons",
                "ddl_query": """
                    Season_Id INT PRIMARY KEY,
                    Season_Year INT NOT NULL,
                    Orange_Cap_Id INT,
                    Purple_Cap_Id INT,
                    Man_of_the_Series_Id INT
                """,
                "csv_path": "./database_data_csv/Season.csv"
            },
            {
                "table_name": "players",
                "ddl_query": """
                    Player_Id INT PRIMARY KEY,
                    Player_Name VARCHAR(100),
                    DOB VARCHAR(20),
                    Batting_Hand VARCHAR(20),
                    Bowling_Skill VARCHAR(50),
                    Country VARCHAR(50),
                    Is_Umpire BOOLEAN,
                    Dummy_Col INT
                """,
                "csv_path": "./database_data_csv/Player.csv"
            },
            {
                "table_name": "player_matches",
                "ddl_query": """
                    Match_Id INT,
                    Player_Id INT,
                    Team_Id INT,
                    Is_Keeper BOOLEAN,
                    Is_Captain BOOLEAN
                """,
                "csv_path": "./database_data_csv/Player_Match.csv"
            },
            {
                "table_name": "matches",
                "ddl_query": """
                    Match_Id INT PRIMARY KEY,
                    Match_Date VARCHAR(20),
                    Team_Name_Id INT,
                    Opponent_Team_Id INT,
                    Season_Id INT,
                    Venue_Name VARCHAR(100),
                    Toss_Winner_Id INT,
                    Toss_Decision VARCHAR(10),
                    IS_Superover BOOLEAN,
                    IS_Result BOOLEAN,
                    Is_DuckWorthLewis BOOLEAN,
                    Win_Type VARCHAR(20),
                    Won_By VARCHAR(20),
                    Match_Winner_Id INT,
                    Man_Of_The_Match_Id INT,
                    First_Umpire_Id INT,
                    Second_Umpire_Id INT,
                    City_Name VARCHAR(50),
                    Host_Country VARCHAR(50)
                """,
                "csv_path": "./database_data_csv/Match.csv"
            },
            {
                "table_name": "ball_by_ball",
                "ddl_query": """
                    Match_Id INT,
                    Innings_Id INT,
                    Over_Id INT,
                    Ball_Id INT,
                    Team_Batting_Id INT,
                    Team_Bowling_Id INT,
                    Striker_Id INT,
                    Striker_Batting_Position VARCHAR(10),
                    Non_Striker_Id VARCHAR(20),
                    Bowler_Id VARCHAR(20),
                    Batsman_Scored VARCHAR(10),
                    Extra_Type VARCHAR(20),
                    Extra_Runs VARCHAR(20),
                    Player_dissimal_Id VARCHAR(20),
                    Dissimal_Type VARCHAR(30),
                    Fielder_Id VARCHAR(20)
                """,
                "csv_path": "./database_data_csv/Ball_by_Ball.csv"
            }
        ]
    }

def create_new_database(db_list: List,
                        connection: psycopg2.extensions.connection,
                        logger: logging = logger,
                        **kwargs
                        ) -> None:
    """
    Create a new database in the PostgreSQL server if it does not already exist.
    Args:
        db_list (List): List of the name of the databases to create.
        connection (psycopg2.extensions.connection): A connection object to the PostgreSQL server.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        None
    """
    for db_name in db_list:
        try:
            connection.autocommit = True
            cursor = connection.cursor()
            drop_db_query = f"DROP DATABASE IF EXISTS {db_name};"
            cursor.execute(drop_db_query)
            create_db_query = f"CREATE DATABASE {db_name};"
            cursor.execute(create_db_query)
            logger.info(f"Database '{db_name}' created successfully.")
            cursor.close()
        except Exception as e:
            logger.error(f"Error creating database '{db_name}': {e}")
        
def create_schema(list_of_schemas: List,
                  connection: psycopg2.extensions.connection,
                  logger: logging = logger,
                  **kwargs
                  ) -> None:
    """
    Create a schema in the PostgreSQL database if it does not already exist.
    Args:
        list_of_schemas (List): List of the name of the schemas to create.
        connection (psycopg2.extensions.connection): A connection object to the PostgreSQL database.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        None
    """
    for name_of_schema in list_of_schemas:
        try:
            cursor = connection.cursor()
            drop_schema_query = f"DROP SCHEMA IF EXISTS {name_of_schema} CASCADE;"
            cursor.execute(drop_schema_query)
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {name_of_schema};"
            cursor.execute(create_schema_query)
            connection.commit()
            cursor.close()
            logger.info(f"Schema '{name_of_schema}' created or already exists.")
        except Exception as e:
            logger.error(f"Error creating schema '{name_of_schema}': {e}")
            
def create_table(connection: psycopg2.extensions.connection,
                 ddl_query: str,
                 schema_name: str,
                 table_name: str,
                 logger: logging = logger,
                 **kwargs
                 ) -> None:
    """
    This function creates a table in the specified schema using the provided DDL query.
    Args:
        connection (psycopg2.extensions.connection): A connection object to the PostgreSQL database.
        ddl_query (str): The DDL query string defining the table structure.
        schema_name (str): The name of the schema where the table will be created.
        table_name (str): The name of the table to be created.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        None
    """
    try:
        cursor = connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {ddl_query}
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info(f"Table '{schema_name}.{table_name}' created or already exists.")
    except Exception as e:
        logger.error(f"Error creating table '{schema_name}.{table_name}': {e}")
        
def load_csv_to_table(connection: psycopg2.extensions.connection,
                      csv_file_path: str,
                      destination_schema: str,
                      destination_table: str,
                      logger: logging = logger,
                      **kwargs
                      ) -> None:
    """
    This function loads data from a CSV file into the specified table.
    Args:
        connection (psycopg2.extensions.connection): A connection object to the PostgreSQL database.
        csv_file_path (str): The file path of the CSV file to be loaded.
        destination_schema (str): The schema where the destination table is located.
        destination_table (str): The name of the destination table.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        None
    """
    try:
        cursor = connection.cursor()
        logger.info(f"Truncating table '{destination_schema}.{destination_table}' before loading new data...")
        cursor.execute(f"TRUNCATE TABLE {destination_schema}.{destination_table};")
        
        copy_sql_query = f""" COPY {destination_schema}.{destination_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' """
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql_query, f)
        connection.commit()
        cursor.close()
        logger.info(f"Data from '{csv_file_path}' loaded into '{destination_schema}.{destination_table}'.")
    except Exception as e:
        logger.error(f"Error loading data into '{destination_schema}.{destination_table}': {e}")