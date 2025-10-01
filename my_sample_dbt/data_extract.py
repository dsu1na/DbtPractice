import psycopg2
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_connection_postgres(host: str,
                               database: str,
                               user: str,
                               password: str,
                               logger: logging = logger,
                               **kwargs
                               ) -> psycopg2.extensions.connection:
    """
    Create a connection to a PostgreSQL database.
    Args:
        host (str): The hostname of the PostgreSQL server.
        database (str): The name of the database to connect to.
        user (str): The username to authenticate with.
        password (str): The password to authenticate with.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connection to PostgreSQL database established successfully.")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        return None
    
def create_schema(name_of_schema: str,
                  connection: psycopg2.extensions.connection,
                  logger: logging = logger,
                  **kwargs
                  ) -> None:
    """
    Create a schema in the PostgreSQL database if it does not already exist.
    Args:
        name_of_schema (str): The name of the schema to create.
        connection (psycopg2.extensions.connection): A connection object to the PostgreSQL database.
        logger (logging, optional): Logger for logging messages. Defaults to a basic logger.
        **kwargs: Additional keyword arguments for future extensions.
    Returns:
        None
    """
    try:
        cursor = connection.cursor()
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {name_of_schema};"
        cursor.execute(create_schema_query)
        connection.commit()
        cursor.close()
        logger.info(f"Schema '{name_of_schema}' created or already exists.")
    except Exception as e:
        logger.error(f"Error creating schema '{name_of_schema}': {e}")

def create_staging_table(connection: psycopg2.extensions.connection,
                         logger: logging = logger,
                         **kwargs
                         ) -> None:
    """
    """
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS STAGING.customer_table (
            customer_id SERIAL PRIMARY KEY,
            gender VARCHAR(10),
            age INTEGER,
            annual_income INTEGER,
            spending_score INTEGER,
            profession VARCHAR(20),
            work_experience INTEGER,
            family_size INTEGER
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info("Table 'STAGING.sample_table' created or already exists.")
    except Exception as e:
        logger.error(f"Error creating table 'STAGING.sample_table': {e}")

def load_csv_to_staging_table(connection: psycopg2.extensions.connection,
                              csv_file_path: str,
                              destination_table: str,
                              logger: logging = logger,
                              **kwargs
                              ) -> None:
    """
    """
    try:
        cursor = connection.cursor()
        copy_sql_query = f""" COPY {destination_table} FROM STDIN WITH CSV HEADER DELIMITER AS ',' """
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql_query, f)
        connection.commit()
        cursor.close()
        logger.info(f"Data from '{csv_file_path}' loaded into 'STAGING.sample_table'.")
    except Exception as e:
        logger.error(f"Error loading data into 'STAGING.sample_table': {e}")

def main():
    """
    """
    
    host_name = "postgres"
    database_name = "dbt_database"
    user_name = "postgres"
    password = "postgres"
    
    try:
        # create connection
        logger.info("Creating connection to PostgreSQL database...")
        connection = create_connection_postgres(host=host_name,
                                                database=database_name,
                                                user=user_name,
                                                password=password,
                                                logger=logger
                                                )
        
        if connection:
            logger.info("Creating STAGING schema in PostgreSQL database...")
            create_schema(name_of_schema="STAGING",
                        connection=connection,
                        logger=logger
                        )
            
            logger.info("Creating staging table in PostgreSQL database...")
            create_staging_table(connection=connection,
                                logger=logger
                                )
            
            logger.info("Loading data into staging table in PostgreSQL database...")
            load_csv_to_staging_table(connection=connection,
                                    csv_file_path="./Customers.csv",
                                    destination_table="STAGING.customer_table",
                                    logger=logger
                                    )  
    
    finally:
        logger.info("Closing connection to PostgreSQL database...")
        connection.close()

if __name__ == "__main__":
    main()
    
