import psycopg2
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from utils import (DatabaseConnection,
                   create_new_database,
                   create_schema, 
                   create_table, 
                   DDLQueries,
                   load_csv_to_table)




# def main():
#     """
#     """
    
#     host_name = "postgres"
#     database_name = "dbt_database"
#     user_name = "postgres"
#     password = "postgres"
    
#     staging_customers_ddl = """
#         customer_id SERIAL PRIMARY KEY,
#         gender VARCHAR(10),
#         age INTEGER,
#         annual_income INTEGER,
#         spending_score INTEGER,
#         profession VARCHAR(20),
#         work_experience INTEGER,
#         family_size INTEGER
#     """
    
#     try:
#         # create connection
#         logger.info("Creating connection to PostgreSQL database...")
#         connection = create_connection_postgres(host=host_name,
#                                                 database=database_name,
#                                                 user=user_name,
#                                                 password=password,
#                                                 logger=logger
#                                                 )
        
#         if connection:
#             logger.info("Creating STAGING schema in PostgreSQL database...")
#             create_schema(name_of_schema="STAGING",
#                           connection=connection,
#                           logger=logger
#                           )
            
#             logger.info("Creating TRANSFORMATION schema in PostgreSQL database...")
#             create_schema(name_of_schema="TRANSFORMATION",
#                           connection=connection,
#                           logger=logger
#                           )
            
#             logger.info("Creating staging customers table in PostgreSQL database...")
#             create_table(connection=connection,
#                          ddl_query=staging_customers_ddl,
#                          schema_name="STAGING",
#                          table_name="customer_table",
#                          logger=logger
#                          )
            
#             logger.info("Loading data into staging table in PostgreSQL database...")
#             load_csv_to_staging_table(connection=connection,
#                                     csv_file_path="./Customers.csv",
#                                     destination_table="STAGING.customer_table",
#                                     logger=logger
#                                     )  
    
#     finally:
#         logger.info("Closing connection to PostgreSQL database...")
#         connection.close()


def main():
    """
    This is the main function
    """
    
    # create the databases within postgres server
    with DatabaseConnection(host="postgres",
                            database="postgres",
                            user="postgres",
                            password="postgres"
                            ) as postgres_connection:
        logger.info("Creating kaggle_db and dbt_database database object ...")
        create_new_database(db_list=["kaggle_db", "dbt_database"],
                            connection=postgres_connection,
                            )
    
    # Create schemas for kaggle_db
    with DatabaseConnection(host="postgres",
                            database="kaggle_db",
                            user="postgres",
                            password="postgres"
                            ) as kaggle_db_connection:
        logger.info("Creating schemas for kaggle_db database ...")
        create_schema(list_of_schemas=["IPL"],
                      connection=kaggle_db_connection,
                      )
        
        logger.info("Creating tables in the schemas of kaggle_db database ...")
        for items in DDLQueries.kaggle_queries["IPL"]:
            create_table(connection=kaggle_db_connection,
                         ddl_query=items["ddl_query"],
                         schema_name="IPL",
                         table_name=items["table_name"],
                         logger=logger
                         )
        
        logger.info("Loading data into the tables of kaggle_db database ...")
        for items in DDLQueries.kaggle_queries["IPL"]:
            load_csv_to_table(connection=kaggle_db_connection,
                              csv_file_path=items["csv_path"],
                              destination_schema="IPL",
                              destination_table=items["table_name"],
                              logger=logger
                              )
        
        
    # Create schemas for dbt_database
    with DatabaseConnection(host="postgres",
                            database="dbt_database",
                            user="postgres",
                            password="postgres"
                            ) as dbt_database_connection:
        logger.info("Creating schemas for dbt_database database ...")
        create_schema(list_of_schemas=["STAGING", "TRANSFORMATION"],
                      connection=dbt_database_connection,
                      )

if __name__ == "__main__":
    main()
    
