from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from faker import Faker
import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp
import logging

fake = Faker()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

def check_catalog_exists(spark: SparkSession, catalog_name: str) -> bool:
    """Check if a catalog exists in Databricks."""
    try:
        catalogs = spark.sql(f"SHOW CATALOGS").collect()
        catalog_names = [row.catalog for row in catalogs]
        return catalog_name in catalog_names
    except Exception as e:
        logger.warning(f"Could not check catalog existence: {e}")
        return False

def check_schema_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    """Check if a schema exists in the specified catalog."""
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_names = [row.databaseName for row in schemas]
        return schema_name in schema_names
    except Exception as e:
        logger.warning(f"Could not check schema existence: {e}")
        return False

def check_table_exists(spark: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> bool:
    """Check if a table exists in the specified schema."""
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
        table_names = [row.tableName for row in tables]
        return table_name in table_names
    except Exception as e:
        logger.warning(f"Could not check table existence: {e}")
        return False

def create_catalog_if_not_exists(spark: SparkSession, catalog_name: str) -> bool:
    """Create a catalog if it doesn't exist."""
    try:
        if not check_catalog_exists(spark, catalog_name):
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            logger.info(f"Created catalog: {catalog_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create catalog {catalog_name}: {e}")
        return False

def create_schema_if_not_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    """Create a schema if it doesn't exist."""
    try:
        if not check_schema_exists(spark, catalog_name, schema_name):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
            logger.info(f"Created schema: {catalog_name}.{schema_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create schema {catalog_name}.{schema_name}: {e}")
        return False

def create_table_if_not_exists(spark: SparkSession, catalog_name: str, schema_name: str, table_name: str, 
                             table_type: str = "customer") -> bool:
    """Create a table if it doesn't exist based on the table type."""
    try:
        if not check_table_exists(spark, catalog_name, schema_name, table_name):
            if table_type == "customer":
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
                    user_id INT,
                    fname STRING,
                    lname STRING,
                    gender STRING,
                    address STRING,
                    contact_number STRING,
                    email_address STRING,
                    date_of_birth DATE,
                    created_date TIMESTAMP
                ) USING DELTA
                """
            elif table_type == "orders":
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
                    order_id INT,
                    user_id INT,
                    item_id INT,
                    num_of_items INT,
                    discount INT,
                    order_placed_date TIMESTAMP,
                    created_date TIMESTAMP
                ) USING DELTA
                """
            else:
                raise ValueError(f"Unknown table type: {table_type}")
            
            spark.sql(create_sql)
            logger.info(f"Created table: {catalog_name}.{schema_name}.{table_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create table {catalog_name}.{schema_name}.{table_name}: {e}")
        return False

def ensure_table_structure(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    """Ensure that the catalog, schema, and tables exist."""
    try:
        # Create catalog if it doesn't exist
        if not create_catalog_if_not_exists(spark, catalog_name):
            logger.error(f"Failed to create or verify catalog: {catalog_name}")
            return False
        
        # Create schema if it doesn't exist
        if not create_schema_if_not_exists(spark, catalog_name, schema_name):
            logger.error(f"Failed to create or verify schema: {catalog_name}.{schema_name}")
            return False
        
        # Create tables if they don't exist
        if not create_table_if_not_exists(spark, catalog_name, schema_name, "customer_bronze", "customer"):
            logger.error(f"Failed to create customer table")
            return False
        
        if not create_table_if_not_exists(spark, catalog_name, schema_name, "orders_bronze", "orders"):
            logger.error(f"Failed to create orders table")
            return False
        
        logger.info("All required tables and schemas are ready")
        return True
        
    except Exception as e:
        logger.error(f"Error ensuring table structure: {e}")
        return False

def validate_gender(value:str) -> str:
    '''This function will check if gender is in ['M','F']'''
    if value not in {"M","F"}:
        raise ValueError("Gender must be either 'M' or 'F'")
    return value

def validate_contact_number(value:int) -> int:
    '''This function will check if contact number is 10 digit long'''
    if len(str(value)) != 10:
        raise ValueError("Contact number must be 10 digits long.")
    return value

def validate_date_of_birth(value:date) -> date:
    '''This function will check if date of birth is in correct format and not in future'''
    try:
        if isinstance(value, str):
            dob = datetime.strptime(value, '%d%m%Y').date()
        else:
            dob = value
            
        if dob > date.today():
            raise ValueError("Date of birth cannot be in the future.")
        if dob.year < 1900:
            raise ValueError("Year of birth must be greater than 1900.")
        return dob
    except ValueError:
        raise ValueError("Invalid date format. Please use 'DDMMYYYY'")


@dataclass
class Customer:
    """Class for customer data."""
    user_id: int
    fname: str
    lname: str
    gender: str = field(metadata={"validate": validate_gender})
    address: str
    contact_number: str = field(metadata={"validate": validate_contact_number})
    email_address: str = field(init=False)
    date_of_birth: date = field(metadata={"validate": validate_date_of_birth})

    def __post_init__(self):
        self.email_address = self.fname.lower()+"_"+self.lname.lower()+"@"+fake.free_email_domain()
        self.gender = validate_gender(self.gender)
        self.contact_number = validate_contact_number(self.contact_number)
        self.date_of_birth = validate_date_of_birth(self.date_of_birth)



@dataclass
class Order:
    """Class for order data."""
    order_id: int
    user_id: int
    item_id: int
    num_of_items: int
    discount: int
    order_placed_date: datetime


def create_customer():
    """Function to create customer data."""
    cust = Customer(
        user_id= random.randint(1,10000),
        fname = fake.first_name(),
        lname= fake.last_name(),
        gender= fake.random_element(elements=("M","F")),
        address= fake.address(),
        contact_number= fake.msisdn()[3:],
        date_of_birth= fake.date_of_birth(minimum_age=16,maximum_age=90).strftime('%d%m%Y')
    )
    return asdict(cust)


def create_orders(num_of_orders, user_id, order_id):
    """Function to create order data."""
    orders = []
    for _ in range(num_of_orders):
        order = Order(
            order_id= order_id,
            user_id= user_id,
            item_id= random.randint(1,20),
            num_of_items= random.randint(1,5),
            discount= random.randint(1,20),
            order_placed_date= fake.past_datetime(start_date= "-1d").strftime("%Y-%m-%d %H:%M:%S")
        )
        orders.append(asdict(order))

    return orders

def generate_random_data(spark: SparkSession, number_of_datapoints: int = 100):
    """Main function will generate random 100 datapoints and store it in bronze tables."""
    
    customer_final_df = None
    order_final_df = None

    try:
        for i in range(number_of_datapoints):
            order_id = random.randrange(1111111, 9999999, 7)
            num_of_order = random.randint(1, 5)

            # Create customer and order data
            customer = create_customer()
            orders = create_orders(num_of_order, customer['user_id'], order_id)
            
            # Store customer and order data in a dataframe
            customer_df = spark.createDataFrame([customer])
            order_df = spark.createDataFrame(orders)
            
            if customer_final_df is None:
                customer_final_df = customer_df
            else:
                customer_final_df = customer_final_df.union(customer_df)
            
            if order_final_df is None:
                order_final_df = order_df
            else:
                order_final_df = order_final_df.union(order_df)
        
        customer_final_df = customer_final_df.withColumn("created_date", current_timestamp())
        order_final_df = order_final_df.withColumn("created_date", current_timestamp())
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

    return customer_final_df, order_final_df


def write_to_tables(spark: SparkSession, customer_data, order_data):
    catalog_name = "my_catalog"
    schema_name = "dev"

    # Ensure table structure exists
    if not ensure_table_structure(spark, catalog_name, schema_name):
        logger.error("Failed to ensure table structure. Exiting.")
        return
    
    # Write data with error handling
    try:
        customer_data = customer_data.select(
            F.col('user_id').cast('int'),
            F.col('fname'),
            F.col('lname'),
            F.col('gender'),
            F.col('address'),
            F.col('contact_number'),
            F.col('email_address'),
            F.col('date_of_birth').cast('date'),
            F.col('created_date')
        )
        customer_data.write.mode('append').format('delta').saveAsTable(f"{catalog_name}.{schema_name}.customer_bronze")
        logger.info("Successfully wrote customer data to bronze table")
    except Exception as e:
        logger.error(f"Failed to write customer data: {e}")
        raise
    
    try:
        order_data = order_data.select(
            F.col('order_id').cast('int'),
            F.col('user_id').cast('int'),
            F.col('item_id').cast('int'),
            F.col('num_of_items').cast('int'),
            F.col('discount').cast('int'),
            F.col('order_placed_date').cast('timestamp'),
            F.col('created_date').cast('timestamp')
        )
        order_data.write.mode('append').format('delta').saveAsTable(f"{catalog_name}.{schema_name}.orders_bronze")
        logger.info("Successfully wrote order data to bronze table")
    except Exception as e:
        logger.error(f"Failed to write order data: {e}")
        raise

def main(spark: SparkSession):
    customers, orders = generate_random_data(spark, 100)
    write_to_tables(spark, customers, orders)

if __name__ == "__main__":
    try:
        spark = get_spark()
        main(spark)
        print("Data imported successfully to bronze tables.")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        print(f"Failed to import data: {e}")