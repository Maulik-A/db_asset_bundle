from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from faker import Faker
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

fake = Faker()

def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


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
        dob = date.fromisiformat(value)
        if dob > date.today():
            raise ValueError("Date of birth cannot be in the future.")
        if value.year <1900:
            raise ValueError("Year of birth must be greater than 1900.")
    except ValueError:
        raise ValueError("Invalid date format. Please use 'YYYY-MM-DD'")


@dataclass
class Customer:
    """Class for customer data."""
    user_id: int
    fname: str
    lname: str
    gender: str = field(metadata={"validate": validate_gender})
    address: str
    contact_number: int = field(metadata={"validate": validate_contact_number})
    email_address: str = field(init=False)
    date_of_birth: date = field(metadata={"validate": validate_date_of_birth})

    def __post_init__(self):
        self.email_address = self.fname.lower()+"_"+self.lname.lower()+"@"+fake.free_email_domain()
        self.gender = validate_gender(self.gender)
        self.contact_number = validate_contact_number(self.contact_number)



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
            order_placed_date= fake.past_datetime(start_date= "-1d").strftime("%d/%m/%Y,%H:%M:%S")
        )
        orders.append(asdict(order))

    return orders


def main(spark: SparkSession):
    """Main function will generate random 100 datapoints and store it in bronze tables."""
    customer_final_df = None
    order_final_df = None

    for i in range(100):
        order_id= random.randrange(1111111, 9999999, 7)
        num_of_order = random.randint(1,5)

        # Create customer and order data
        customer = create_customer()
        orders = create_orders(num_of_order,customer['user_id'],order_id)
        
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

    customer_final_df.write.mode('append').format('delta').saveAsTable("mycatalog.dev.customer_bronze")
    order_final_df.write.mode('append').format('delta').saveAsTable("mycatalog.dev.orders_bronze")


if __name__ == "__main__":
    main(get_spark())
    print("Data imported successfully to bronze tables.")