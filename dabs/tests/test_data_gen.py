import pytest
import sys
from data_gen.datagen import create_customer, create_orders



def test_create_customer():
    """Check if create_csutomer returns a single list"""
    assert len([create_customer()]) == 1


def test_create_customer_items():
    """Check if crete_customer returns a dict of 8 items"""
    assert isinstance(create_customer(),dict)
    assert len(create_customer()) == 8

@pytest.fixture
def create_number_of_orders():
    return 3

@pytest.fixture
def create_user_id():
    return 1

@pytest.fixture
def create_order_id():
    return 50

def test_create_order(create_number_of_orders, create_user_id, create_order_id):
    """Check if create_orders returns correct number of orders"""
    assert len(create_orders(create_number_of_orders, create_user_id, create_order_id)) == 3


def test_create_order_items(create_number_of_orders, create_user_id, create_order_id):
    """Check if create_orders returns a dict of 6 items"""
    assert isinstance(create_orders(create_number_of_orders, create_user_id, create_order_id),list)
    assert len(create_orders(create_number_of_orders, create_user_id, create_order_id)[0]) == 6
