"""
PROJECT: Enterprise Inventory Management System
TECHNOLOGY: Python + SQLAlchemy ORM
AUTHOR: Demo Backend Engineering Project

FEATURES:
    - SQLAlchemy ORM models
    - Database relationships
    - CRUD operations
    - Transactions
    - Inventory tracking
    - Order processing
    - Supplier management
    - Reporting queries
    - Logging
    - Exception handling
    - Session management

DATABASE:
    SQLite (Can be migrated to PostgreSQL/MySQL)

TABLES:
    - suppliers
    - products
    - warehouses
    - inventory
    - customers
    - orders
    - order_items
"""

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Text,
    func
)

from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker
)

from sqlalchemy.exc import SQLAlchemyError

from datetime import datetime
import logging

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DATABASE_URL = "sqlite:///inventory_management.db"

engine = create_engine(
    DATABASE_URL,
    echo=False
)

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

# ============================================================
# ORM MODELS
# ============================================================

class Supplier(Base):

    __tablename__ = "suppliers"

    supplier_id = Column(Integer, primary_key=True)

    supplier_name = Column(String(100), nullable=False)

    contact_email = Column(String(100))

    city = Column(String(50))

    created_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    products = relationship(
        "Product",
        back_populates="supplier"
    )


class Product(Base):

    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True)

    product_name = Column(String(120), nullable=False)

    category = Column(String(50))

    price = Column(Float, nullable=False)

    supplier_id = Column(
        Integer,
        ForeignKey("suppliers.supplier_id")
    )

    created_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    supplier = relationship(
        "Supplier",
        back_populates="products"
    )

    inventory_items = relationship(
        "Inventory",
        back_populates="product"
    )

    order_items = relationship(
        "OrderItem",
        back_populates="product"
    )


class Warehouse(Base):

    __tablename__ = "warehouses"

    warehouse_id = Column(Integer, primary_key=True)

    warehouse_name = Column(String(100))

    location = Column(String(100))

    inventory_items = relationship(
        "Inventory",
        back_populates="warehouse"
    )


class Inventory(Base):

    __tablename__ = "inventory"

    inventory_id = Column(Integer, primary_key=True)

    product_id = Column(
        Integer,
        ForeignKey("products.product_id")
    )

    warehouse_id = Column(
        Integer,
        ForeignKey("warehouses.warehouse_id")
    )

    stock_quantity = Column(Integer)

    updated_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    product = relationship(
        "Product",
        back_populates="inventory_items"
    )

    warehouse = relationship(
        "Warehouse",
        back_populates="inventory_items"
    )


class Customer(Base):

    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True)

    customer_name = Column(String(100))

    email = Column(String(100))

    city = Column(String(50))

    created_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    orders = relationship(
        "Order",
        back_populates="customer"
    )


class Order(Base):

    __tablename__ = "orders"

    order_id = Column(Integer, primary_key=True)

    customer_id = Column(
        Integer,
        ForeignKey("customers.customer_id")
    )

    order_status = Column(String(50))

    total_amount = Column(Float)

    created_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    customer = relationship(
        "Customer",
        back_populates="orders"
    )

    order_items = relationship(
        "OrderItem",
        back_populates="order"
    )


class OrderItem(Base):

    __tablename__ = "order_items"

    order_item_id = Column(Integer, primary_key=True)

    order_id = Column(
        Integer,
        ForeignKey("orders.order_id")
    )

    product_id = Column(
        Integer,
        ForeignKey("products.product_id")
    )

    quantity = Column(Integer)

    unit_price = Column(Float)

    order = relationship(
        "Order",
        back_populates="order_items"
    )

    product = relationship(
        "Product",
        back_populates="order_items"
    )

# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def initialize_database():
    """
    Create all tables.
    """

    logger.info("Creating database tables")

    Base.metadata.create_all(bind=engine)

    logger.info("Database initialized")

# ============================================================
# SUPPLIER OPERATIONS
# ============================================================

def create_supplier(
    session,
    supplier_name,
    contact_email,
    city
):
    """
    Create new supplier.
    """

    try:

        supplier = Supplier(
            supplier_name=supplier_name,
            contact_email=contact_email,
            city=city
        )

        session.add(supplier)

        session.commit()

        logger.info(
            f"Supplier created: {supplier_name}"
        )

        return supplier

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# PRODUCT OPERATIONS
# ============================================================

def create_product(
    session,
    product_name,
    category,
    price,
    supplier_id
):
    """
    Create new product.
    """

    try:

        product = Product(
            product_name=product_name,
            category=category,
            price=price,
            supplier_id=supplier_id
        )

        session.add(product)

        session.commit()

        logger.info(
            f"Product created: {product_name}"
        )

        return product

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# WAREHOUSE OPERATIONS
# ============================================================

def create_warehouse(
    session,
    warehouse_name,
    location
):
    """
    Create warehouse.
    """

    try:

        warehouse = Warehouse(
            warehouse_name=warehouse_name,
            location=location
        )

        session.add(warehouse)

        session.commit()

        logger.info(
            f"Warehouse created: {warehouse_name}"
        )

        return warehouse

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# INVENTORY MANAGEMENT
# ============================================================

def add_inventory(
    session,
    product_id,
    warehouse_id,
    stock_quantity
):
    """
    Add inventory record.
    """

    try:

        inventory = Inventory(
            product_id=product_id,
            warehouse_id=warehouse_id,
            stock_quantity=stock_quantity
        )

        session.add(inventory)

        session.commit()

        logger.info(
            f"Inventory added for product {product_id}"
        )

        return inventory

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# CUSTOMER MANAGEMENT
# ============================================================

def create_customer(
    session,
    customer_name,
    email,
    city
):
    """
    Create customer record.
    """

    try:

        customer = Customer(
            customer_name=customer_name,
            email=email,
            city=city
        )

        session.add(customer)

        session.commit()

        logger.info(
            f"Customer created: {customer_name}"
        )

        return customer

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# ORDER PROCESSING
# ============================================================

def create_order(
    session,
    customer_id,
    items
):
    """
    Create order with order items.

    items format:
    [
        {
            "product_id": 1,
            "quantity": 2
        }
    ]
    """

    try:

        total_amount = 0

        order = Order(
            customer_id=customer_id,
            order_status="PENDING",
            total_amount=0
        )

        session.add(order)

        session.flush()

        for item in items:

            product = session.query(Product).filter(
                Product.product_id == item["product_id"]
            ).first()

            if not product:
                continue

            item_total = (
                product.price * item["quantity"]
            )

            total_amount += item_total

            order_item = OrderItem(
                order_id=order.order_id,
                product_id=product.product_id,
                quantity=item["quantity"],
                unit_price=product.price
            )

            session.add(order_item)

        order.total_amount = total_amount

        session.commit()

        logger.info(
            f"Order created with ID: {order.order_id}"
        )

        return order

    except SQLAlchemyError as error:

        session.rollback()

        logger.error(error)

# ============================================================
# REPORTING QUERIES
# ============================================================

def top_selling_products(session):
    """
    Generate top selling products report.
    """

    logger.info("Fetching top selling products")

    results = (
        session.query(
            Product.product_name,
            func.sum(OrderItem.quantity).label("units_sold")
        )
        .join(OrderItem)
        .group_by(Product.product_name)
        .order_by(
            func.sum(OrderItem.quantity).desc()
        )
        .all()
    )

    return results


def customer_order_summary(session):
    """
    Generate customer spending summary.
    """

    logger.info("Generating customer summary")

    results = (
        session.query(
            Customer.customer_name,
            func.sum(Order.total_amount).label(
                "total_spent"
            )
        )
        .join(Order)
        .group_by(Customer.customer_name)
        .all()
    )

    return results


def inventory_status_report(session):
    """
    Generate inventory stock report.
    """

    logger.info("Generating inventory report")

    results = (
        session.query(
            Product.product_name,
            Warehouse.warehouse_name,
            Inventory.stock_quantity
        )
        .join(Inventory)
        .join(Warehouse)
        .all()
    )

    return results

# ============================================================
# DATA SEEDING
# ============================================================

def seed_sample_data(session):
    """
    Insert sample records.
    """

    logger.info("Seeding sample data")

    supplier = create_supplier(
        session,
        "Global Tech Supplies",
        "supply@globaltech.com",
        "Mumbai"
    )

    warehouse = create_warehouse(
        session,
        "Central Warehouse",
        "Bangalore"
    )

    customer = create_customer(
        session,
        "Rahul Sharma",
        "rahul@email.com",
        "Delhi"
    )

    product1 = create_product(
        session,
        "Laptop",
        "Electronics",
        65000,
        supplier.supplier_id
    )

    product2 = create_product(
        session,
        "Mechanical Keyboard",
        "Accessories",
        4500,
        supplier.supplier_id
    )

    add_inventory(
        session,
        product1.product_id,
        warehouse.warehouse_id,
        50
    )

    add_inventory(
        session,
        product2.product_id,
        warehouse.warehouse_id,
        120
    )

    create_order(
        session,
        customer.customer_id,
        [
            {
                "product_id": product1.product_id,
                "quantity": 1
            },
            {
                "product_id": product2.product_id,
                "quantity": 2
            }
        ]
    )

# ============================================================
# MAIN APPLICATION
# ============================================================

def run_application():

    logger.info(
        "Inventory Management Application Started"
    )

    initialize_database()

    session = SessionLocal()

    try:

        seed_sample_data(session)

        logger.info("Running reports")

        top_products = top_selling_products(session)

        for product in top_products:

            logger.info(product)

        customer_summary = customer_order_summary(
            session
        )

        for customer in customer_summary:

            logger.info(customer)

        inventory_report = inventory_status_report(
            session
        )

        for item in inventory_report:

            logger.info(item)

    finally:

        session.close()

        logger.info("Database session closed")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    logger.info(
        f"Process started at {datetime.now()}"
    )

    run_application()

    logger.info(
        f"Process completed at {datetime.now()}"
    )