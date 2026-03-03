from pathlib import Path
import pandas as pd
from src.config import DATA_DIR

def load_data():

    dfs = {
        'orders': pd.read_csv(DATA_DIR / 'ecommerce_orders.csv'),
        'order_items': pd.read_csv(DATA_DIR / 'ecommerce_order_items.csv'),
        'products': pd.read_csv(DATA_DIR / 'ecommerce_products.csv'),
        'customers': pd.read_csv(DATA_DIR / 'ecommerce_customers.csv'),
        'categories': pd.read_csv(DATA_DIR / 'ecommerce_categories.csv'),
        'inventory': pd.read_csv(DATA_DIR / 'ecommerce_inventory.csv'),
        'brands': pd.read_csv(DATA_DIR / 'ecommerce_brands.csv'),
        'promotions': pd.read_csv(DATA_DIR / 'ecommerce_promotions.csv'),
        'reviews': pd.read_csv(DATA_DIR / 'ecommerce_reviews.csv'),
        'suppliers': pd.read_csv(DATA_DIR / 'ecommerce_suppliers.csv'),
        'warehouses': pd.read_csv(DATA_DIR / 'ecommerce_warehouses.csv')
    }

    return dfs