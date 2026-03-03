import pandas as pd

# Nulls treatment/strategy -----------------------------------------------------------------------------------------------

def clean_nulls(dfs: dict[str,pd.DataFrame]) -> dict[str,pd.DataFrame]:
    dfs['orders']['notes'] = dfs['orders']['notes'].fillna('Sin notas')

    for name, df in dfs.items():
        dfs[name] = df.replace(r'^\s*$', pd.NA, regex=True)
    
    return dfs

# Duplicated records -----------------------------------------------------------------------------------------------

def check_duplicates(dfs: dict[str, pd.DataFrame]):
    for name, df in dfs.items():
        if df.duplicated().any() == True:
            print(f"Duplicated records in {name}: {df.duplicated().sum()}")
        else:
            print(f"Duplicated records in {name}: 0")


# Data types -----------------------------------------------------------------------------------------------

# helper: cast columns
def cast_columns (df: pd.DataFrame, dtypes_for_casting: dict[str,str]) -> pd.DataFrame:
    for col, dtype in dtypes_for_casting.items():
        if col not in df.columns:
            print(f"{col} does not exists in the dataframe")
            continue

        try:
            df[col] = df[col].astype(dtype)
            print(f'column {col} was successfully converted to {df[col].dtype}')
        except Exception as e:
            print(f"{col} could not be converted to {dtype}. Error: {e}")

    return df


# helper: cast to datetime type (separated from the previous function because we have to use pd.to_datetime())
def cast_to_date(df: pd.DataFrame, cols_to_cast: list[str]) -> pd.DataFrame:
    for col in cols_to_cast:
        if col not in df.columns:
            print(f"{col} does not exists in the dataframe")
            continue

        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f'{col} was successfully converted to datetime')
        except Exception as e:
            print(f"{col} could not be converted to datetime. Error: {e}")


    return df


# df_orders:
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    
    orders_cols = {
    'promotion_id': 'Int64',
    'status': 'category',
    'payment_method': 'category',
    'shipping_method': 'category',
    'shipping_cost': 'float64',
    'order_number': 'string',
    'notes': 'string'
    }
    
    orders_cols_to_date = ['order_date']

    df = cast_columns(df, orders_cols)
    df = cast_to_date(df, orders_cols_to_date)

    return df


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:

    customers_cols = {
    'first_name': 'string',
    'last_name': 'string',
    'email': 'string',
    'phone': 'string',
    'city': 'category', # 500 records, not much granularity, category is ok.
    'country': 'category',
    'postal_code': 'string',
    'segment': 'category',
    'is_verified': 'boolean',
    'accepts_marketing': 'boolean'
    }

    customers_cols_to_date = ['birth_date', 'registration_date', 'last_login']

    df = cast_columns(df, customers_cols)
    df = cast_to_date(df, customers_cols_to_date)

    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:

    products_cols = {
    'sku': 'string',
    'product_name': 'string',
    'description': 'string',
    'is_active': 'boolean'
    }

    products_cols_to_date = ['created_at', 'updated_at']

    df = cast_columns(df, products_cols)
    df = cast_to_date(df, products_cols_to_date)

    return df


def transform_categories(df: pd.DataFrame) -> pd.DataFrame:
    
    categories_cols = {
    'category_name': 'category', # instead of string. Just a few categories, don't vary much.
    'description': 'string',
    'parent_category_id': 'Int64',
    'is_active': 'boolean'
    }

    df = cast_columns(df, categories_cols)

    return df


def transform_inventory(df: pd.DataFrame) -> pd.DataFrame:

    inventory_cols_to_date = ['last_restock_date']
    
    df = cast_to_date(df, inventory_cols_to_date)

    return df


def transform_brands(df: pd.DataFrame) -> pd.DataFrame:
    
    brands_cols = {
    'brand_name': 'string',
    'country_of_origin': 'category',
    'website': 'string'
    }

    df = cast_columns(df, brands_cols)

    return df


def transform_promotions(df: pd.DataFrame) -> pd.DataFrame:
    
    promotions_cols = {
    'promotion_code': 'string',
    'promotion_name':'string',
    'promotion_type': 'category',
    'discount_value': 'Int64', # more robust to consider future nulls, not like int64.
    'min_order_amount': 'Int64', # more robust to consider future nulls, not like int64.
    'max_uses': 'Int64', # more robust to consider future nulls, not like int64.
    'current_uses': 'Int64', # more robust to consider future nulls, not like int64.
    'is_active': 'boolean' # more robust, it can avoid masking nulls as False.
    }

    promotions_cols_to_date = ['start_date', 'end_date']

    df = cast_columns(df, promotions_cols)
    df = cast_to_date(df, promotions_cols_to_date)

    return df


def transform_reviews(df: pd.DataFrame) -> pd.DataFrame:
    
    reviews_cols = {
    'title': 'string',
    'comment': 'string',
    'is_verified_purchase': 'boolean' # more robust, it's safe from, for example, false False
    }

    reviews_cols_to_date = ['created_at']

    df = cast_columns(df, reviews_cols)
    df = cast_to_date(df,reviews_cols_to_date)

    return df


def transform_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    
    suppliers_cols = {
    'supplier_name': 'string',
    'contact_name': 'string',
    'email': 'string',
    'phone': 'string',
    'address': 'string',
    'is_active': 'boolean'
    }

    df = cast_columns(df, suppliers_cols)

    return df


def transform_warehouses(df: pd.DataFrame) -> pd.DataFrame:
    
    warehouses_cols = {
    'warehouse_name': 'string',
    'location': 'category',
    'manager_name': 'string'
    }

    df = cast_columns(df, warehouses_cols)

    return df


# function (wrapper) that uses the previous ones. Entry point.

def transform_all(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:

    dfs = clean_nulls(dfs)
    check_duplicates(dfs)

    dfs['orders'] = transform_orders(dfs['orders'])
    dfs['customers'] = transform_customers(dfs['customers'])
    dfs['products'] = transform_products(dfs['products'])
    dfs['categories'] = transform_categories(dfs['categories'])
    dfs['inventory'] = transform_inventory(dfs['inventory'])
    dfs['brands'] = transform_brands(dfs['brands'])
    dfs['promotions'] = transform_promotions(dfs['promotions'])
    dfs['reviews'] = transform_reviews(dfs['reviews'])
    dfs['suppliers'] = transform_suppliers(dfs['suppliers'])
    dfs['warehouses'] = transform_warehouses(dfs['warehouses'])

    return dfs

