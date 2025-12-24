import pandas as pd
import glob as gb
import os



# EXTRACT: EXTRACCION, CARGA Y EXPLORACION DE DATAFRAMES -----------------------------------------------------------------------------------------------

# Ejecutar desde el root del proyecto. chequear antes de avanzar.

archives = gb.glob('data/ecommerce_*.csv')

if not archives:
    print("Archivos no encontrados. Revisar ubicacion de los mismos.")
else:
    print(f"Archivos encontrados: {len(archives)}")
    for i in sorted(archives):
        print(f" -  {os.path.basename(i)}")


df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')

df_categories = pd.read_csv('data/ecommerce_categories.csv')
df_inventory = pd.read_csv('data/ecommerce_inventory.csv')
df_brands = pd.read_csv('data/ecommerce_brands.csv')
df_promotions = pd.read_csv('data/ecommerce_promotions.csv')
df_reviews = pd.read_csv('data/ecommerce_reviews.csv')
df_suppliers = pd.read_csv('data/ecommerce_suppliers.csv')
df_warehouses = pd.read_csv('data/ecommerce_warehouses.csv')


# Diccionario de dataframes para manipularlos de una manera mas eficiente.

df_dicts = {
    'orders': df_orders,
    'order_items': df_order_items,
    'customers': df_customers,
    'products': df_products,
    'categories': df_categories,
    'inventory': df_inventory,
    'brands': df_brands,
    'promotions': df_promotions,
    'reviews': df_reviews,
    'suppliers': df_suppliers,
    'warehouses': df_warehouses
}


for name, df in df_dicts.items():
    print("\n")
    print(f"{name.upper()}: {len(df)} filas. {len(df.columns)} columnas.")


# Primeras filas de los dataframes

for name, df in df_dicts.items():
    print("\n")
    print(f"PRIMERAS FILAS DE {name}:")
    print(df.head())


# TRANSFORM: manejo de nulos -----------------------------------------------------------------------------------------------


# DF_ORDERS: reemplazo de nulls en df_orders['notes'] por "Sin notas", ya que simplemente son opcionales para el registro de la orden de compra.

df_orders['notes'] = df_orders['notes'].fillna('Sin notas')

print("\n")
print("ORDERS (notes) post reemplazo de nulos:")
print("\n")
df_orders.info()

# DF_ORDERS: se mantiene el 'promotion_id' con sus nulos intactos, porque puede no existir una promocion vigente.
# 'promotion_id' es una FK, por ende, un 0 en lugar de null, implica la existencia de la "promocion 0", no la ausencia de promocion.
# dejar el null representa la ausencia en la relacion, lo cual es mas preciso en este caso.
# En caso de querer modificarlo, seria:
# df_orders['promotion_id'] = df_orders['promotion_id'].fillna(0)
# df_orders.info()


# DF_CATEGORIES: si bien df_categories['parent_category_id'] tiene 7 nulos de 10 registros, se opta por conservarlos ya que los no-nulls indican categorias padre. 


# Chequeo de espacios camuflados de non-nulls

for name, df in df_dicts.items():
    df_dicts[name] = df.replace(r'^\s*$', pd.NA, regex=True)

for name, df in df_dicts.items():
    print("\n")
    print(f"INFO SOBRE {name}:")
    df.info()


# TRANSFORM: manejo de duplicados -----------------------------------------------------------------------------------------------


# El resultado de lo siguiente nos hace ver que todos los id en cada dataframe conforman un registro unico.
for name, df in df_dicts.items():
    if df.duplicated().any() == True:
        print(f"Duplicados en {name}: {df.duplicated().sum()}")
    else:
        print(f"Duplicados en {name}: 0")

print("\n")


# A modo de prueba, se dejó afuera la primera columna de los DF, que en este caso particular siempre es PK de su respectiva tabla. Verificado con EDA (Exploratory Data Analysis).
for name, df in df_dicts.items():
    columns_subset = df.columns[1:]
    duplicated_values = df.duplicated(subset=columns_subset, keep=False)

    print(f"{name}: {duplicated_values.sum()} duplicados (ignorando la primera columna)")

print(df_order_items)

# Podemos ver como funciona el sistema: un cliente al querer mas de una unidad de determinado producto, genera un 'order_item_id' nuevo/unico
# pero mantiene el mismo 'order_id' al igual que el resto de parametros, siendo englobado en la misma compra/orden.
# esta es la razon de por que vemos duplicados en 'order_items' si excluimos el 'order_item_id'.



# TRANSFORM: corregir tipo de datos -----------------------------------------------------------------------------------------------


# Definimos funcion cast_columns para castear columnas que deben ser modificadas de forma mas eficiente.
# NO aplica para conversion de tipo fecha, ya que para eso se usa pd.to_datetime() en lugar de astype()

def cast_columns (df: pd.DataFrame, dtypes_for_casting: dict[str,str], df_name: str) -> pd.DataFrame:
    for col, dtype in dtypes_for_casting.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
                print(f"\n Dataframe: {df_name}")
                print(f"Columna: {col}")
                print(f"Tipo convertido: {df[col].dtype}")
                print(f"Nulos:{df[col].isna().sum()}")
            except Exception as e:
                print(f"No se pudo convertir la columna: {col}, al dtype: {dtype}. Error: {e}")
        else:
            print(f"Columna: {col}, es inexistente")
    return df


# Funcion para convertir a tipo fecha. Se usa pd.to_datetime(), y se pasa lista como parametro ya que se convierte todo a un mismo tipo de dato. 

def cast_to_date(df: pd.DataFrame, cols_to_cast: list[str], df_name: str) -> pd.DataFrame:
    for col in cols_to_cast:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f"\n Dataframe: {df_name}")
            print(f"Columna: {col}")
            print(f"Tipo convertido: {df[col].dtype}")
            print(f"Nulos:{df[col].isna().sum()}")
        else:
            print(f"No se pudo convertir la columna: {col}, a tipo datetime")
    return df


# df_orders:
df_orders.info()

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


cast_columns(df_orders, orders_cols, 'df_orders')
cast_to_date(df_orders, orders_cols_to_date, 'df_orders')


# df_customers: 
df_customers.info()
print(df_customers.head())


customers_cols = {
    'first_name': 'string',
    'last_name': 'string',
    'email': 'string',
    'phone': 'string',
    'city': 'category', # no es tan granular (500 registros) asi que category va ok.
    'country': 'category',
    'postal_code': 'string',
    'segment': 'category',
    'is_verified': 'boolean',
    'accepts_marketing': 'boolean'
}

customers_cols_to_date = ['birth_date', 'registration_date', 'last_login']
cast_columns(df_customers, customers_cols, 'df_customers')
cast_to_date(df_customers, customers_cols_to_date, 'df_customers')


# df_products
df_products.info()

products_cols = {
    'sku': 'string',
    'product_name': 'string',
    'description': 'string',
    'is_active': 'boolean'
}

products_cols_to_date = ['created_at', 'updated_at']
cast_columns(df_products, products_cols, 'df_products')
cast_to_date(df_products, products_cols_to_date, 'df_products')

# df_categories
df_categories.info()

categories_cols = {
    'category_name': 'category', # en vez de string. son pocas categorias, varian poco.
    'description': 'string',
    'parent_category_id': 'Int64',
    'is_active': 'boolean'
}

cast_columns(df_categories, categories_cols, 'df_categories')


# df_inventory
df_inventory.info()
inventory_cols_to_date = ['last_restock_date']
cast_to_date(df_inventory, inventory_cols_to_date, 'df_inventory')

# df_brands
df_brands.info()

brands_cols = {
    'brand_name': 'string',
    'country_of_origin': 'category',
    'website': 'string'
}

cast_columns(df_brands, brands_cols, 'df_brands')


# df_promotions
df_promotions.info()

promotions_cols = {
    'promotion_code': 'string',
    'promotion_name':'string',
    'promotion_type': 'category',
    'discount_value': 'Int64', # mas robusto, considerando futuros nulls, no como int64.
    'min_order_amount': 'Int64', # mas robusto, considerando futuros nulls, no como int64.
    'max_uses': 'Int64', # mas robusto, considerando futuros nulls, no como int64.
    'current_uses': 'Int64', # mas robusto, considerando futuros nulls, no como int64.
    'is_active': 'boolean' # mas robusto, evitando enmascarar nulls por False.
}
promotions_cols_to_date = ['start_date', 'end_date']

cast_columns(df_promotions, promotions_cols, 'df_promotions')
cast_to_date(df_promotions, promotions_cols_to_date, 'df_promotions')

# df_reviews
df_reviews.info()

reviews_cols = {
    'title': 'string',
    'comment': 'string',
    'is_verified_purchase': 'boolean' # mas robusto, seguro ante, por ejemplo, falsos False
}

reviews_cols_to_date = ['created_at']

cast_columns(df_reviews, reviews_cols, 'df_reviews')
cast_to_date(df_reviews,reviews_cols_to_date, 'df_reviews')


# df_suppliers
df_suppliers.info()

suppliers_cols = {
    'supplier_name': 'string',
    'contact_name': 'string',
    'email': 'string',
    'phone': 'string',
    'address': 'string',
    'is_active': 'boolean'
}

cast_columns(df_suppliers, suppliers_cols, 'df_suppliers')

# df_warehouse
df_warehouses.info()

warehouses_cols = {
    'warehouse_name': 'string',
    'location': 'category',
    'manager_name': 'string'
}

cast_columns(df_warehouses, warehouses_cols, 'df_warehouses')

# TRANSFORM: resolver preguntas del negocio -----------------------------------------------------------------------------------------------

# TRES preguntas de negocio:
# 1. ¿Cuáles son los 5 clientes que más gastaron?
# 2. ¿Cuál es el producto más vendido (por cantidad)?
# 3. ¿Cómo evolucionaron las ventas mes a mes?

# 1:
sales_per_client = df_orders.groupby('customer_id').agg({
    'total_amount': 'sum',
    'order_id': 'count'
}).rename(columns={'total_amount': 'valor_total_gastado', 'order_id': 'cantidad_ordenes'}).sort_values('valor_total_gastado', ascending=False)


print("TOP 10 CLIENTES:")
print(sales_per_client.head(10))

# 2:
most_sold_products = df_order_items.merge(
    df_products[['product_id', 'product_name']],
    on='product_id',
    how='left').groupby(['product_id', 'product_name'], as_index=False).agg({
        'quantity': 'sum'
    }).rename(columns={'quantity': 'cantidad_total'}).sort_values('cantidad_total', ascending=False)

top_product = most_sold_products.iloc[0]
print("\n")
print(f"Producto mas vendido: ID {top_product['product_id']}, CANTIDAD: {top_product['cantidad_total']}, NOMBRE: {top_product['product_name']}")

# 3:
df_orders['month'] = df_orders['order_date'].dt.to_period('M')
monthly_sales = df_orders.groupby('month')['total_amount'].sum().reset_index().rename(columns={'total_amount': 'total_sales'})
print("TOTAL VENTAS POR MES:")
print(monthly_sales)


# LOAD: guardado en formato CSV -----------------------------------------------------------------------------------------------

# guardado de metricas
sales_per_client.to_csv('output/sales_per_client.csv', index=False)
monthly_sales.to_csv('output/sales_per_month.csv', index=False)

# guardado del resto de los datos limpios
df_orders.to_csv('output/orders_clean.csv', index=False)
df_order_items.to_csv('output/order_items_clear.csv', index=False)
df_products.to_csv('output/products_clear.csv', index=False)
df_customers.to_csv('output/customers_clear.csv', index=False)
df_categories.to_csv('output/categories_clear.csv', index=False)
df_inventory.to_csv('output/inventory_clear.csv', index=False)
df_brands.to_csv('output/brands_clear.csv', index=False)
df_promotions.to_csv('output/promotions_clear.csv', index=False)
df_reviews.to_csv('output/reviews_clear.csv', index=False)
df_suppliers.to_csv('output/suppliers_clear.csv', index=False)
df_warehouses.to_csv('output/warehouses_clear.csv', index=False)

print("Guardado exitoso en .csv")

# LOAD: guardado en formato Parquet -----------------------------------------------------------------------------------------------

sales_per_client.to_parquet('output/sales_per_client.parquet', index=False)
monthly_sales.to_parquet('output/sales_per_month.parquet', index=False)
df_orders.to_parquet('output/orders_clean.parquet', index=False)
df_order_items.to_parquet('output/order_items_clear.parquet', index=False)
df_products.to_parquet('output/products_clear.parquet', index=False)
df_customers.to_parquet('output/customers_clear.parquet', index=False)
df_categories.to_parquet('output/categories_clear.parquet', index=False)
df_inventory.to_parquet('output/inventory_clear.parquet', index=False)
df_brands.to_parquet('output/brands_clear.parquet', index=False)
df_promotions.to_parquet('output/promotions_clear.parquet', index=False)
df_reviews.to_parquet('output/reviews_clear.parquet', index=False)
df_suppliers.to_parquet('output/suppliers_clear.parquet', index=False)
df_warehouses.to_parquet('output/warehouses_clear.parquet', index=False)

print("Guardado exitoso en .parquet")


