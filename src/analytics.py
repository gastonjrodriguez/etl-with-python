import pandas as pd

# ANALYTICS: answer business questions -----------------------------------------------------------------------------------------------

#1. Which customers spent the most?
#2. What is the best-selling product (by quantity)?
#3. How did sales evolve month by month?

def top_clients(dfs: dict[str,pd.DataFrame], n_top: int = 5) -> pd.DataFrame:

    df_orders = dfs['orders']
    
    
    results = df_orders.groupby('customer_id').agg(valor_total_gastado=('total_amount', 'sum'), cantidad_ordenes=('order_id', 'count')).sort_values('valor_total_gastado', ascending=False).head(n_top)
    
    return results


def most_sold_product(dfs: dict[str,pd.DataFrame]) -> pd.DataFrame:

    df_order_items = dfs['order_items']
    df_products = dfs['products']

    results = df_order_items.merge(
        df_products[['product_id', 'product_name']],
        on='product_id',
        how='left').groupby(['product_id', 'product_name'], as_index=False).agg(cantidad_total=('quantity', 'sum')).sort_values('cantidad_total', ascending=False).head(1)
    

    return results


def monthly_sales_evolution(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    df_orders = dfs['orders']

    results = df_orders.assign(month=lambda x: x["order_date"].dt.to_period("M")).groupby("month", as_index=False).agg(total_sales=("total_amount", "sum")).assign(month=lambda x: x["month"].dt.to_timestamp()).sort_values('month')
    # dt.to_timestamp to ensure that it's export friendly.

    return results



# wrapper that uses the previous functions and create metrics. Entry point.
def run_analytics(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:

    return {
        'top_clients': top_clients(dfs,10),
        'most_sold_product': most_sold_product(dfs),
        'monthly_sales': monthly_sales_evolution(dfs)
    }



