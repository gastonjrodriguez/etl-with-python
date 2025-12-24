# ETL con Python

## Descripción
Pipeline ETL que procesa datos de e-commerce para generar métricas de ventas.
Realizado en Python, utilizando pandas, enfocado en limpieza, normalización y tipado correcto de datos.

## Dataset
El proyecto trabaja con múltiples archivos CSV que forman un sistema de e-commerce (orders, customers, products, etc.).

## Cómo correr
```bash
pip install pandas pyarrow
python etl.py
```

### Extract
- Lectura de archivos CSV
- Validación de carga

### Transform
- Normalización de nulos
- Limpieza de strings vacíos
- Conversión de tipos de datos, considerando claves primarias y foráneas para mantener coherencia relacional
- Conversión de fechas


## Decisiones de limpieza
- **Nulos**:
    - DF_ORDERS: Reemplazo de nulls en df_orders['notes'] por "Sin notas", ya que simplemente son opcionales para el registro de la orden de compra.
    - DF_ORDERS: Se mantiene el 'promotion_id' con sus nulos intactos, porque puede no existir una promocion vigente. 'promotion_id' es una FK, por ende, un 0 en lugar de null implica la existencia de la "promocion 0", no la ausencia de promocion. Dejar el null representa la ausencia en la relacion, lo cual es mas preciso en este caso.
    - DF_CATEGORIES: si bien df_categories['parent_category_id'] tiene 7 nulos de 10 registros, se opta por conservarlos ya que los valores no nulos indican relaciones jerárquicas con una categoría padre (por ende si es null, nos encontramos ante una categoria padre en si misma).
- **Duplicados**: A modo de prueba, con un for se dejó afuera la primera columna de cada DF, que en este caso particular siempre es PK de su respectiva tabla. Verificado con EDA (Exploratory Data Analysis). Sirve para ver como funciona el sistema: un cliente al querer mas de una unidad de determinado producto, genera un 'order_item_id' nuevo/unico pero mantiene el mismo 'order_id' al igual que el resto de parametros, siendo englobado en la misma compra/orden. Esta es la razon de por que vemos duplicados en 'order_items' si excluimos el 'order_item_id'.
- **Tipos**: se crearon dos funciones para que la conversion sea mas eficiente: cast_columns, y cast_to_date exclusivamente para tipo fecha, ya que usa pd.to_datetime() en vez de astype(). Se convirtieron los tipos de datos pensando en la funcionalidad del dato para con el modelo, carga de nuevos datos, y analisis futuros.


## Output 
- `ventas_por_cliente.csv`: Total gastado y cantidad de órdenes por cliente
- `ventas_por_mes.csv`: Ventas totales por mes
- archivos cargados inicialmente, que conforman el modelo, ahora devueltos en formato csv/parquet.



## Autor
Gaston Rodriguez - Dic 2025