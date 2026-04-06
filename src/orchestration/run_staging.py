from ..staging.orders import transform_orders
from ..staging.order_items import transform_order_items
from ..staging.order_payments import transform_order_payments
from ..staging.order_reviews import transform_order_reviews
from ..staging.customers import transform_customers
from ..staging.products import transform_products
from ..staging.sellers import transform_sellers
from ..staging.geolocation import transform_geolocation

from ..utils.staging_base import save_parquet

from ..config import RAW_PATH
from ..config import STAGING_PATH

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

DATASETS = [
    ("orders.csv",         "orders.parquet",         transform_orders),
    ("order_items.csv",    "order_items.parquet",    transform_order_items),
    ("order_payments.csv", "order_payments.parquet", transform_order_payments),
    ("order_reviews.csv",  "order_reviews.parquet",  transform_order_reviews),
    ("customers.csv",      "customers.parquet",      transform_customers),
    ("products.csv",       "products.parquet",       transform_products),
    ("sellers.csv",        "sellers.parquet",        transform_sellers),
    ("geolocation.csv",    "geolocation.parquet",    transform_geolocation)
]

def run_staging():
    logger.info("[STAGING] Iniciando processo de staging")
    
    success = 0
    fail = 0

    for csv_name, parquet_name, transform_def in DATASETS:
        try:
            logger.info(f"[STAGING] Processando {csv_name}")
            
            df = transform_def(RAW_PATH / csv_name)

            logger.info(f"[STAGING] Salvando {parquet_name}")
            
            save_parquet(df, STAGING_PATH / parquet_name)

            success += 1
        except Exception as e:
            fail += 1

            logger.error(f"Erro em {csv_name}: {e}")

    logger.info(f"[STAGING] Processo de staging finalizado: {success} sucesso, {fail} falhas")

if __name__ == "__main__":
    run_staging()