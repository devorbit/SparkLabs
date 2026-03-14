from sparklabs.batch.sales.dataset import utils
from sparklabs.batch.sales.dataset.models import Order, Product


def run() -> None:
    distinct_customer_count = utils.to_records(utils.orders, Order).map(
        lambda order: order.customer_id
    ).count()
    expensive_product = utils.to_records(utils.products, Product).takeOrdered(
        1, key=lambda product: -product.price
    )

    print(f"Total distinct customers: {distinct_customer_count}")
    print("Most Expensive Product:")
    print(expensive_product)


if __name__ == "__main__":
    run()
