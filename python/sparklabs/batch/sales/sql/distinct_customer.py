from sparklabs.batch.sales.sql import utils


def run() -> None:
    customer_count = utils.sql(
        """
        SELECT COUNT(customer_id) AS total_customers
        FROM orders
        """
    ).collect()[0][0]
    expensive_product = utils.sql(
        """
        SELECT *
        FROM products
        ORDER BY price DESC
        LIMIT 1
        """
    )

    print(f"Total distinct customers: {customer_count}")
    print("Most Expensive Product:")
    expensive_product.show(truncate=False)


if __name__ == "__main__":
    run()
