from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT category, ROUND(AVG(price)) AS avg_price
        FROM products
        GROUP BY category
        ORDER BY avg_price DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
