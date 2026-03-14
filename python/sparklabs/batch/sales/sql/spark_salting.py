from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT id, CAST(rand() * 10 AS INT) AS salt
        FROM RANGE(20)
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
