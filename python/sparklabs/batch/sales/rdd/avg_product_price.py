from sparklabs.batch.sales.rdd import utils


def run() -> None:
    (
        utils.products.map(lambda product: (product["category"], (product["price"], 1)))
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
        .map(lambda item: (item[0], round(item[1][0] / item[1][1])))
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    )
    for row in (
        utils.products.map(lambda product: (product["category"], (product["price"], 1)))
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
        .map(lambda item: (item[0], round(item[1][0] / item[1][1])))
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    ):
        print(row)


if __name__ == "__main__":
    run()
