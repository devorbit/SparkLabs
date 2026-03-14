import random

from sparklabs.batch.sales.rdd import utils


def run() -> None:
    for row in utils.sc.parallelize(range(20)).map(
        lambda identifier: (identifier, int(random.random() * 10))
    ).collect():
        print(row)


if __name__ == "__main__":
    run()
