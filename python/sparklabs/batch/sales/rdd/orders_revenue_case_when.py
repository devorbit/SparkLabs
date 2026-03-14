from sparklabs.batch.sales.rdd import utils


def run() -> None:
    orders_by_id = utils.orders.map(lambda order: (order["order_id"], order))
    products_by_id = utils.products.map(lambda product: (product["product_id"], product))
    promotions_by_category = utils.promotions.groupBy(lambda promotion: promotion["category"])

    rows = (
        utils.order_items.map(lambda item: (item["order_id"], item))
        .join(orders_by_id)
        .map(lambda item: (item[1][0]["product_id"], (item[1][0], item[1][1])))
        .join(products_by_id)
        .map(
            lambda item: (
                item[1][1]["category"],
                (item[1][0][0], item[1][0][1], item[1][1]),
            )
        )
        .leftOuterJoin(promotions_by_category)
        .map(_enrich_revenue)
        .reduceByKey(
            lambda left, right: (
                left[0] + right[0],
                left[1] + right[1],
                left[2] + right[2],
            )
        )
        .sortBy(lambda item: item[1][2], ascending=False)
        .collect()
    )
    for row in rows:
        print(row)


def _enrich_revenue(item):
    _, ((order_item, order, product), promotions) = item
    gross_revenue = product["price"] * order_item["quantity"]
    discount_pct = 0.0
    if promotions:
        for promotion in promotions:
            if promotion["start_date"] <= order["order_date"] <= promotion["end_date"]:
                discount_pct = promotion["discount_pct"]
                break
    discount_value = gross_revenue * discount_pct / 100
    return (
        order_item["order_id"],
        (gross_revenue, discount_value, gross_revenue - discount_value),
    )


if __name__ == "__main__":
    run()
