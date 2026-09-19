"""
Generates daily incremental transactional data: orders, payments, refunds,
delivery SLA events, and support tickets. All generated together per day so
foreign keys (order_id -> payment/refund/delivery/ticket) stay consistent -
mimicking what several source systems would each drop for the same day.
"""
import csv
import os
import random
import datetime
from config import (
    SEED, NUM_CUSTOMERS, NUM_MERCHANTS, NUM_COURIERS, DAY_DATES,
    ORDERS_PER_DAY_MIN, ORDERS_PER_DAY_MAX, PAYMENT_METHODS,
    REFUND_REASONS, SUPPORT_ISSUE_CATEGORIES,
)

random.seed(SEED)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data")
ORDERS_DIR = os.path.join(BASE_DIR, "orders")
PAYMENTS_DIR = os.path.join(BASE_DIR, "payments")
REFUNDS_DIR = os.path.join(BASE_DIR, "refunds")
DELIVERY_DIR = os.path.join(BASE_DIR, "delivery_events")
SUPPORT_DIR = os.path.join(BASE_DIR, "support_tickets")

ORDER_STATUS_WEIGHTED = (
    ["Delivered"] * 70 + ["Cancelled"] * 8 + ["In Progress"] * 12 + ["Failed"] * 10
)

_order_ctr = 0
_payment_ctr = 0
_refund_ctr = 0
_ticket_ctr = 0


def next_id(prefix, counter_name):
    global _order_ctr, _payment_ctr, _refund_ctr, _ticket_ctr
    if counter_name == "order":
        _order_ctr += 1
        return f"{prefix}{str(_order_ctr).zfill(7)}"
    if counter_name == "payment":
        _payment_ctr += 1
        return f"{prefix}{str(_payment_ctr).zfill(7)}"
    if counter_name == "refund":
        _refund_ctr += 1
        return f"{prefix}{str(_refund_ctr).zfill(6)}"
    if counter_name == "ticket":
        _ticket_ctr += 1
        return f"{prefix}{str(_ticket_ctr).zfill(6)}"


def ensure_dirs():
    for d in [ORDERS_DIR, PAYMENTS_DIR, REFUNDS_DIR, DELIVERY_DIR, SUPPORT_DIR]:
        os.makedirs(d, exist_ok=True)


def generate_day(snapshot_date):
    orders, payments, refunds, delivery_events, tickets = [], [], [], [], []
    n_orders = random.randint(ORDERS_PER_DAY_MIN, ORDERS_PER_DAY_MAX)

    for _ in range(n_orders):
        order_id = next_id("ORD", "order")
        customer_id = f"CUST{str(random.randint(1, NUM_CUSTOMERS)).zfill(5)}"
        merchant_id = f"MERCH{str(random.randint(1, NUM_MERCHANTS)).zfill(5)}"
        courier_id = f"COUR{str(random.randint(1, NUM_COURIERS)).zfill(5)}"

        order_amount = round(random.uniform(8.0, 95.0), 2)
        status = random.choice(ORDER_STATUS_WEIGHTED)
        order_time = datetime.datetime.combine(
            snapshot_date, datetime.time(random.randint(7, 22), random.randint(0, 59))
        )

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "courier_id": courier_id,
            "order_datetime": order_time.isoformat(),
            "order_amount": order_amount,
            "status": status,
            "snapshot_date": snapshot_date.isoformat(),
        })

        # Payment: generated for nearly every order (some fail before payment)
        if status != "Failed" or random.random() < 0.3:
            payment_status = "Success" if status != "Failed" else "Failed"
            payments.append({
                "payment_id": next_id("PAY", "payment"),
                "order_id": order_id,
                "payment_method": random.choice(PAYMENT_METHODS),
                "amount": order_amount,
                "payment_status": payment_status,
                "payment_datetime": order_time.isoformat(),
                "snapshot_date": snapshot_date.isoformat(),
            })

        # Refund: only for a subset of delivered/cancelled orders
        if status in ("Delivered", "Cancelled") and random.random() < 0.07:
            refund_amount = round(order_amount * random.uniform(0.3, 1.0), 2)
            refunds.append({
                "refund_id": next_id("REF", "refund"),
                "order_id": order_id,
                "refund_amount": refund_amount,
                "refund_reason": random.choice(REFUND_REASONS),
                "refund_date": snapshot_date.isoformat(),
            })

        # Delivery SLA event: only for orders that actually got delivered/in-progress
        if status in ("Delivered", "In Progress"):
            promised_minutes = random.randint(25, 45)
            promised_time = order_time + datetime.timedelta(minutes=promised_minutes)
            # 80% chance delivered close to promised time, 20% chance late
            if random.random() < 0.8:
                actual_minutes = promised_minutes + random.randint(-8, 5)
            else:
                actual_minutes = promised_minutes + random.randint(6, 35)
            actual_time = order_time + datetime.timedelta(minutes=max(actual_minutes, 5))
            sla_met = actual_time <= promised_time

            delivery_events.append({
                "order_id": order_id,
                "courier_id": courier_id,
                "promised_time": promised_time.isoformat(),
                "actual_delivery_time": actual_time.isoformat() if status == "Delivered" else "",
                "sla_met": sla_met if status == "Delivered" else "",
                "snapshot_date": snapshot_date.isoformat(),
            })

        # Support ticket: small fraction of orders generate a support case
        if random.random() < 0.05:
            tickets.append({
                "ticket_id": next_id("TICK", "ticket"),
                "order_id": order_id,
                "customer_id": customer_id,
                "issue_category": random.choice(SUPPORT_ISSUE_CATEGORIES),
                "opened_date": snapshot_date.isoformat(),
                "resolution_time_minutes": random.randint(5, 720),
                "status": random.choice(["Open", "Resolved", "Resolved", "Escalated"]),
            })

    return orders, payments, refunds, delivery_events, tickets


def write_csv(directory, filename, rows, fieldnames):
    if not rows:
        return
    path = os.path.join(directory, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {path}")


if __name__ == "__main__":
    ensure_dirs()
    for snapshot_date in DAY_DATES:
        orders, payments, refunds, delivery_events, tickets = generate_day(snapshot_date)
        d = snapshot_date.isoformat()

        write_csv(ORDERS_DIR, f"orders_{d}.csv", orders, [
            "order_id", "customer_id", "merchant_id", "courier_id",
            "order_datetime", "order_amount", "status", "snapshot_date",
        ])
        write_csv(PAYMENTS_DIR, f"payments_{d}.csv", payments, [
            "payment_id", "order_id", "payment_method", "amount",
            "payment_status", "payment_datetime", "snapshot_date",
        ])
        write_csv(REFUNDS_DIR, f"refunds_{d}.csv", refunds, [
            "refund_id", "order_id", "refund_amount", "refund_reason", "refund_date",
        ])
        write_csv(DELIVERY_DIR, f"delivery_events_{d}.csv", delivery_events, [
            "order_id", "courier_id", "promised_time", "actual_delivery_time",
            "sla_met", "snapshot_date",
        ])
        write_csv(SUPPORT_DIR, f"support_tickets_{d}.csv", tickets, [
            "ticket_id", "order_id", "customer_id", "issue_category",
            "opened_date", "resolution_time_minutes", "status",
        ])
