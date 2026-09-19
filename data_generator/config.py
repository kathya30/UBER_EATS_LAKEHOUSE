"""Shared config for Uber Eats marketplace synthetic data generation."""
import datetime

SEED = 42

NUM_CUSTOMERS = 800
NUM_MERCHANTS = 150
NUM_COURIERS = 120

ORDERS_PER_DAY_MIN = 150
ORDERS_PER_DAY_MAX = 300

# 14 days: day 0 is the full initial master-data load, days 1-13 are
# incremental snapshots/transactions - enough runway for real SCD2 drift
# (merchant suspensions, courier status churn, tier upgrades) and enough
# order volume for meaningful gold-layer aggregates and Genie questions.
START_DATE = datetime.date(2026, 8, 1)
NUM_DAYS = 14
DAY_DATES = [START_DATE + datetime.timedelta(days=i) for i in range(NUM_DAYS)]

CITIES = [
    ("Chicago", "IL"), ("Dallas", "TX"), ("Denver", "CO"),
    ("Seattle", "WA"), ("Miami", "FL"), ("Atlanta", "GA"),
    ("Austin", "TX"), ("Phoenix", "AZ"),
]

CUISINE_CATEGORIES = [
    "American", "Italian", "Mexican", "Chinese", "Indian",
    "Japanese", "Thai", "Mediterranean", "Fast Food", "Desserts",
    "Pizza", "Vegan",
]

VEHICLE_TYPES = ["Bike", "Scooter", "Car", "Walk"]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Wallet", "Cash On Delivery"]

REFUND_REASONS = [
    "Missing items", "Wrong order", "Food quality issue",
    "Late delivery", "Order cancelled by merchant", "Damaged packaging",
]

SUPPORT_ISSUE_CATEGORIES = [
    "Order not received", "Wrong items", "Payment issue",
    "Courier behavior", "Refund request", "App issue", "Other",
]
