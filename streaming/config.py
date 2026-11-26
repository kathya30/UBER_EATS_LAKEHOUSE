"""Shared constants for the streaming (Kafka -> S3) part of the platform."""

TOPIC = "order_events"
DEFAULT_BOOTSTRAP = "localhost:9092"

# Order lifecycle, in the order a healthy order moves through it.
LIFECYCLE = [
    "order_placed",
    "merchant_accepted",
    "courier_assigned",
    "order_picked_up",
    "order_delivered",
]
TERMINAL_EVENTS = ["order_delivered", "order_cancelled"]
VALID_EVENT_TYPES = LIFECYCLE + ["order_cancelled"]

# Business SLA: placed -> delivered within this many minutes.
SLA_MINUTES = 45

# Same ID formats as the batch data generators, so streaming events can be
# joined to the batch dimensions (dim_merchant / dim_customer / dim_courier).
NUM_MERCHANTS = 150
NUM_CUSTOMERS = 800
NUM_COURIERS = 120
CITIES = [
    "Chicago", "Dallas", "Denver", "Seattle",
    "Miami", "Atlanta", "Austin", "Phoenix",
]
