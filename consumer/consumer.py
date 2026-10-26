import json
from datetime import datetime, timedelta

from kafka import KafkaConsumer

TOPICS = ["postgres.public.users", "postgres.public.orders"]

OPERATIONS = {
    "r": "READ (snapshot)",
    "c": "CREATE",
    "u": "UPDATE",
    "d": "DELETE",
}

# Логические типы Debezium для TIMESTAMP -> количество единиц в секунде
TIMESTAMP_TYPES = {
    "io.debezium.time.Timestamp": 1_000,
    "io.debezium.time.MicroTimestamp": 1_000_000,
    "io.debezium.time.NanoTimestamp": 1_000_000_000,
    "org.apache.kafka.connect.data.Timestamp": 1_000,
}


def timestamp_columns(schema):
    """Колонки строки (before/after), которые Debezium передаёт как числовой timestamp."""
    columns = {}
    for field in (schema or {}).get("fields", []):
        if field.get("field") in ("before", "after"):
            for column in field.get("fields", []):
                if column.get("name") in TIMESTAMP_TYPES:
                    columns[column["field"]] = TIMESTAMP_TYPES[column["name"]]
    return columns


def format_row(row, ts_columns):
    if row is None:
        return None
    row = dict(row)
    for column, units in ts_columns.items():
        if row.get(column) is not None:
            # TIMESTAMP без часового пояса кодируется относительно эпохи как есть
            seconds = row[column] / units
            row[column] = (datetime(1970, 1, 1) + timedelta(seconds=seconds)).isoformat(sep=" ")
    return row


def decode(raw):
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))


consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
)

print(f"Waiting for messages from {', '.join(TOPICS)}...\n")

for message in consumer:

    print("=" * 80)
    print(f"Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}")

    try:
        key = decode(message.key)
        value = decode(message.value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        print(message.value)
        continue

    if key is not None:
        print(f"Key: {json.dumps(key.get('payload', key), ensure_ascii=False)}")

    # После DELETE Debezium отправляет tombstone-сообщение с пустым value
    if value is None:
        print("Tombstone (value = null)")
        continue

    # JsonConverter с schemas.enable=true оборачивает данные в {"schema": ..., "payload": ...}
    payload = value.get("payload", value)
    ts_columns = timestamp_columns(value.get("schema"))

    op = payload.get("op")
    source = payload.get("source", {})

    print(f"Operation: {OPERATIONS.get(op, op)}")
    print(f"Table: {source.get('schema')}.{source.get('table')}")
    print(f"Before: {json.dumps(format_row(payload.get('before'), ts_columns), ensure_ascii=False)}")
    print(f"After:  {json.dumps(format_row(payload.get('after'), ts_columns), ensure_ascii=False)}")
