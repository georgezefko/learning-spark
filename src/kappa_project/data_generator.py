# minimal_generator.py
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---- Config ----
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "10"))
TELEMETRY_EPS = float(os.getenv("TELEMETRY_EVENTS_PER_SEC", "10"))
EVENTS_EPS = float(os.getenv("EVENTS_PER_SEC", "1"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TELEMETRY = os.getenv("TOPIC_TELEMETRY", "iot-telemetry")
TOPIC_EVENTS = os.getenv("TOPIC_EVENTS", "iot-events")
THRESH_MIN = float(os.getenv("THRESH_MIN", "10"))  # 70
THRESH_MAX = float(os.getenv("THRESH_MAX", "40"))  # 90
WINDOW_SECS = 300  # 5 min windows
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))

# Scenario knobs (simple & deterministic)
HOT_EVERY_N_WINDOWS = int(
    os.getenv("HOT_EVERY_N_WINDOWS", "4")
)  # every 4th window is hot
HOT_TEMP_BOOST = float(os.getenv("HOT_TEMP_BOOST", "18.0"))
OUTAGE_SECONDS = int(
    os.getenv("OUTAGE_SECONDS", "90")
)  # outage length inside an outage window
LATE_SHARE = float(
    os.getenv("LATE_SHARE", "0.25")
)  # share of late points for late device
LATE_OFFSET_SEC = int(
    os.getenv("LATE_OFFSET_SEC", "150")
)  # lateness (keep < watermark to show inclusion/exclusion by tuning)

# Event knobs (keep tiny)
P_EVENT_BASE = float(os.getenv("P_EVENT_BASE", "0.02"))
P_EVENT_IF_HOT = float(os.getenv("P_EVENT_IF_HOT", "0.25"))

random.seed(RANDOM_SEED)


def now_iso_utc_ms(ts=None):
    if ts is None:
        ts = time.time()
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def synth_temp(prev):
    # gentle drift + occasional spike
    base = 35.0 if prev is None else (0.7 * prev + 0.3 * random.uniform(20, 45))
    if random.random() < 0.05:
        base += random.uniform(10, 25)
    return round(max(0.0, base + random.uniform(-2, 2.5)), 1)


def choose_event_type(hot):
    return random.choices(
        ["failure", "maintenance", "inspection"],
        [0.6, 0.3, 0.1] if hot else [0.1, 0.3, 0.6],
        k=1,
    )[0]


def choose_severity(hot):
    return random.choices(
        ["low", "medium", "high"], [1, 2, 4] if hot else [3, 2, 1], k=1
    )[0]


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "iot-demo-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    device_ids = [f"device_{i}" for i in range(1, NUM_DEVICES + 1)]
    device_hot, device_outage, device_late = (
        device_ids[0],
        device_ids[1],
        (
            device_ids[2]
            if NUM_DEVICES >= 3
            else (device_ids[0], device_ids[0], device_ids[0])
        ),
    )
    thresholds = {d: round(random.uniform(THRESH_MIN, THRESH_MAX), 1) for d in device_ids}
    prev_temp = {d: None for d in device_ids}

    tel_interval = 1.0 / max(1.0, TELEMETRY_EPS)
    evt_interval = 1.0 / max(1.0, EVENTS_EPS)
    next_tel, next_evt = time.time(), time.time()
    i_tel, i_evt = 0, 0

    try:
        while True:
            now = time.time()
            win_idx = int(now // WINDOW_SECS)
            win_start = win_idx * WINDOW_SECS
            in_hot_window = win_idx % HOT_EVERY_N_WINDOWS == 0
            in_outage_window = (
                win_idx % (HOT_EVERY_N_WINDOWS + 2) == 0
            )  # simple different cadence
            outage_end = win_start + OUTAGE_SECONDS

            # ---- Telemetry tick ----
            if now >= next_tel:
                dev = device_ids[i_tel]
                t = synth_temp(prev_temp[dev])
                prev_temp[dev] = t

                # Force hot windows for the hot device
                if dev == device_hot and in_hot_window:
                    t = max(t, thresholds[dev] + HOT_TEMP_BOOST)

                # Outage: skip emits for outage device during first OUTAGE_SECONDS of the outage window
                if dev == device_outage and in_outage_window and now < outage_end:
                    pass  # skip produce → incomplete window
                else:
                    # Late: send a fixed fraction with a backdated timestamp
                    ts_for_msg = (
                        (now - LATE_OFFSET_SEC)
                        if (dev == device_late and random.random() < LATE_SHARE)
                        else now
                    )
                    telemetry = {
                        "device_id": dev,
                        "timestamp": now_iso_utc_ms(ts_for_msg),
                        "temperature": t,
                    }
                    print("telemetry", json.dumps(telemetry, separators=(",", ":")))
                    producer.produce(
                        TOPIC_TELEMETRY,
                        key=dev,
                        value=json.dumps(telemetry, separators=(",", ":")),
                    )

                i_tel = (i_tel + 1) % len(device_ids)
                next_tel += tel_interval

            # ---- Events tick ----
            if now >= next_evt:
                dev = device_ids[i_evt]
                # Deterministic anchor during hot windows for hot device (once per window near midpoint)
                if (
                    dev == device_hot
                    and in_hot_window
                    and abs(now - (win_start + WINDOW_SECS / 2))
                    < (evt_interval * len(device_ids))
                ):
                    anchor = {
                        "event_id": str(uuid.uuid4()),
                        "device_id": dev,
                        "event_timestamp": now_iso_utc_ms(win_start + WINDOW_SECS / 2),
                        "event_type": "failure",
                        "severity": "high",
                    }
                    print("hot event", json.dumps(anchor, separators=(",", ":")))
                    producer.produce(
                        TOPIC_EVENTS,
                        key=dev,
                        value=json.dumps(anchor, separators=(",", ":")),
                    )
                else:
                    # Light probabilistic events; hotter windows → higher chance
                    hotish = dev == device_hot and in_hot_window
                    if random.random() < (P_EVENT_IF_HOT if hotish else P_EVENT_BASE):
                        evt = {
                            "event_id": str(uuid.uuid4()),
                            "device_id": dev,
                            "event_timestamp": now_iso_utc_ms(),
                            "event_type": choose_event_type(hotish),
                            "severity": choose_severity(hotish),
                        }
                        print("event", json.dumps(evt, separators=(",", ":")))
                        producer.produce(
                            TOPIC_EVENTS,
                            key=dev,
                            value=json.dumps(evt, separators=(",", ":")),
                        )

                i_evt = (i_evt + 1) % len(device_ids)
                next_evt += evt_interval

            producer.poll(0)
            time.sleep(0.001)

    except KeyboardInterrupt:
        print("Stopping…")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
