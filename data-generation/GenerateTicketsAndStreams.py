import pandas as pd
import random, time
from kafka import KafkaProducer

next_ticket_id = 2358
next_stream_id = 124

artist_df = pd.read_csv("artist.csv")
event_df = pd.read_csv("event.csv")
customer_df = pd.read_csv("customer.csv")

producer = KafkaProducer(
    bootstrap_servers = "localhost:19092",
    client_id="Generator"
)

def generate_ticket():
    global next_ticket_id
    global customer_df
    global event_df

    id = next_ticket_id
    next_ticket_id += 1

    msg = "{"
    msg +=  '"id":"' + str(id) + '",'
    msg += '"customerid":"' + str(customer_df.sample()["id"].values[0]) + '",'
    msg += '"eventid":"' + str(event_df.sample()["id"].values[0]) + '",'
    msg += '"price":"' + str(random.randint(50, 500)) + '"'
    msg += "}"

    future = producer.send(
        "data-demo-tickets",
        value=str.encode(msg),
        key=str.encode(str(id))
    )

    future.add_callback(lambda: print("produced ticket " + str(id)))
    future.add_errback(lambda e: print("error producing ticket " + str(id) + ": " + str(e)))

def generate_stream():
    global next_stream_id
    global customer_df
    global artist_df

    id = next_stream_id
    next_stream_id += 1

    msg = "{"
    msg += '"id":"' + str(id) + '",'
    msg += '"customerid":"' + str(customer_df.sample()["id"].values[0]) + '",'
    msg += '"artistid":"' + str(artist_df.sample()["id"].values[0]) + '",'
    msg += '"streamtime":"' + str(random.randint(10, 240)) + '"'
    msg += "}"

    future = producer.send(
        "data-demo-streams",
        value=str.encode(msg),
        key=str.encode(str(id))
    )

    future.add_callback(lambda: print("produced stream " + str(id)))
    future.add_errback(lambda e: print("error producing stream " + str(id) + ": " + str(e)))

while True:
    # Generate more streams than tickets
    roll = random.randint(1, 100)
    if roll < 30:
        generate_ticket()
    if roll < 95:
        generate_stream()
    
    time.sleep(0.5)
    