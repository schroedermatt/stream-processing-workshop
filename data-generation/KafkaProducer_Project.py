import sys, names, time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic


topic = sys.argv[1]
sourcefile = sys.argv[2]

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:19092", 
    client_id='Admin'
)

tl = admin_client.list_topics()
if topic in tl:
  print(f"Topic exists, no need to create.")
else:
  topic_list = []
  topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
  admin_client.create_topics(new_topics=topic_list, validate_only=False)
  print(f"Create passed topic")

admin_client.close();

time.sleep(3)

producer = KafkaProducer(
    bootstrap_servers = "localhost:19092",
    client_id="Producer"
)


df = pd.read_csv(sourcefile)

def on_success(metadata):
  print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

# Produce asynchronously with callbacks

msg = ""
for index, row in df.iterrows():
  msg = '{"'
  for col in df.columns:
    msg = msg + str(col) + '":"' + str(row[col]) + '","'
  msg = msg[:-2]
  msg = msg + '}'
  #print(msg)
  #msg = '{"id":"' + str(row["id"]) + '","name":"' + str(row["name"]) + '","genre":"' + str(row["genre"]) + '"}'
  future = producer.send(
    topic,
    value=str.encode(msg),
    key=str.encode(str(int(row["id"])))
  )
  future.add_callback(on_success)
  future.add_errback(on_error)
  msg = ""

producer.flush()
producer.close()