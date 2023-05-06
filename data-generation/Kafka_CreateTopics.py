from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:19092", 
    client_id='Admin'
)

topic_names = [
    "data-demo-venues",
    "data-demo-customers",
    "data-demo-streams",
    "data-demo-addresses",
    "data-demo-events",
    "data-demo-tickets",
    "data-demo-artists",
    "artist-ticket-ratio",
    "ct-favorite-artist-topic",
    "superfan-topic",
]

for topic in topic_names:
    print(f"Creating topic {topic}")
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)