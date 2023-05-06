import sys
from kafka import KafkaConsumer

topic = sys.argv[1]
consumer = KafkaConsumer(
	bootstrap_servers=["localhost:19092"],
	group_id="consumer_group1",
	client_id="demo_user1",
	auto_offset_reset="earliest",
	enable_auto_commit=False,
	consumer_timeout_ms=6000
	)

print('Topic: ', topic)

try:
	consumer.subscribe(topic)
	while True:
		msg = consumer.poll(timeout_ms=10)
		for tp, message in msg.items():
			for m in message:
				print ("%s:%d:%d: key=%s value=%s" % (m.topic, m.partition, m.offset, m.key, m.value))
except KeyboardInterrupt:
        stored_exception=sys.exc_info()

finally:
	# Close down consumer to commit final offsets.
	consumer.close()

#if stored_exception:
#    raise stored_exception[0], stored_exception[1], stored_exception[2]

sys.exit()