from confluent_kafka import Consumer, Producer


def get_producer(bootstrap_servers):
    return Producer({"bootstrap.servers": bootstrap_servers})


def get_consumer(bootstrap_servers, group_id, auto_offset_reset="earliest"):
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
        }
    )
