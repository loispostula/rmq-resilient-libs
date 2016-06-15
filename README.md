# rmq-resilient-libs
## Resilient
These libs provide resilient connection to a RabbitMQ queue. Respawning the 
connections whenever they have been dropped (after having wait a given timeout).

## RMQConsumer
The RMQConsumer is a simple libs to consume message and retry them with a delay
when the processing could not succeeded. It uses pika to connect to RabbitMQ 
and a `SelectConnection` to handle the connection.

Whenever a message arrive, is consumed, this consumer will forward it to a 
callback function, which you need to define. This function take the same 
arguments as a normal pika consumer callback but must return a boolean value
reflecting the fact that you want to acknowledge (`basic_ack`) or 
non-acknowledge (`basic_nack`) the message.

In the case of an `ack` nothing will be done to the message beside 
acknowledging it.

In the case of an `nack` the consumer will wait a specified amount of time
before sending the `nack`. You have not to worry about the connection being 
drop when the timeout you choose is big, because the consumer will take care 
of that.

### Usage
To create a consumer, you need to provide:
 - `callback`:  Your custom function use to handle the messages, 
 it should return a boolean value
 - `url`: The RabbitMQ url to connect to
 - `port`: The RabbitMQ port to connect through
 - `credentials`: The RabbitMQ credential (type: `pika.PlainCredentials`)
 - `exchange`: The name of the exchange to consume from
 - `exchange_type`: The type of the exchange
 - `queue`: Dict representing the queue, only the name is compulsory 
 and the structure is as follow
 
 ```
 {
    'name': '',
    'passive', True/False,
    'durable', True/False,
    'exclusive', True/False,
    'auto_delete', True/False,
    'nowait', True/False,
    'arguments', None/{}
 }
 ```
 - `routing_keys`: List of routing keys that the queue should be bind to
 - `delay`: The time to wait before sending the `nack` when a message could 
 not be processed
 
 Once created you can run it by doing:
 
 ```
 try:
    consumer.run()
 except KeyboardInterrupt:
    consumer.stop()
 ```
 
 The `KeyboardInterrupt` catch is important if you want the consumer to close
 cleanly all of his connection.
