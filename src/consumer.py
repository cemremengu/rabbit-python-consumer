import logging
import pika
import abc

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Consumer(metaclass=abc.ABCMeta):

    def __init__(self, host, queue):
        
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

        self._host = host
        self._queue = queue

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self._host))
            
        self._channel = self._connection.channel()

        self._channel.basic_consume(self.on_message,
                    queue=self._queue)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body
        """
        LOGGER.info(' [*] Working...')
        self.execute(body)
        LOGGER.info(' [*] Done...')
        self.acknowledge_message(basic_deliver.delivery_tag)
        
    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    @abc.abstractmethod
    def execute(self, data): raise NotImplementedError

    def start(self):
        LOGGER.info(' [*] Waiting for logs. To exit press CTRL+C')
        self._channel.start_consuming()
