import abc
import logging

import pika

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Consumer(metaclass=abc.ABCMeta):
    """
    An abstract consumer class that provides a hook method
    for custom consumer behaviour
    """
    def __init__(self, host, queue):
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

        self._host = host
        self._queue = queue

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self._host))
        self._channel = self._connection.channel()

        self._channel.basic_consume(self.on_message, queue=self._queue)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """
        Invoked by pika when a message is delivered from RabbitMQ. The
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
        self.execute(self.adapt(self.extract(body)))
        LOGGER.info(' [*] Done...')
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """
        Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    @abc.abstractmethod
    def execute(self, batch):
        """
        Concrete consumers implement this method for custom behaviour
        """
        raise NotImplementedError

    @abc.abstractmethod
    def adapt(self, batch):
        """
        Convert incoming data to a usable custom format
        """
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self, byte_data):
        """
        Extract the incoming byte data
        """
        raise NotImplementedError

    def start(self):
        """
        Begin consuming
        """
        LOGGER.info(' [*] Waiting for logs. To exit press CTRL+C')
        self._channel.start_consuming()


    def stop(self):
        """
        Stop consuming
        """
        LOGGER.info(' [*] Stopping')
        self._channel.stop_consuming()
