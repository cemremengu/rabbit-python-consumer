import argparse
import time

from consumer import Consumer


class DummyConsumer(Consumer):
    """
    A dummy consumer implementation that prints the data it receives
    and sleeps
    """
    def __init__(self, host, queue):
        super().__init__(host, queue)
    def execute(self, data):
        time.sleep(3)


def main(opts):
    """Bootstrap and run the consumer"""
    consumer = DummyConsumer(opts["rabbit"], opts["queue"])
    consumer.start()

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='A dummy worker')
    parser.add_argument('-r', '--rabbit', help='Rabbit host address', required=True)
    parser.add_argument('-q', '--queue', help='Queue name to consume from', required=True)
    return vars(parser.parse_args())

if __name__ == '__main__':
    main(parse_args())
