from consumer import Consumer
import time
import argparse

class DummyConsumer(Consumer):
    def __init__(self, host, queue):
        super().__init__(host, queue)
    def execute(self, data):
        print(data)
        time.sleep(3)


def main(opts):
    consumer = DummyConsumer(opts["rabbit"], opts["queue"])
    consumer.start()


def parse_args():
    parser = argparse.ArgumentParser(description='A dummy worker')
    parser.add_argument('-r','--rabbit', help='Rabbit host address', required=True)
    parser.add_argument('-q','--queue', help='Queue name to consume from', required=True)
    return vars(parser.parse_args())

if __name__ == '__main__':
    opts = parse_args()
    main(opts)


