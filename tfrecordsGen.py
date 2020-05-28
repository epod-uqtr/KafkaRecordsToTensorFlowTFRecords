import argparse
import os
import sys
import threading, logging, time
from typing import Optional, Callable, Any, Iterable, Mapping

from kafka import KafkaConsumer
from os.path import join
import tensorflow as tf


class tfrecordsGen(threading.Thread):
    daemon = True

    def __init__(self, output_file_path):
        super().__init__()
        self.output_file_path = output_file_path

    def _bytes_feature(self, value):
        return tf.train.Feature(
            bytes_list=tf.train.BytesList(value=[value])
        )

    def get_feature(self, dataset, label):
        return {
            'dataset': self._bytes_feature(tf.compat.as_bytes(dataset)),
            'label': self._bytes_feature(tf.compat.as_bytes(label))
        }

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9091',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['MEDICAL_FILE_LIPID_PROFILE'])

        output_path = join(self.output_file_path, "tfrecords")
        os.makedirs(output_path)
        path = join(output_path, "tf.tfrecords")
        # filenames = [path]
        # raw_dataset = tf.data.TFRecordDataset(filenames)
        # for raw_record in raw_dataset.take(10):
        #     print(repr(raw_record))

        with tf.io.TFRecordWriter(path) as writer:
            for message in consumer:
                #print(message.value)
                example = tf.train.Example(
                    features=tf.train.Features(
                        feature=self.get_feature(message.value, message.value)
                    ))
                writer.write(example.SerializeToString())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--path', '-p',
                   default=None)
    args = p.parse_args()
    vargs = vars(args)
    if args.path is None:
        print('The path is null.')
        sys.exit()

    threads = [
        tfrecordsGen(output_file_path=args.path),
    ]
    for t in threads:
        t.start()
    time.sleep(1)