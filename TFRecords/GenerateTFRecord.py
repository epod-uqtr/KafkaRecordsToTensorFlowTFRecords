from __future__ import absolute_import, division, print_function
import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

from copy import deepcopy
from skimage.io import imread, imsave
import numpy as np
from PIL import Image
import cv2
import tensorflow as tf
from os.path import join


class GenerateTFRecord:

    def __init__(self, data, label, output_file_path):
        self.data = data
        self.label = label
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

    def createTfRecord(self, data, label, output_file_path):
        with tf.io.TFRecordWriter(output_file_path) as writer:
            example = tf.train.Example(
                features=tf.train.Features(
                    feature=self.get_feature(data, label)
                ))
            writer.write(example.SerializeToString())

    def run(self):
        output_path = join(self.output_file_path, "tfrecords")
        os.makedirs(output_path)

        path = join(output_path, "tf.tfrecords")
        self.createTfRecord(path, self.data, self.label)
