# -*- coding: utf-8 -*-
"""A setup file reader for the aws_console module"""

import os
import ConfigParser


class ConfigReader:
    """
    A class to implement custom cfg file reader
    """
    def __init__(self):
        self.cfg_file = os.path.join(os.path.dirname(__file__), 'setup.cfg')
        self._config = ConfigParser.ConfigParser()
        self._config.read(self.cfg_file)

    def read(self, section, item):
        """
        A read method to read key and values
        :return:
        """
        return self._config.get(section, item)


conf = ConfigReader()
