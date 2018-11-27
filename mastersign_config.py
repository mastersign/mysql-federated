# _*_ coding: utf-8 _*_

"""
Configuration module to load and merge multiple configuration files in INI format
with options, provided as commandline argument.
"""

import argparse
import configparser
import os


def _option(v):
    if '=' not in v:
        raise argparse.ArgumentTypeError(
            '"{}" is not a valid configuration option.'.format(v))
    key, value = v.split('=', 1)
    if '.' not in key:
        raise argparse.ArgumentTypeError(
            'The key of the configuration option "{}" is invalid.'.format(v))
    section, name = key.split('.', 1)
    return section, name, value


class Configuration(object):

    def __init__(self, config_data):
        self.data = config_data

    def bool(self, section, name):
        return self.data[section].getboolean(name) if section in self.data else None

    def int(self, section, name):
        return self.data[section].getint(name) if section in self.data else None

    def float(self, section, name):
        return self.data[section].getfloat(name) if section in self.data else None

    def str(self, section, name):
        return self.data[section].get(name) if section in self.data else None

    def str_list(self, section, name):
        lst = self.str(section, name)
        return list(map(lambda v: v.strip(), lst.split(','))) if lst is not None else []

    @staticmethod
    def load(args, default_config_file=None):
        p = configparser.ConfigParser()
        if default_config_file and os.path.exists(default_config_file):
            p.read(default_config_file, encoding='utf-8')
        if args.config_files:
            p.read(args.config_files, encoding='utf-8')
        if args.options:
            for section, name, value in args.options:
                p.set(section, name, value)
        return Configuration(p)

    @staticmethod
    def add_config_arguments(parser):
        parser.add_argument('-c', '--config-file', dest='config_files', action='append', required=False,
                            help='A path to a configuration file in UTF-8 encoded INI format. '
                            'This argument can be used multiple times.')
        parser.add_argument('-o', '--options', nargs='+', type=_option, required=False,
                            help='One or more configuration options, given in the format <section>.<option>=<value>.')
