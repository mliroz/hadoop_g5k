#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="hadoop_g5k",
    version="0.1",

    packages=["hadoop_g5k", "hadoop_g5k/util", "hadoop_g5k/engine",
              "hadoop_g5k/ecosystem"],
    scripts=["scripts/hg5k", "scripts/hadoop_engine", "scripts/spark_g5k",
             "scripts/mahout_g5k"],

    install_requires=["execo", " networkx"],

    # PyPI
    author='Miguel Liroz Gistau',
    author_email='miguel.liroz_gistau@inria.fr',
    description="A collection of scripts and packages that help in the "
                "deployment and experimental evaluation of Hadoop in Grid5000.",
    url="https://github.com/mliroz/hadoop_g5k",
    license="BSD",
    keywords="hadoop spark g5k grid5000 execo",
  
)