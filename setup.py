#!/usr/bin/env python

from setuptools import setup,find_packages

setup (
  name = "hadoop_g5k",
  version = "0.1",

  packages = ["hadoop_g5k"],
  scripts = ["scripts/hg5k", "scripts/hadoop_engine"],

  install_requires=["execo", " networkx"],

  # PyPI
  author = 'Miguel Liroz Gistau',
  author_email = 'miguel.liroz_gistau@inria.fr',
  description = "A collection of scripts and packages that help in the deployment"
    " and experimental evaluation of Hadoop in Grid5000.",
  url = "https://github.com/mliroz/hadoop_g5k",
  license = "BSD",  
  keywords = "hadoop g5k grid5000 execo",
  
)