import os
import yaml

with open(os.path.join(os.path.dirname(__file__), "defaults.yaml")) as r:
    Config = yaml.safe_load(r)
