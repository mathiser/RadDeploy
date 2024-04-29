import os

import yaml
from pydantic.v1.utils import deep_update


def load_configs(conf_dir, current_conf="current_config.yaml"):
    config = {}
    for file in sorted(os.listdir(conf_dir)):
        if file.endswith(".yaml") or file.endswith(".yml"):
            with open(os.path.join(conf_dir, file), "r") as r:
                config = deep_update(config, yaml.safe_load(r))

    for k, v in config.items():
        if k in os.environ.keys():
            config[k] = os.environ.get(k)

    with open(current_conf, "w") as f:
        f.write(yaml.dump(config))

    return config