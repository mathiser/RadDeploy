import os
import yaml

with open(os.environ.get("CURRENT_CONF"), "r") as r:
    config = yaml.safe_load(r)

exit(os.system(f"/usr/local/bin/python3 -m pynetdicom echoscu {config['AE_HOSTNAME']} {config['AE_PORT']} -aet {config['AE_TITLE']}"))