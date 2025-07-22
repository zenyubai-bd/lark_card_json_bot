# parse bot keys in config.txt

# get current path
import os
def get_current_path():
    return os.path.dirname(os.path.abspath(__file__))

PATH = get_current_path()
config_path = os.path.join(PATH, "config.txt")

def parse_env():
    secrets = {}
    with open(config_path) as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                secrets[key.strip()] = value.strip().strip('"')

    APP_ID = secrets["APP_ID"]
    APP_SECRET = secrets["APP_SECRET"]
    TEMPLATE_ID = secrets["TEMPLATE_ID"]
    return APP_ID, APP_SECRET, TEMPLATE_ID