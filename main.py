from utils.utility import parse_config
conf = parse_config(r"config/conf.yaml")
from utils.gather import Gather

self = Gather(conf)
self.trigger_data_gathering()