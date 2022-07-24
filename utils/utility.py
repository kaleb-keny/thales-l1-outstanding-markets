import time
from sqlalchemy import create_engine
import requests
import yaml
import web3 as w3

def parse_config(path):
    with open(path, 'r') as stream:
        return  yaml.load(stream, Loader=yaml.FullLoader)

def get_w3(conf):
    rpc = conf["node"][0]
    web3 = w3.Web3(w3.HTTPProvider(rpc))
    return web3

def get_abi(conf,address):
    headers      = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    url = f'https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={conf["etherscan"]}'
    while True:
        try:
            result = requests.get(url,headers=headers).json()
            if result["status"] != '0':
                return result["result"]
            else:
                print("error seen with abi fetch, trying again")
                time.spleep(3)
                continue
        except:
            print("error seen with abi fetch, trying again")
            time.sleep(3)

async def run_post_request(session,url,data):
    async with session.post(url=url,data=data) as response:
        return await response.json(content_type=None) 

def get_mysql_connection(conf):
    sqlConf =conf.get('mysql')
    engine_string = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(sqlConf["user"],sqlConf["password"],sqlConf["host"],sqlConf["database"])
    engine = create_engine(engine_string)
    con = engine.connect()
    return con          