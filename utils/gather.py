import ast
import pandas as pd
import itertools
from utils.logs import Logs
from utils.database import Database
from utils.utility import get_w3, get_abi
from collections import namedtuple
from utils.multicall import Multicall
from eth_event import decode_logs, get_topic_map
import json

class Gather(Logs,Database,Multicall):
    
    def __init__(self,conf):

        self.tupple = namedtuple('topic', ['topic','address','initialBlock','endingBlock'])
        Logs.__init__(self,conf)
        Database.__init__(self,conf)
        Multicall.__init__(self,conf)
        
    def trigger_data_gathering(self):
        self.trigger_gather_markets()
        self.get_market_data()
        self.get_token_movement()
        df = self.get_pending_users()
        df.to_csv("output.csv",index=False)
        

    def trigger_gather_markets(self):
                        
        w3          = get_w3(self.conf)
        latestBlock = w3.eth.blockNumber            
        initialBlock  = 1
        input_        = self.tupple(topic='0x7f681fcb8e8dbe5d590262f6cbc95ff1f3106f3faae484a573b74f3de4b0ef96',
                                    address='0x5ed98Ebb66A929758C7Fe5Ac60c979aDF0F4040a',
                                    initialBlock=initialBlock,
                                    endingBlock=latestBlock)
        inputList, outputList = self.trigger_log_gathering(inputList=[input_],runCounter=1)
        df                    = pd.DataFrame(list(itertools.chain(*outputList)))
        df["blockNumber"]         = df["blockNumber"].apply(lambda x: int(x,16))
        df["logIndex"]            = df["logIndex"].apply(lambda x: int(x,16))            
        abi             = get_abi(self.conf,'0x5ed98Ebb66A929758C7Fe5Ac60c979aDF0F4040a')
        topicMap        = get_topic_map(json.loads(abi))
        decodedLogs     = decode_logs(logs=outputList[0],topic_map=topicMap)
        dfLogs          = pd.DataFrame(decodedLogs)
        columnNames     = [data["name"] for data in dfLogs["data"].values[0]]
        for columnName in columnNames:
            dfLogs[columnName]  = self.get_decoded_column(dfLogs["data"],columnName)
        df = pd.concat([df,dfLogs],axis=1)
        df.drop(columns=["topics","data"],inplace=True)
        df.rename(columns={"long":"above","short":"below"},inplace=True)
        self.push_new_df_to_server(df=df,tableName='markets')
                
    def get_decoded_column(self,dataColumn,columnName):
        dataList    = dataColumn.values[0]
        dataNames   = [data["name"] for data in dataList]
        dataTypes   = [data["type"] for data in dataList]
        columnIndex = dataNames.index(columnName)
        dataType    = dataTypes[columnIndex]
        if dataType == 'address':
            return dataColumn.str[columnIndex].apply(lambda x: x["value"])
        elif dataType == 'bytes32':
            w3 = get_w3(self.conf)
            return dataColumn.str[columnIndex].apply(lambda x: w3.toText(hexstr=x["value"])).str.replace("\x00","")
        elif dataType == 'uint256':
            w3 = get_w3(self.conf)
            return dataColumn.str[columnIndex].apply(lambda x: str(x)) 
        return dataColumn.str[columnIndex].apply(lambda x: x["value"])
    
    def get_market_data(self):
        df = self.get_df_from_server("SELECT * FROM MARKETS;")
        marketAddress = df["market"].values[0]
        abi = get_abi(conf=self.conf, address=marketAddress)
        payloads=list()
        #first get market result
        for marketAddress in df["market"].to_list():
            contract = self.w3.eth.contract(address=self.w3.toChecksumAddress(marketAddress),abi=abi)
            payloads.append(self.argListFormat(functionName='result', arg=[], contract=contract, description='times'))
        self.setup_multicall_contract(argList=payloads)
        output = self.run_multicall()        
        dfResult = pd.DataFrame(output,columns=["result"])
        dfResult ["market"] = df["market"]    
        #then get the usd amount in each market
        usdAddress = '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'
        abi = get_abi(conf=self.conf, address=usdAddress )
        payloads=list()
        contract = self.w3.eth.contract(address=usdAddress,abi=abi)
        for marketAddress in df["market"].to_list():
            payloads.append(self.argListFormat(functionName='balanceOf', arg=[self.w3.toChecksumAddress(marketAddress)], contract=contract, description='balanceOf'))
        self.setup_multicall_contract(argList=payloads)
        output = self.run_multicall()        
        dfResult["usdBalance"] = pd.DataFrame(output,columns=["result"]).astype(str)
        #get amount of usd outstanding in markets
        self.push_new_df_to_server(df=dfResult, tableName='market_data')
        
    def get_token_movement(self):
        
        #first get the market short/long tokens
        markets = self.get_df_from_server("SELECT above, below, market from markets;")
        result  = self.get_df_from_server("SELECT result, usdBalance, market from market_data;")
        result  = result[result["usdBalance"].apply(lambda x: int(x)/1e18 > 0)]
        dfMerged = pd.merge(left=result,right=markets,on='market',how='inner')
        dfMerged ["targetToken"] = dfMerged .apply(lambda x: x.above if x.result == 0 else x.below,axis=1)
        
        #get all tokens transfers for the tokens 
        w3          = get_w3(self.conf)
        latestBlock = w3.eth.blockNumber            
        initialBlock  = 1
        inputList = list()
        for tokenAddress in dfMerged["targetToken"].to_list():
            input_        = self.tupple(topic='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                                        address=self.w3.toChecksumAddress(tokenAddress),
                                        initialBlock=initialBlock,
                                        endingBlock=latestBlock)
            inputList.append(input_)
        inputList, outputList = self.trigger_log_gathering(inputList=inputList,runCounter=1)
        outputs = list()
        for nestedOutput in outputList:
            for output in nestedOutput:
                outputs.append(output)
        df = pd.DataFrame(outputs)
        df["blockNumber"]         = df["blockNumber"].apply(lambda x: int(x,16))
        df["logIndex"]            = df["logIndex"].apply(lambda x: int(x,16))            
        abi             = get_abi(self.conf,'0x57Ab1ec28D129707052df4dF418D58a2D46d5f51')
        topicMap        = get_topic_map(json.loads(abi))
        decodedLogs     = decode_logs(logs=outputs,topic_map=topicMap)
        dfLogs          = pd.DataFrame(decodedLogs)
        columnNames     = [data["name"] for data in dfLogs["data"].values[0]]
        for columnName in columnNames:
            dfLogs[columnName ]  = self.get_decoded_column(dfLogs["data"],columnName)
        dfLogs["amount"] = dfLogs["value"].apply(lambda x :ast.literal_eval(x)["value"]/1e18)
        dfLogs.rename(columns={"from":"fromAddress","to":"toAddress"},inplace=True)
        df = pd.concat([df,dfLogs],axis=1)
        df.drop(columns=["topics","data"],inplace=True)
        self.push_new_df_to_server(df=df,tableName='tokens')
        
    def get_pending_users(self):
        sql=\
        '''
        SELECT
            userAddress, sum(amount) as usdCompensation, tokenAddress
        
        FROM
        (
            SELECT 
                toAddress as userAddress, amount, address as tokenAddress
            from
                tokens
            where 
                toAddress <> '0x0000000000000000000000000000000000000000'
                
            UNION ALL
    
            SELECT 
                fromAddress as userAddress, -amount, address as tokenAddress
            from
                tokens
        ) un
        WHERE
            userAddress <> '0x0000000000000000000000000000000000000000'
        GROUP BY 
            userAddress, tokenAddress
        ;                    
        '''
        df = self.get_df_from_server(sql)
        df = df[df.usdCompensation>0].copy()
        df.to_csv("output.csv")
        groupedDF = df[["userAddress","usdCompensation"]].groupby(by=['userAddress']).sum()
        groupedDF.to_csv("output_grouped.csv")

            
            
