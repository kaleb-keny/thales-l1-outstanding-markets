from utils.utility import run_post_request
from funcy import chunks as chunks
import asyncio
import nest_asyncio
from itertools import cycle
nest_asyncio.apply()
from aiohttp import ClientSession
from web3.providers.base import JSONBaseProvider
import pandas as pd

class Logs():

    def __init__(self,conf):

        self.conf    = conf
        self.rpcList = cycle(conf["node"])
                
    def trigger_log_gathering(self,inputList,runCounter):

        if runCounter == 50:
            raise NotImplementedError(f"After {runCounter} runs failed to gather all receipts, manual intervention needed.")

        #First run in recursive loop
        if runCounter == 1:
            self.outputList = list()

        #Runs on all missing Blocks
        self.run_batch_iteration(inputList)

        #get input list
        inputList = self.find_missing_inputs()

        #Reruns Recursively in case we have missing transactions
        if len(inputList)>0:
            return self.trigger_log_gathering(inputList=inputList,runCounter=runCounter+1)

        #After the last missing tupples is run, topics list will
        #contail only 1 list of tupples and 1 list of topics (appending undone)
        inputList, outputList   = self.outputList[0]
        inputs                  =  [input for input, output in zip(inputList,outputList) if "result" in output]
        outputs                 =  [output["result"] for output in outputList            if "result" in output]
        return [inputs, outputs]

    def run_batch_iteration(self,inputList):
        
        batchCount = 10

        for inputChunk in  chunks(batchCount,inputList):
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.setup_post_request(inputChunk))
            loop.run_until_complete(future)
            self.outputList.append([inputChunk,future.result()])


    def find_missing_inputs(self):

        inputsToSplit=list()#inputs that need splitting
        inputsToRedo =list()#inputs that need to be redone because of limit on asynch batch calls
        inputsCovered=list()#inputs that have results (i.e. done correctly)
        responsesCovered=list()#list of responses

        for inputs, responses in self.outputList:

            #First find responses where we have errors
            #1 Type of error could be due to the tupple band being too large (i.e. more than 10k results)
            #or query taking too much time... in these case we need to split after performing calibration
            #https://infura.io/docs/ethereum/json-rpc/eth_getLogs

            inputsToSplit.extend([tupple for tupple, response in zip(inputs,responses)
                          if "error" in response.keys() and
                                          ("query returned more than 10000 results" in response["error"]["message"] or
                                           "query timeout exceeded" in response["error"]["message"] or
                                           "request failed or timed out" in response["error"]["message"])])

            inputsToRedo.extend([tupple for tupple, response in zip(inputs,responses)
                          if "error" in response.keys() and
                                          "project ID request rate exceeded" in response["error"]["message"]])

            inputsCovered.extend([tupple for tupple, response in zip(inputs,responses)\
                                               if "result" in response.keys()\
                                                   and not response["result"] is None
                                                   and len(response["result"]) > 0 ])
            responsesCovered.extend([response for tupple, response in zip(inputs,responses)\
                                               if "result" in response.keys()\
                                                   and not response["result"] is None
                                                   and len(response["result"]) > 0 ])

            inputsUnknownError = [[tupple, response] for tupple, response in zip(inputs,responses)
                          if "error" in response.keys() and  not
                                          ("query returned more than 10000 results" in response["error"]["message"] or
                                           "query timeout exceeded" in response["error"]["message"] or
                                           "project ID request rate exceeded" in response["error"]["message"] or
                                           "request failed or timed out" in response["error"]["message"])]
            if len(inputsUnknownError)>0:
                raise NotImplementedError(f"{inputsUnknownError} New type of error found in the inputs, manual intervention required!")


        #Kills all data except captured in Receipts, in order to increase the speed. 
        #Note that without the deletion, the model still works fine
        #but it'll take more time as it'll also iterate on previous splits
        self.outputList=list()
        self.outputList.append([inputsCovered,responsesCovered])

        #Generates Missing inputs that need to be gathered in the next run
        #after doing some calibration
        if len(inputsToSplit)>0:
            inputsToSplit = self.split_input_list(inputList=inputsToSplit)

        #Then does the union on split and redo inputs
        return list(set(inputsToSplit).union(set(inputsToRedo)))


    def split_input_list(self,inputList):
        #Splits tupples in half - simple approach
        #less error prone, but slower

        newInputList = list()

        #Splits Tupples in Half
        for input_ in inputList:
            topic   = input_.topic
            address = input_.address
            initial = input_.initialBlock
            mid     = (input_.initialBlock+input_.endingBlock)//2
            end     = input_.endingBlock
            network = input_.network

            newInputList.extend([(topic,
                                  address,
                                  initial,
                                  mid,
                                  network)
                                 ])
            newInputList.extend([(topic,
                                  address,
                                  mid+1,
                                  end,
                                  network)
                                 ])

        df = pd.DataFrame(newInputList,
                          columns=["topic",
                                   "address",
                                   "initialBlock",
                                   "endingBlock",
                                   "network"])

        return list(df.itertuples(index=False,name="eth_getLogs"))

    async def setup_post_request(self,inputList):

        tasks = []
        method='eth_getLogs'
        base_provider = JSONBaseProvider()

        async with ClientSession() as session:

            for input_ in inputList:
                params=[{"fromBlock":hex(input_.initialBlock),
                         "address":input_.address,
                         "toBlock":hex(input_.endingBlock),
                         'topics':[input_.topic]}]

                data = base_provider.encode_rpc_request(method, params)

                task = asyncio.ensure_future(run_post_request(session=session,
                                                              url=next(self.rpcList),
                                                              data=data))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses