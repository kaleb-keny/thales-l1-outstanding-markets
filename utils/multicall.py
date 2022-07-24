from web3._utils.abi import get_abi_output_types
from utils.utility import get_abi, get_w3
from collections import namedtuple

class Multicall():
    def __init__(self,conf):
        self.conf        = conf
        self.w3          = get_w3(conf=conf) 
        self.load_multicall()
        self.argListFormat = namedtuple('multicall',
                                        ['functionName',
                                         'arg',
                                         'contract',
                                         'description'])
    
    def load_multicall(self):
        address      = self.conf["multicaller"]
        abi          = get_abi(conf=self.conf, 
                               address=address)
        self.multiCallContract = self.w3.eth.contract(address=address,abi=abi)
    
    def setup_multicall_contract(self, argList):
        self.payload     = list()
        self.decoderList = list()
        for arg in argList:
            functionName = arg.functionName
            contract     = arg.contract
            args         = arg.arg
            callData = contract.encodeABI(fn_name=functionName, args=args)
            self.payload.append((contract.address, callData))            
            fn = contract.get_function_by_name(fn_name=functionName)
            self.decoderList.append(get_abi_output_types(fn.abi))
    
    def run_multicall(self):
        outputs = self.multiCallContract.functions.aggregate(self.payload).call()[1]
        return [self.w3.codec.decode_abi(decoder, output) for output, decoder in zip(outputs,self.decoderList)]
