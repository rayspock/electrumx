import json
from aiohttp import web
from aiorpcx import RPCError
from functools import reduce
from decimal import Decimal
from electrumx.lib.hash import hash_to_hex_str
import electrumx.lib.util as util

import sys
import time
import math
import codecs
import asyncio

BAD_REQUEST = 1
MAX_TX_QUERY = 50


class HttpHandler(object):

    def __init__(self, session_mgr, db, mempool, peer_mgr, kind):
        self.logger = util.class_logger(__name__, self.__class__.__name__)
        self.session_mgr = session_mgr
        self.db = db
        self.mempool = mempool
        self.peer_mgr = peer_mgr
        self.kind = kind
        self.env = session_mgr.env
        self.coin = self.env.coin
        self.client = 'unknown'
        self.anon_logs = self.env.anon_logs
        self.txs_sent = 0
        self.log_me = False
        # coroutine function
        self.daemon_request = self.session_mgr.daemon_request
        self.prec = int(math.log(self.coin.VALUE_PER_COIN, 10))

    async def estimatefee(self, request):
        query_str = request.rel_url.query
        nb = util.parse_int(query_str['nbBlocks'], 2) if 'nbBlocks' in query_str else None
        if nb is not None:
            fee = await self.daemon_request('estimatefee', nb)
        else:
            fee = await self.daemon_request('estimatefeenoarg')
        res = {"fee": format(fee, '.8f')}
        return web.json_response(res)

    async def send_transaction(self, request):
        body = await request.json()
        try:
            hex_hash = await self.session_mgr.broadcast_transaction(body['rawtx'])
        except Exception as ex:
            return web.Response(status=400, text=str(ex))
        else:
            res = {"txid": hex_hash}
            return web.json_response(res)

    async def address_listunspent(self, request):
        '''Return the list of UTXOs of an address.'''
        addrs = request.match_info.get('addrs', '')
        if not addrs:
            return web.Response(status=404)
        list_addr = list(dict.fromkeys(addrs.split(',')))
        list_tx = list()
        for address in list_addr:
            hashX = self.address_to_hashX(address)
            list_utxo = await self.hashX_listunspent(hashX)
            for utxo in list_utxo:
                tx_detail = await self.transaction_get(utxo["tx_hash"], True)
                list_tx.append(await self.wallet_unspent(address, utxo, tx_detail))
        return web.json_response(list_tx)

    async def address(self, request):
        addr = request.match_info.get('addr', '')
        if not addr:
            return web.Response(status=404)
        addr_balance = await self.address_get_balance(addr)
        confirmed_sat = addr_balance["confirmed"]
        unconfirmed_sat = addr_balance["unconfirmed"]
        res = {"addrStr": addr,
               "balance": float(self.coin.decimal_value(confirmed_sat)),
               "balanceSat": confirmed_sat,
               "unconfirmedBalance": float(self.coin.decimal_value(unconfirmed_sat)),
               "unconfirmedBalanceSat": addr_balance["unconfirmed"]}
        return web.json_response(res)

    async def history(self, request):
        '''
        The history api requries two step of querying to meet our use case.
        The first step is to get pairs of tx_hash and block_height as history provided by electrumx.
        The second step is to ask coin core for transaction details.
        '''
        # path variable
        addrs = request.match_info.get('addrs', '')
        if not addrs:
            return web.Response(status=404)
        
        # query string
        query = request.rel_url.query
        query_from = util.parse_int(query['from'], 0)
        query_to   = util.parse_int(query['to'], MAX_TX_QUERY)
        
        if query_from < 0:
            return web.Response(
                status=400,
                text=f'Invalid parameter: "from" must be greater than or equal to 0')

        if query_to < 0:
            return web.Response(
                status=400,
                text=f'Invalid parameter: "to" must be greater than or equal to 0')

        if query_from > query_to:
            return web.Response(
                status=400,
                text=f'Invalid parameter: "from" must be less than "to"')

        if query_to > query_from + MAX_TX_QUERY:
            query_to = query_from + MAX_TX_QUERY

        tasks_fetch_tuples = []
        for addr in addrs.split(','):
            # query electrumx history db
            tasks_fetch_tuples.append(self.get_history(addr))
            
        list_history_by_address = await asyncio.gather(*tasks_fetch_tuples)
        list_joined_history = [ entry for per_address in list_history_by_address for entry in per_address ]
        
        max_height = max([ entry['height'] for entry in list_joined_history ])
        # sort history by block height and pop unconfirmed history to the top
        list_joined_history.sort(key=lambda x: x['height'] if x['height']!=0 else max_height+1, reverse=True)
        
        tx_hashes = [ entry["tx_hash"] for entry in list_joined_history[query_from:query_to] ]

        tasks_fetch_data = []
        for hash in tx_hashes:
            tasks_fetch_data.append(self.transaction_get_detail(hash))
            
        tx_details = await asyncio.gather(*tasks_fetch_data)
        
        task_hist = []
        for hist, tx in zip(list_joined_history[query_from:query_to], tx_details):
            task_hist.append(self.history_factory(hist['height'], tx))
            
        list_history = await asyncio.gather(*task_hist)
        
        response = {
            "totalItems": len(list_joined_history),
            "items": list_history,
        }
                
        jsonStr = json.dumps(response, cls=DecimalEncoder)        
        return web.json_response(json.loads(jsonStr))

    async def history_factory(self, height, tx):
                
        if tx is None:
            return None
        
        if 'time' in tx:
            time = tx["time"]
        else:
            # This is unconfirmed transaction, so get the time from memory pool
            # The time the transaction entered the memory pool, Unix epoch time format
            mempool = await self.mempool_get(True)
            tx = mempool.get(tx["txid"])
            time = tx.get('time') if tx is not None else None
                             
        if time is None:
            raise RPCError(BAD_REQUEST, f'cannot get the transaction\'s time')
        
        vin = tx["vin"]
        vin_n = [ v.get('vout') for v in tx["vin"] ]
        vin_hashes = [ v.get('txid') for v in tx["vin"] ]

        vin_details = []
        list_vin_bytes = await self.transaction_get_multiple(vin_hashes)
        for bytes in list_vin_bytes:
            # decode hashed transaction data
            vin_details.append(self.coin.DESERIALIZER(bytes).read_tx())
        
        prev_outs = []
        for i, d in enumerate(vin_details):
            # extract specific output from input transaction detail
            if d is not None: prev_outs.append(d.outputs[vin_n[i]])
            else: prev_outs.append(None)
        
        # convert hex bytes to text string
        list_pk_script = [ codecs.encode(o.pk_script, 'hex').decode('ascii') for o in prev_outs ]
        list_descriptor_info = await self.descriptorinfo_get_multiple(list_pk_script, 'raw')
                
        list_descriptor = [ info['descriptor'] for info in list_descriptor_info ]
        list_address = await self.address_get_multiple(list_descriptor)
        
        for i in range(len(vin)):
            if 'txid' in vin[i]:
                vin[i] = {
                    "txid": vin[i]["txid"],
                    "addr": list_address[i][0],
                    #"valueSat": prev_outs[i].value,
                    "value": prev_outs[i].value / self.coin.VALUE_PER_COIN,
                }

        vout = tx["vout"]
        value_in = round(Decimal(str(reduce(lambda sum, x: sum + x["value"], vin, 0))), self.prec)
        value_out = round(Decimal(str(reduce(lambda sum, x: sum + x["value"], vout, 0))), self.prec)
        
        return {
            "txid": tx["txid"],
            "blockheight": height,
            "vin": vin,
            "vout": vout,
            "valueOut": value_out,
            "valueIn": value_in,
            "fees": value_in - value_out,
            "confirmations": tx["confirmations"] if 'confirmations' in tx else 0,
            "time": time}

    async def wallet_unspent(self, address, utxo, tx_detail):
        height = utxo["height"]
        satoshis = utxo["value"]
        vout = utxo["tx_pos"]
        confirmations = tx_detail["confirmations"] if 'confirmations' in tx_detail else 0
        list_vout = tx_detail["vout"]
        list_pick = []
        for item in list_vout:
            '''In case some vout will contain OP_RETURN and no addresses key'''
            addr = item["scriptPubKey"]["addresses"][0] if 'addresses' in item["scriptPubKey"] else ""
            n = item["n"] if 'n' in item else ""
            if addr == address or (addr == "" and n == vout):
                list_pick.append(item)

        if len(list_pick) > 0:
            obj = list_pick[0]
            amount = obj["value"]
            script_pub_key = obj["scriptPubKey"]["hex"]
        else:
            raise Exception(f'cannot get the transaction\'s list of outputs from address:{address}')
        return {"address": address,
                "txid": tx_detail["txid"],
                "vout": vout,
                "scriptPubKey": script_pub_key,
                "amount": amount,
                "satoshis": satoshis,
                "height": height,
                "confirmations": confirmations}

    def address_to_hashX(self, address):
        try:
            return self.coin.address_to_hashX(address)
        except Exception:
            pass
        raise RPCError(BAD_REQUEST, f'{address} is not a valid address')

    async def address_get_balance(self, address):
        '''Return the confirmed and unconfirmed balance of an address.'''
        hashX = self.address_to_hashX(address)
        return await self.get_balance(hashX)

    async def get_history(self, address):
        '''Return the confirmed and unconfirmed history of an address.'''
        hashX = self.address_to_hashX(address)
        
        uth = await asyncio.create_task(self.unconfirmed_history(hashX))        
        cth = await asyncio.create_task(self.session_mgr.history(hashX)) # list of tuple
        cth = [{'height': height, 'tx_hash': hash_to_hex_str(tx_hash)} for tx_hash, height in cth ]
        # cth.reverse() # make the result identical to the original one

        return uth + cth

    async def get_balance(self, hashX):
        utxos = await self.db.all_utxos(hashX)
        confirmed = sum(utxo.value for utxo in utxos)
        unconfirmed = await self.mempool.balance_delta(hashX)
        return {'confirmed': confirmed, 'unconfirmed': unconfirmed}

    async def unconfirmed_history(self, hashX):
        # Note unconfirmed history is unordered in electrum-server
        # height is -1 if it has unconfirmed inputs, otherwise 0
        return [{
            'fee': tx.fee,
            'tx_hash': hash_to_hex_str(tx.hash),
            'height': -tx.has_unconfirmed_inputs,
        } for tx in await self.mempool.transaction_summaries(hashX)]

    async def confirmed_and_unconfirmed_history(self, hashX):
        '''latest in the blockchain first.'''
        # Note history is ordered but unconfirmed is unordered in e-s
        history = await self.session_mgr.history(hashX)
        conf = [{'tx_hash': hash_to_hex_str(tx_hash), 'height': height}
                for tx_hash, height in history]
        return await self.unconfirmed_history(hashX) + list(reversed(conf))

    def assert_tx_hash(self, value):
        '''Raise an RPCError if the value is not a valid transaction
        hash.'''
        try:
            if len(util.hex_to_bytes(value)) == 32:
                return
        except Exception:
            pass
        raise RPCError(BAD_REQUEST, f'{value} should be a transaction hash')

    async def hashX_listunspent(self, hashX):
        '''Return the list of UTXOs of a script hash, including mempool
        effects.'''
        utxos = await self.db.all_utxos(hashX)
        utxos = sorted(utxos)
        utxos.extend(await self.mempool.unordered_UTXOs(hashX))
        spends = await self.mempool.potential_spends(hashX)

        return [{'tx_hash': hash_to_hex_str(utxo.tx_hash),
                 'tx_pos': utxo.tx_pos,
                 'height': utxo.height, 'value': utxo.value}
                for utxo in utxos
                if (utxo.tx_hash, utxo.tx_pos) not in spends]

    async def transaction_get(self, tx_hash, verbose=False):
        '''Return the serialized raw transaction given its hash

        tx_hash: the transaction hash as a hexadecimal string
        verbose: passed on to the daemon
        '''
        self.assert_tx_hash(tx_hash)
        if verbose not in (True, False):
            raise RPCError(BAD_REQUEST, f'"verbose" must be a boolean')

        return await self.daemon_request('getrawtransaction', tx_hash, verbose)
    
    async def transaction_get_detail(self, tx_hash):
        '''
        Return the serialized raw transaction given its hash\n
        tx_hash: the transaction hash as a hexadecimal string
        verbose: passed on to the daemon
        '''
        self.assert_tx_hash(tx_hash)
        return await self.daemon_request('getrawtransaction', tx_hash, True)
    
    async def transaction_get_multiple(self, tx_hashes):
        '''
        Return the a bunch of serialized raw transaction given hash values\n
        tx_hashes: list of transaction hash;
        verbose: passed on to the daemon;
        '''
        return await self.daemon_request('getrawtransactions', tx_hashes)
    
    async def transaction_get_multiple_details(self, tx_hashes):
        '''
        Return the a bunch of serialized raw transaction given hash values\n
        tx_hashes: list of transaction hash;
        verbose: passed on to the daemon;
        '''
        return await self.daemon_request('getdetailedtransactions', tx_hashes)

    async def descriptorinfo_get_multiple(self, descriptors, script_type):
        return await self.daemon_request('getdescriptorsinfo', descriptors, script_type)
        
    async def address_get_multiple(self, descriptors):
        return await self.daemon_request('getderiveaddresses', descriptors)
    
    async def mempool_get(self, verbose=False):
        '''Returns all transaction ids in memory pool as a json array of string transaction ids

        verbose: True for a json object, false for array of transaction ids
        '''
        if verbose not in (True, False):
            raise RPCError(BAD_REQUEST, f'"verbose" must be a boolean')

        return await self.daemon_request('getrawmempool', verbose)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
