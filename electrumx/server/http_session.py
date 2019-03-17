import json
from aiohttp import web
from aiorpcx import RPCError
from functools import reduce
from decimal import Decimal
from electrumx.lib.hash import hash_to_hex_str
import electrumx.lib.util as util

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
        self.bw_limit = self.env.bandwidth_limit
        self.daemon_request = self.session_mgr.daemon_request

    async def estimatefee(self, request):
        query_str = request.rel_url.query
        nb = util.parse_int(query_str['nbBlocks'], 2) if 'nbBlocks' in query_str else 2
        fee = await self.daemon_request('estimatefee', nb)
        res = {str(nb): format(fee, '.8f')}
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
        '''Query parameters check.'''
        addrs = request.match_info.get('addrs', '')
        query_str = request.rel_url.query
        query_from = util.parse_int(query_str['from'], 0) if 'from' in query_str else 0
        query_to = util.parse_int(query_str['to'], MAX_TX_QUERY) if 'to' in query_str else MAX_TX_QUERY
        if query_from < 0:
            return web.Response(status=400, text=f'Invalid state: "from" ({query_from}) is expected to be greater '
            f'than or equal to 0')

        if query_to < 0:
            return web.Response(status=400, text=f'Invalid state: "to" ({query_to}) is expected to be greater '
            f'than or equal to 0')

        if query_from > query_to:
            return web.Response(status=400, text=f'Invalid state: "from" ({query_from}) is '
            f'expected to be less than "to" ({query_to})')

        if not addrs:
            return web.Response(status=404)

        query_to = query_to if query_to - query_from < MAX_TX_QUERY else query_from + MAX_TX_QUERY

        list_addr = list(dict.fromkeys(addrs.split(',')))
        items = list()
        list_history = []
        for address in list_addr:
            list_history = list_history + await self.address_get_history(address)
        for i in range(len(list_history)):
            if i < query_from or i >= query_to:
                continue
            item = list_history[i]
            blockheight = item["height"]
            tx_detail = await self.transaction_get(item["tx_hash"], True)
            items.append(await self.wallet_history(blockheight, tx_detail))
        res = {"totalItems": len(list_history),
               "from": query_from,
               "to": query_to,
               "items": items}
        jsonStr = json.dumps(res, cls=DecimalEncoder)
        return web.json_response(json.loads(jsonStr))

    async def wallet_history(self, blockheight, tx_detail):
        txid = tx_detail["txid"]
        confirmations = tx_detail["confirmations"] if 'confirmations' in tx_detail else 0
        if 'time' in tx_detail:
            time = tx_detail["time"]
        else:
            # This is unconfirmed transaction, so get the time from memory pool
            # The time the transaction entered the memory pool, Unix epoch time format
            mempool = await self.mempool_get(True)
            tx = mempool.get(txid)
            if tx is not None:
                time = tx["time"] if 'time' in tx else None
            else:
                time = None
        if time is None:
            raise RPCError(BAD_REQUEST, f'cannot get the transaction\'s time')
        list_vin = tx_detail["vin"]
        list_vout = tx_detail["vout"]
        list_final_vin = [await self.vin_factory(item) for item in list_vin]
        value_in = Decimal(str(reduce(lambda prev, x: prev + x["value"], list_final_vin, 0)))
        value_out = Decimal(str(reduce(lambda prev, x: prev + x["value"], list_vout, 0)))
        value_in_places = value_in.as_tuple().exponent
        value_out_places = value_out.as_tuple().exponent
        min_places = min(value_in_places, value_out_places)
        if min_places < 0:
            pos = abs(min_places)
        else:
            pos = 0
        if value_in > 0:
            fees = round(value_in - value_out, pos)
        else:
            '''from Block Reward'''
            fees = 0

        return {"txid": txid,
                "blockheight": blockheight,
                "vin": list_final_vin,
                "vout": list_vout,
                "valueOut": value_out,
                "valueIn": value_in,
                "fees": fees,
                "confirmations": confirmations,
                "time": time}

    async def vin_factory(self, obj):
        if 'txid' in obj:
            txid = obj["txid"]
            vout = obj["vout"]
            tx_detail = await self.transaction_get(txid, True)
            list_vout = tx_detail["vout"]
            prev_vout = list_vout[vout]
            value = prev_vout["value"]
            addr = prev_vout["scriptPubKey"]["addresses"][0]
            return {
                "txid": txid,
                "addr": addr,
                "valueSat": value * self.coin.VALUE_PER_COIN,
                "value": value
            }
        else:
            '''from Block Reward'''
            obj["value"] = 0
            return obj

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

    async def address_get_history(self, address):
        '''Return the confirmed and unconfirmed history of an address.'''
        hashX = self.address_to_hashX(address)
        return await self.confirmed_and_unconfirmed_history(hashX)

    async def get_balance(self, hashX):
        utxos = await self.db.all_utxos(hashX)
        confirmed = sum(utxo.value for utxo in utxos)
        unconfirmed = await self.mempool.balance_delta(hashX)
        return {'confirmed': confirmed, 'unconfirmed': unconfirmed}

    async def unconfirmed_history(self, hashX):
        # Note unconfirmed history is unordered in electrum-server
        # height is -1 if it has unconfirmed inputs, otherwise 0
        return [{'tx_hash': hash_to_hex_str(tx.hash),
                 'height': -tx.has_unconfirmed_inputs,
                 'fee': tx.fee}
                for tx in await self.mempool.transaction_summaries(hashX)]

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

