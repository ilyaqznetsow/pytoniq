import asyncio
import hashlib
import logging
import traceback
from types import coroutine
from pytoniq_core.tlb.block import ExtBlkRef
import requests
from pytoniq.liteclient import LiteClient
from pytoniq_core.tl import BlockIdExt
from pytoniq import Cell, Slice
from pytoniq import BlockIdExt
import httpx
from aiogram import Bot
import asyncio

DEX = "779dcc815138d9500e449c5291e7f12738c23d575b5310000f6a253bd607384e"
whitelist = ["EQAM2KWDp9lN0YvxvfSbI0ryjBXwM70rakpNIHbuETatRWA1"]


TOKEN = '6752508886:AAFrq6pAWgHDpOn4oygoMOGrWBrPpqkGDVA'
bot = Bot(token=TOKEN, parse_mode="HTML")
# logging.basicConfig(filename='app.log', level=logging.INFO,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def get_hash(string: str) -> int:
    return int.from_bytes(hashlib.sha256(string.encode()).digest(), 'big')

def get_str_attr_value(meta: dict, attr_name: str):
    hash_ = get_hash(attr_name)
    value: Slice = meta.get(hash_)
    if value is not None:
        return value.load_snake_string().replace('\x00', '')
    
def get_bytes_attr_value(meta: dict, attr_name: str) -> bytes:
    hash_ = get_hash(attr_name)
    value: Slice = meta.get(hash_)
    if value is not None:
        return value.load_snake_bytes()

def process_metadata(cell: Cell):
    cs = cell.begin_parse()
    if not len(cell.refs):  # some metadata cells do not have b'\x01' prefix
        url = cs.load_snake_string().replace('\x01', '')
        return url
    else:
        cs.load_uint(8)
        metadata = cs.load_dict(key_length=256)
        if metadata is None:
            return {}

        result = {
            'name': get_str_attr_value(metadata, 'name'),
            'description': get_str_attr_value(metadata, 'description'),
            'image': get_str_attr_value(metadata, 'image'),
            'image_data': get_bytes_attr_value(metadata, 'image_data'),
            'symbol': get_str_attr_value(metadata, 'symbol'),
            'decimals': get_str_attr_value(metadata, 'decimals'),
            'uri': get_str_attr_value(metadata, 'uri')
        }
        
        try:
            if result['decimals']:
                result['decimals'] = int(result['decimals'])
        except Exception as e:
            print(f'Process_metadata error: {e}')

        return result

async def get_pool_address(client: LiteClient, dex_address, token0, token1):
    stack = await client.run_get_method(address=dex_address, method="get_pool_address", stack=[token0,token1])
    return stack[2].load_address().to_str(1, 1, 1)

async def is_in_whitelist(client: LiteClient, jetton_wallet):
    try:
        stack = await client.run_get_method(address=jetton_wallet, method="get_wallet_data", stack=[])
        if len(stack) < 3:
            return False
        jetton_address = stack[2].load_address().to_str(1, 1, 1)
        return jetton_address in whitelist
    except Exception as e:
        print(f'Is_in_whitelist error: {e}')
        return False

async def get_jetton_content_by_jetton_wallet(client:LiteClient, jetton_wallet):
    result = {}

    stack = await client.run_get_method(address=jetton_wallet, method="get_wallet_data", stack=[])
    result["jetton_address"] = stack[2].load_address().to_str(1, 1, 1)
    
    stack = await client.run_get_method(address=result["jetton_address"], method="get_jetton_data", stack=[])
    metadata = process_metadata(stack[3])
    if isinstance(metadata, str):
        async with httpx.AsyncClient() as async_client:
            if metadata[0:4] == "ipfs":
                r = await async_client.get(f"https://ipfs.io/ipfs/{metadata.replace('ipfs://', '')}")
            else:
                r = await async_client.get(metadata)
            r = r.json()
        result["decimals"] = r.get("decimals", 9)
        result["symbol"] = r.get("symbol", "None")
    elif not metadata["symbol"]:
        async with httpx.AsyncClient() as async_client:
            r = await async_client.get(metadata["uri"])
            r = r.json()
        result["decimals"] = r.get("decimals", 9)
        result["symbol"] = r.get("symbol", "None")

    else:
        result["decimals"] = metadata["decimals"]
        result["symbol"] = metadata["symbol"]

    if result["symbol"].upper() in ["JUSDT", "JUSDC"]:
        result["decimals"] = 6

    return result

class BlockScanner:

    def __init__(self,
                 client: LiteClient,
                 block_handler: coroutine
                 ):
        """
        :param client: LiteClient
        :param block_handler: function to be called on new block
        """
        self.client = client
        self.block_handler = block_handler
        self.shards_storage = {}
        self.blks_queue = asyncio.Queue()

    async def run(self, mc_seqno: int = None):
        if not self.client.inited:
            raise Exception('should init client first')

        if mc_seqno is None:
            master_blk: BlockIdExt = self.mc_info_to_tl_blk(await self.client.get_masterchain_info())
        else:
            master_blk, _ = await self.client.lookup_block(wc=-1, shard=-9223372036854775808, seqno=mc_seqno)

        master_blk_prev, _ = await self.client.lookup_block(wc=-1, shard=-9223372036854775808, seqno=master_blk.seqno - 1)

        shards_prev = await self.client.get_all_shards_info(master_blk_prev)
        for shard in shards_prev:
            self.shards_storage[self.get_shard_id(shard)] = shard.seqno

        while True:
            await self.blks_queue.put(master_blk)

            shards = await self.client.get_all_shards_info(master_blk)
            for shard in shards:
                await self.get_not_seen_shards(shard)
                self.shards_storage[self.get_shard_id(shard)] = shard.seqno

            while not self.blks_queue.empty():
                await self.block_handler(self.client, self.blks_queue.get_nowait())

            while True:
                # if not hasattr(self.client, "last_mc_block"):
                #     break
                if master_blk.seqno + 1 == self.client.last_mc_block.seqno:
                    master_blk = self.client.last_mc_block
                    break
                elif master_blk.seqno + 1 < self.client.last_mc_block.seqno:
                    master_blk, _ = await self.client.lookup_block(wc=-1, shard=-9223372036854775808, seqno=master_blk.seqno + 1)
                    break
                await asyncio.sleep(0.1)

    async def get_not_seen_shards(self, shard: BlockIdExt):
        if self.shards_storage.get(self.get_shard_id(shard)) == shard.seqno:
            return

        full_blk = await self.client.raw_get_block_header(shard)
        prev_ref = full_blk.info.prev_ref
        if prev_ref.type_ == 'prev_blk_info':  # only one prev block
            prev: ExtBlkRef = prev_ref.prev
            prev_shard = self.get_parent_shard(shard.shard) if full_blk.info.after_split else shard.shard
            await self.get_not_seen_shards(BlockIdExt(
                    workchain=shard.workchain, seqno=prev.seqno, shard=prev_shard,
                    root_hash=prev.root_hash, file_hash=prev.file_hash
                )
            )
        else:
            prev1: ExtBlkRef = prev_ref.prev1
            prev2: ExtBlkRef = prev_ref.prev2
            await self.get_not_seen_shards(BlockIdExt(
                    workchain=shard.workchain, seqno=prev1.seqno, shard=self.get_child_shard(shard.shard, left=True),
                    root_hash=prev1.root_hash, file_hash=prev1.file_hash
                )
            )
            await self.get_not_seen_shards(BlockIdExt(
                    workchain=shard.workchain, seqno=prev2.seqno, shard=self.get_child_shard(shard.shard, left=False),
                    root_hash=prev2.root_hash, file_hash=prev2.file_hash
                )
            )

        await self.blks_queue.put(shard)

    def get_child_shard(self, shard: int, left: bool) -> int:
        x = self.lower_bit64(shard) >> 1
        if left:
            return self.simulate_overflow(shard - x)
        return self.simulate_overflow(shard + x)

    def get_parent_shard(self, shard: int) -> int:
        x = self.lower_bit64(shard)
        return self.simulate_overflow((shard - x) | (x << 1))

    @staticmethod
    def simulate_overflow(x) -> int:
        return (x + 2**63) % 2**64 - 2**63

    @staticmethod
    def lower_bit64(num: int) -> int:
        return num & (~num + 1)

    @staticmethod
    def mc_info_to_tl_blk(info: dict):
        return BlockIdExt.from_dict(info['last'])

    @staticmethod
    def get_shard_id(blk: BlockIdExt):
        return f'{blk.workchain}:{blk.shard}'

def short_address(address: str):
    return f"{address[:3]}..{address[-3:]}"

async def handle_block(client:LiteClient, block: BlockIdExt):
    if block.workchain == -1:  # skip masterchain blocks
        return
    transactions = await client.raw_get_block_transactions_ext(block)

    for transaction in transactions:
        if not transaction.in_msg.is_internal:
            continue
        if len(transaction.in_msg.body.bits) < 32:
            continue
        if transaction.account_addr_hex == DEX:
            is_liquidity = False
            body_slice = transaction.in_msg.body.begin_parse()
            op_code = body_slice.load_uint(32)
            # from user to router. transfer
            if op_code == 0x7362d09c:
                query_id = body_slice.load_uint(64)
                jetton_amount = body_slice.load_coins()
                from_user = body_slice.load_address().to_str()
                ref_msg_data = body_slice.load_ref().begin_parse()
                transferred_op = ref_msg_data.load_uint(32)
                pool_address = ref_msg_data.load_address().to_str()
                if transferred_op == 0x25938561:
                    t = 0
                    #swap
                    # min_out = ref_msg_data.load_coins()
                    # to_address = ref_msg_data.load_address().to_str()
                    # has_ref = ref_msg_data.load_uint(1)
                elif transferred_op == 0xfcf9e58f:
                    #liquidity
                    continue
                    min_lp_out = ref_msg_data.load_coins()
                    is_liquidity = True
                else:
                    #unknown
                    continue

                jetton_data_in = await get_jetton_content_by_jetton_wallet(client, pool_address)

                if transaction.out_msgs:
                    out_body = transaction.out_msgs[0].body.begin_parse()
                    out_op_code = out_body.load_uint(32)
                    out_j = 0
                    if out_op_code == 0x25938561:
                        out_query_id = out_body.load_uint(64)
                        out_to_address = out_body.load_address().to_str()
                        out_sender_address = out_body.load_address().to_str()
                        out_jetton_amount = out_body.load_coins()
                        out_j = out_body.load_coins()
                    # elif out_op_code == 0xfcf9e58f:
                    #     #liquidity
                    #     out_j = ref_msg_data.load_coins()
                    #     is_liquidity = True
                    else:
                        continue

                    jetton_data_out = await get_jetton_content_by_jetton_wallet(client, out_sender_address)

                    if not await is_in_whitelist(client, out_sender_address) and not await is_in_whitelist(client, pool_address):
                        continue
                    # is_jarbuz = jetton_data_out['symbol'] == "jARBUZ" or jetton_data_in['symbol'] == "jARBUZ"
                    
                    decimals_in = 18 if jetton_data_in['symbol'] == "jARBUZ" else jetton_data_in['decimals']
                    decimals_out = 18 if jetton_data_out['symbol'] == "jARBUZ" else  jetton_data_out['decimals']
                    in_amount = jetton_amount / (10 ** int(decimals_out))
                    out_amount = out_j / (10 ** int(decimals_in))

                    short = short_address(from_user)
                    text = ""
                    if is_liquidity:
                        text = f"\n+{in_amount} #{jetton_data_out['symbol']}\n+{out_amount} #{jetton_data_in['symbol']}"
                    else:
                        is_sell = jetton_data_out['symbol'] == "ARBUZ"
                        emoji_in = "🍉" if jetton_data_out['symbol'] == "ARBUZ" else "💎" if jetton_data_out['symbol'] == "pTON" else f"${jetton_data_out['symbol']}"
                        emoji_out = "🍉" if jetton_data_in['symbol'] == "ARBUZ" else "💎"if jetton_data_in['symbol'] == "pTON" else f"${jetton_data_in['symbol']}"
                        emoji_type = "💔" if is_sell else "💚"
                        text = f"{in_amount:,.2f} {emoji_in} ➡️ {out_amount:,.2f} {emoji_out}"
                    message_with_link = f"{text}\n{emoji_type} [{short}](https://tonviewer.com/{from_user})"
                    await bot.send_message(-1001998422328,message_with_link, disable_web_page_preview=True, parse_mode="Markdown")
        # else:
        #     body_slice = transaction.in_msg.body.begin_parse()
        #     op_code = body_slice.load_uint(32)
        #     text = ""
        #     user = ""
        #     if len(transaction.out_msgs) > 0:
        #         out_body = transaction.out_msgs[0].body.begin_parse()
        #         out_op_code = out_body.load_uint(32)
        #         if out_op_code == 0x178d4519:
        #             out_query_id = out_body.load_uint(64)
        #             out_amount = out_body.load_coins()

        #             user = out_body.load_address().to_str()
            
        #             short = short_address(user)
        #             text = f"{short}"

        #     if op_code == 0x0f8a7ea5:
        #         is_whitelisted = await is_in_whitelist(client, Address(f"0:{transaction.account_addr_hex}") )
        #         if not is_whitelisted:
        #             continue
        #         query_id = body_slice.load_uint(64)
        #         amount = body_slice.load_coins()
        #         destination = body_slice.load_address().to_str()
                
                # dest = short_address(destination)
                # amount_dfc = amount / (10 ** 9)
                # text = f"{amount_dfc} DFC ➡️ {dest}"
        
                # message_with_link = f"{text}\n[{short}](https://tonviewer.com/{user})"
                # await bot.send_message(-1001998422328,message_with_link, disable_web_page_preview=True, parse_mode="Markdown")
               
async def main():
    client = None
    while True:
        try:
            
            config = requests.get('https://tondiamonds.fra1.digitaloceanspaces.com/config-tonvision-liteserver.json').json()

            client = LiteClient.from_config(config, 2, 20)
            await client.connect()
            scanner = BlockScanner(client=client, block_handler=handle_block)
            await scanner.run()
        except Exception as exc:
            print(f"Attempt failed with error: {str(exc)}")
            # my_traceback = "".join(traceback.format_exception(etype=None, value=exc, tb=exc.__traceback__))
            # print(my_traceback)
            try:
                await client.close()
            except:
                pass
        finally:
            await asyncio.sleep(3)  # Wait for 10 seconds before retrying

if __name__ == '__main__':
    # Setup logging
    # logging.basicConfig(level=logging.INFO)
    asyncio.run(main())