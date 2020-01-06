import asyncio
import os
import sys
import aiohttp
from aiohttp import web
import yaml
from urllib.parse import urljoin


def read_file(path):
    with open(path) as fin:
        return fin.read()


def write_file(path, text):
    with open(path, 'w') as fout:
        fout.write(text)


async def fetch_one(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={'req_from_node': 'True'}) as resp:
            print(resp.status)
            return await resp.text()


async def handle(request):
    err_msg = "File does not exist"
    file_name = request.match_info.get('file_name')
    p = os.path.join(CONFIG['dir_path'], file_name)

    if os.path.exists(p):
        # read file from local dir
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(None, read_file, p)
        text = await future
    else:
        text = err_msg
        if not request.query.get('req_from_node', False):
            # request file from other nodes
            coro_list = [fetch_one(urljoin(url, file_name))
                         for url in CONFIG['node_urls']]
            wait_coro = asyncio.wait(coro_list)
            res, _ = await wait_coro

            for task in res:
                if task.result() != err_msg:
                    text = task.result()
                    # save file from other node
                    loop = asyncio.get_running_loop()
                    future = loop.run_in_executor(None, write_file, p, text)
                    await future
                    break

    return web.Response(text=text)


if __name__ == '__main__':
    # read config
    conf_file = sys.argv[1]
    with open(conf_file) as fin:
        CONFIG = yaml.safe_load(fin)
    # start server
    app = web.Application()
    app.add_routes([
        web.get('/{file_name}', handle)])
    web.run_app(app, port=CONFIG['port'], host=CONFIG['host'])
