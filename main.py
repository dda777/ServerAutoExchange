# python3
import asyncio
from queue import Queue
import pickle
from run_process import StartProcess



class MyAsync:
    async def run_server(self, host, port):
        self.counter = 0
        self.queue = Queue()
        server = await asyncio.start_server(self.serve_client, host, port)
        task2 = server.serve_forever()
        task = asyncio.create_task(self.put())
        await asyncio.gather(task, task2)

    async def serve_client(self, reader, writer):
        print(writer.get_extra_info('socket'))
        self.writer = writer
        self.counter += 1  # Потоко-безопасно, так все выполняется в одном потоке
        print(f'Client #{self.counter} connected')
        request = await self.read_request(reader)
        self.queue.put(request)
        if request is None:
            print(f'Client #{self.counter} unexpectedly disconnected')
        else:
            await self.write_response(writer)

    async def put(self):
        while True:
            if self.queue.empty():
                pass
            elif not self.queue.empty():
                print('ok')
                #StartProcess.start(self.queue.get())
                self.close_connect(self.writer)


    def close_connect(self, writer):
        writer.close()

    async def read_request(self, reader, delimiter=b'.'):
        request = bytearray()
        while True:
            data = await reader.read(100)
            if not data:
                break
            request += data
            if delimiter in request:
                print(pickle.loads(request))
                return pickle.loads(request)
        return None

    async def write_response(self, writer):
        response = pickle.dumps('Ок')
        writer.write(response)
        await writer.drain()
        print(f'Client #{self.counter} has been served')


if __name__ == '__main__':
    q = MyAsync()
    asyncio.run(q.run_server('10.88.2.6', 8888))
