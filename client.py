import ctypes
import os
from subprocess import check_output


print(ctypes.windll.kernel32.GetOEMCP())
script = r'Openfiles /Query /s SRVOCEAN /fo csv  | find /i "db.adm"'
encoding = os.device_encoding(0)
text = check_output(script, encoding='866', shell=True)
text = text.split('\n')
data = []
q = []
for i in text:
    data += i.split(',')
for j in range(0, len(data), 4):
    q.append(data[j].replace('"', ''))
q.pop()
for k in q:
    close = f'Openfiles /Disconnect /s SRVOCEAN /ID {k}'
    text = check_output(close, encoding='866', shell=True)

print(q)