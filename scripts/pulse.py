import base64
import hashlib
import json
from urllib import request

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def get_data(url: str="https://web.pulsepoint.org/DB/giba.php?agency_id=EMS1205") -> dict:

    data = json.loads(request.urlopen(url).read().decode())

    ct = base64.b64decode(data.get("ct"))
    iv = bytes.fromhex(data.get("iv"))
    salt = bytes.fromhex(data.get("s"))

    # Build the password
    t = ""
    # e = 'IncidentsCommon'
    e = "CommonIncidents"
    t += e[13] + e[1] + e[2] + "brady" + "5" + "r" + e.lower()[6] + e[5] + "gs"

    # Calculate a key from the password
    hasher = hashlib.md5()
    key = b""
    block = None
    while len(key) < 32:
        if block:
            hasher.update(block)
        hasher.update(t.encode())
        hasher.update(salt)
        block = hasher.digest()

        hasher = hashlib.md5()
        key += block

    # Create a cipher and decrypt the data
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
    decryptor = cipher.decryptor()
    out = decryptor.update(ct) + decryptor.finalize()

    # Clean up output data
    out = out[1 : out.rindex(b'"')].decode()  # Strip off extra bytes and wrapper quotes
    out = out.replace(r"\"", r'"')  # Un-escape quotes

    data = json.loads(out)
    # print(data)
    # active = data.get("incidents", {}).get("active", {})
    # [print("%s @ %s" % (c.get("PulsePointIncidentCallType"), c.get("FullDisplayAddress"))) for c in active]

    return data

if __name__=='main':
    results=get_data()
    with open('data.json', 'w+') as outfile:
        json.dump(results, outfile, indent=4)