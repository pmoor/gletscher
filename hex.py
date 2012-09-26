import binascii

def h2b(hexstr):
  return binascii.unhexlify(str.encode(hexstr))

def b2h(bytestr):
  return bytes.decode(binascii.hexlify(bytestr))