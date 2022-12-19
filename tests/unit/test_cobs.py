
from io import BytesIO

# The following intimidating-looking functions implement Consistent Overhead
# Byte Stuffing (COBS), as described in the paper of the same name by Cheshire
# and Baker in the ACM SIGCOMM proceedings, 1997. The functions are translations
# of the reference C implementations given in the paper's appendix.
#
# By using COBS to encode each dbt event as a packet of non-zero bytes, deliminated
# by zero separators, we avoid having to implement our own packet-encoding scheme,
# and support recoverability in case of binary stream corruption.


def stuff_data(src: bytes) -> bytes:
    src_ptr = 0
    code_ptr = 0
    dst = [0]
    code = 1
    while src_ptr < len(src):
        if src[src_ptr] == 0:
            dst[code_ptr] = code
            code_ptr = len(dst)
            dst.append(0)
            code = 1
        else:
            dst.append(src[src_ptr])
            code += 1
            if code == 255:
                dst[code_ptr] = 255
                code_ptr = len(dst)
                dst.append(0)
                code = 1
        src_ptr += 1
    dst[code_ptr] = code
    return bytes(dst)


def unstuff_data(src: BytesIO) -> bytes:
    dst: list[int] = []
    src_ptr = 0
    code_read = src.read(1)
    while len(code_read) == 1 and code_read[0] != 0:
        code = code_read[0]
        src_ptr += 1
        dst += src.read(code - 1)
        if code < 255:
            dst.append(0)
        code_read = src.read(1)
    return bytes(dst[:-1])


every_byte = bytes([b for b in range(0, 256)])  # All 256 bytes in order, i.e 00 01 02 ... FF


test_cases = [
    # small cases / corner cases
    (b"", b"\x01"),
    (b"\x00", b"\x01\x01"),
    (b"ab\x00c", b"\x03ab\x02c"),
    (b"q", b"\x02q"),
    (b"qr", b"\x03qr"),
    (b"hello world", b"\x0Chello world"),
    (b"hello\x00world", b"\x06hello\x06world"),

    # Test cases longer than 254 bytes to exercise chunking
    (every_byte[1:-1], b"\xFF" + every_byte[1:-1] + b"\x01"),     # 01 02 ... FE -> FF 01 02 ... FE 01
    (every_byte[1:], b"\xFF" + every_byte[1:-1] + b"\x02\xFF"),   # 01 02 ... FF -> FF 01 02 ... FE 02 FF
    (every_byte[:-1], b"\x01\xFF" + every_byte[1:-1] + b"\x01"),  # 00 01 ... FE -> 01 FF 01 02 .. FE
    (every_byte, b"\x01\xFF" + every_byte[1:-1] + b"\x02\xFF")    # 00 01 ... FF -> 01 FF 01 02 ... FE 02 FF
]


class TestCobs:
    def test_stuff_data(self):
        for unstuffed, stuffed in test_cases:
            assert stuffed == stuff_data(unstuffed)

    def test_unstuff_data(self):
        for unstuffed, stuffed in test_cases:
            assert unstuffed == unstuff_data(BytesIO(stuffed))

    def test_packet_stream_round_trip(self):

        # pack all the test packets into a stream
        bytes = b""
        for unstuffed, _ in test_cases:
            bytes = bytes + stuff_data(unstuffed) + b"\x00"
        stream = BytesIO(bytes)

        # read the test messages back and confirm they match
        for unstuffed, _ in test_cases:
            assert unstuffed == unstuff_data(stream)
