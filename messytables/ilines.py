# Created by Scott David Daniels on Wed, 23 Jun 2004, licensed under the PSF
# http://code.activestate.com/recipes/286165-ilines-universal
#   -newlines-from-any-data-source/


def ilines(source_iterable):
    for line in _ilines(source_iterable):
        yield line.decode('utf-8')

def _ilines(source_iterable):
    '''yield lines as in universal-newlines from a stream of data blocks'''
    tail = b''
    for block in source_iterable:
        if not block:
            continue
        if len(tail) and tail[-1] == b'\015':
            yield tail[:-1] + b'\012'
            if block[0] == b'\012':
                pos = 1
            else:
                tail = b''
        else:
            pos = 0
        try:
            while True:  # While we are finding LF.
                npos = block.index(b'\012', pos) + 1
                try:
                    rend = npos - 2
                    rpos = block.index(b'\015', pos, rend)
                    if pos:
                        yield block[pos: rpos] + b'\n'
                    else:
                        yield tail + block[:rpos] + b'\n'
                    pos = rpos + 1
                    while True:  # While CRs 'inside' the LF
                        rpos = block.index(b'\015', pos, rend)
                        yield block[pos: rpos] + b'\n'
                        pos = rpos + 1
                except ValueError:
                    pass
                if b'\015' == block[rend]:
                    if pos:
                        yield block[pos: rend] + b'\n'
                    else:
                        yield tail + block[:rend] + b'\n'
                elif pos:
                    yield block[pos: npos]
                else:
                    yield tail + block[:npos]
                pos = npos
        except ValueError:
            pass
        # No LFs left in block.  Do all but final CR (in case LF)
        try:
            while True:
                rpos = block.index(b'\015', pos, -1)
                if pos:
                    yield block[pos: rpos] + b'\n'
                else:
                    yield tail + block[:rpos] + b'\n'
                pos = rpos + 1
        except ValueError:
            pass

        if pos:
            tail = block[pos:]
        else:
            tail += block
    if tail:
        yield tail
