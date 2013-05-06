from messytables.util import OrderedDict
import cStringIO


def seekable_stream(fileobj):
    try:
        fileobj.seek(0)
        # if we got here, the stream is seekable
    except:
        # otherwise seek failed, so slurp in stream and wrap
        # it in a BytesIO
        fileobj = BufferedFile(fileobj)
    return fileobj


class BufferedFile(object):
    ''' A buffered file that preserves the beginning of a stream up to buffer_size
    '''
    def __init__(self, fp, buffer_size=2048):
        self.data = cStringIO.StringIO()
        self.fp = fp
        self.offset = 0
        self.len = 0
        self.fp_offset = 0
        self.buffer_size = buffer_size

    def _next_line(self):
        try:
            return self.fp.readline()
        except AttributeError:
            return self.fp.next()

    def _read(self, n):
        return self.fp.read(n)

    @property
    def _buffer_full(self):
        return self.len >= self.buffer_size

    def readline(self):
        if self.len < self.offset < self.fp_offset:
            raise BufferError('Line is not available anymore')
        if self.offset >= self.len:
            line = self._next_line()
            self.fp_offset += len(line)

            self.offset += len(line)

            if not self._buffer_full:
                self.data.write(line)
                self.len += len(line)
        else:
            line = self.data.readline()
            self.offset += len(line)
        return line

    def read(self, n=-1):
        if n == -1:
            # if the request is to do a complete read, then do a complete
            # read.
            self.data.seek(self.offset)
            return self.data.read(-1) + self.fp.read(-1)

        if self.len < self.offset < self.fp_offset:
            raise BufferError('Data is not available anymore')
        if self.offset >= self.len:
            byte = self._read(n)
            self.fp_offset += len(byte)

            self.offset += len(byte)

            if not self._buffer_full:
                self.data.write(byte)
                self.len += len(byte)
        else:
            byte = self.data.read(n)
            self.offset += len(byte)
        return byte

    def tell(self):
        return self.offset

    def seek(self, offset):
        if self.len < offset < self.fp_offset:
            raise BufferError('Cannot seek because data is not buffered here')
        self.offset = offset
        if offset < self.len:
            self.data.seek(offset)


class Cell(object):
    """ A cell is the basic value type. It always has a ``value`` (that
    may be ``None`` and may optionally also have a type and column name
    associated with it. If no ``type`` is set, the String type is set
    but no type conversion is set. """

    def __init__(self, value, column=None, type=None):
        if type is None:
            from messytables.types import StringType
            type = StringType()
        self.value = value
        self.column = column
        self.column_autogenerated = False
        self.type = type

    def __repr__(self):
        if self.column is not None:
            return "<Cell(%s=%s:%s>" % (self.column,
                    self.type, self.value)
        return "<Cell(%r:%s>" % (self.type, self.value)

    @property
    def empty(self):
        """ Stringify the value and check that it has a length. """
        if self.value is None:
            return True
        value = self.value
        if not isinstance(value, basestring):
            value = unicode(value)
        if len(value.strip()):
            return False
        return True


class TableSet(object):
    """ A table set is used for data formats in which multiple tabular
    objects are bundeled. This might include relational databases and
    workbooks used in spreadsheet software (Excel, LibreOffice).

    The primary way to instantiate is through a file object passed to
    the constructor pointer. This means you can stream a table set
    directly off a web site or some similar source.
    """

    @property
    def tables(self):
        """ Return a listing of tables in the ``TableSet``. Each table
        has a name. """
        pass

    @classmethod
    def from_fileobj(cls, fileobj, *args, **kwargs):
        """ Deprecated, only for compatibility reasons """
        return cls(fileobj, *args, **kwargs)


class RowSet(object):
    """ A row set (aka: table) is a simple wrapper for an iterator of
    rows (which in turn is a list of ``Cell`` objects). The main table
    iterable can only be traversed once, so on order to allow analytics
    like type and header guessing on the data, a sample of ``window``
    rows is read, cached, and made available. """

    def __init__(self, typed=False):
        self.typed = typed
        self._processors = []
        self._types = None

    def set_types(self, types):
        self.typed = True
        self._types = types

    def get_types(self):
        return self._types

    types = property(get_types, set_types)

    def register_processor(self, processor):
        """ Register a stream processor to be used on each row. A
        processor is a function called with the ``RowSet`` as its
        first argument and the row to be processed as the second
        argument. """
        self._processors.append(processor)

    def __iter__(self, sample=False):
        """ Apply processors to the row data. """
        for row in self.raw(sample=sample):
            for processor in self._processors:
                row = processor(self, row)
                if row is None:
                    break
            if row is not None:
                yield row

        # this is a bit dirty but required for the offset processor:
        self._offset = 0

    @property
    def sample(self):
        return self.__iter__(sample=True)

    def dicts(self, sample=False):
        """ Return a representation of the data as an iterator of
        ordered dictionaries. This is less specific than the cell
        format returned by the generic iterator but only gives a
        subset of the information. """
        generator = self.sample if sample else self
        for row in generator:
            yield OrderedDict([(c.column, c.value) for c in row])

    def __repr__(self):
        return "RowSet(%s)" % self.name
