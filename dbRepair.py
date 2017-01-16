# -*- coding: utf-8 -*-
#!/usr/bin/python

import MySQLdb.cursors
import struct
import sys
import getopt
import time
import signal

is_sigint_up = False

def getConn(host, user, passwd, port, db):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd,
                           port=port, db=db,
                           cursorclass=MySQLdb.cursors.DictCursor)
        return conn
    except MySQLdb.Error, e:
        print inred("ERROR: %d: %s" % (e.args[0], e.args[1]))
        return False


class EventType :
    UNKNOWN_EVENT= 0
    START_EVENT_V3= 1
    QUERY_EVENT= 2
    STOP_EVENT= 3
    ROTATE_EVENT= 4
    INTVAR_EVENT= 5
    LOAD_EVENT= 6
    SLAVE_EVENT= 7
    CREATE_FILE_EVENT= 8
    APPEND_BLOCK_EVENT= 9
    EXEC_LOAD_EVENT= 10
    DELETE_FILE_EVENT= 11
    NEW_LOAD_EVENT= 12
    RAND_EVENT= 13
    USER_VAR_EVENT= 14
    FORMAT_DESCRIPTION_EVENT= 15
    XID_EVENT= 16
    BEGIN_LOAD_QUERY_EVENT= 17
    EXECUTE_LOAD_QUERY_EVENT= 18
    TABLE_MAP_EVENT = 19
    PRE_GA_WRITE_ROWS_EVENT = 20
    PRE_GA_UPDATE_ROWS_EVENT = 21
    PRE_GA_DELETE_ROWS_EVENT = 22
    WRITE_ROWS_EVENT = 23
    UPDATE_ROWS_EVENT = 24
    DELETE_ROWS_EVENT = 25
    #mysql5.6.2 events
    # WRITE_ROWS_EVENT= 30
    # UPDATE_ROWS_EVENT= 31
    # DELETE_ROWS_EVENT= 32
    # INCIDENT_EVENT= 26
    # HEARTBEAT_LOG_EVENT= 27

class EVENT_LEN:
    EVENT_HEADER_LEN = 19
    TABLE_ID = 6
    BINLOG_HEADER_LEN = 4
    BINLOG_HEADER = '\xfe\x62\x69\x6e'

class NUMBER_TYPE:
    UNSIGNED = 0
    SIGNED = 1
    INTEGER = 0
    FRACTIONAL = 1
    ERROR = False

def check_signed(a, b):
    if a < 0:
        return a
    else : return b

class BinFileReader(object):

    def __init__(self, bin_filename):
        self.stream = open(bin_filename, 'rb')

    def int8(self, type=NUMBER_TYPE.SIGNED):
        buf = self.stream.read(1)
        if buf is None or buf == '':
            raise Exception('End of file.')
        if type == NUMBER_TYPE.UNSIGNED:
            return struct.unpack('<B', buf)[0]
        elif type == NUMBER_TYPE.SIGNED:
            return check_signed(struct.unpack('<b', buf)[0], struct.unpack('<B', buf)[0])
        else:
            return NUMBER_TYPE.ERROR

    def int16(self, type=NUMBER_TYPE.SIGNED):
        buf = self.stream.read(2)
        if buf is None or buf == '':
            raise Exception('End of file.')
        if type == NUMBER_TYPE.UNSIGNED:
            return struct.unpack('H', buf)[0]
        elif type == NUMBER_TYPE.SIGNED:
            return check_signed(struct.unpack('<h', buf)[0], struct.unpack('<H', buf)[0])
        else:
            return NUMBER_TYPE.ERROR


    def uint24(self):
        buf = self.stream.read(3)
        if buf is None or buf == '':
            raise Exception('End of file.')
        a = struct.unpack('<H', buf[0:2])[0]
        b = struct.unpack('<B', buf[2:3])[0] << 16
        return a+b

    def int32(self, type=NUMBER_TYPE.SIGNED) :
        buf = self.stream.read(4)
        if buf is None or buf == '':
            raise Exception('End of file.')
        if type == NUMBER_TYPE.UNSIGNED:
            return struct.unpack('I' , buf)[0]
        elif type == NUMBER_TYPE.SIGNED:
            return check_signed(struct.unpack('<i', buf)[0], struct.unpack('<I', buf)[0])
        else:
            return NUMBER_TYPE.ERROR

    def int64_float(self):
        buf = self.stream.read(4)
        if buf is None or buf == '':
            raise Exception('End of file.')
        return struct.unpack('<f' , buf)[0]

    def uint48(self):
        buf = self.stream.read(6)
        if buf is None or buf == '':
            raise Exception('End of file.')
        a=struct.unpack('<I', buf[0:4])[0]
        b=struct.unpack('<H', buf[4:6])[0]
        return a+b

    def int64(self, type=NUMBER_TYPE.SIGNED):
        buf = self.stream.read(8)
        if buf is None or buf == '':
            raise Exception('End of file.')
        if type == NUMBER_TYPE.UNSIGNED:
            return struct.unpack('<Q' , buf)[0]
        elif type == NUMBER_TYPE.SIGNED:
            return check_signed(struct.unpack('<q',buf)[0], struct.unpack('<Q',buf)[0])
        else:
            return NUMBER_TYPE.ERROR

    def int64_double(self):
        buf = self.stream.read(8)
        if buf is None or buf == '':
            raise Exception('End of file.')
        return struct.unpack('<d' , buf)[0]
        # if type == NUMBER_TYPE.UNSIGNED:
        #     return struct.unpack('<Q' , buf)[0]
        # elif type == NUMBER_TYPE.SIGNED:
        #     return check_signed(struct.unpack('<q',buf)[0], struct.unpack('<Q',buf)[0])
        # else:
        #     return NUMBER_TYPE.ERROR

    def char(self):
        buf = self.stream.read(1)
        if buf is None or buf == '':
            raise Exception('End of file.')
        return struct.unpack('c', buf)

    def chars(self, byte_size=1):
        buf = self.stream.read(byte_size)
        if buf is None or buf == '':
            raise Exception('End of file.')
        return buf

    def close(self):
        self.stream.close()

    def seek(self, p, seek_type=0):
        self.stream.seek(p, seek_type)

class EventHeader:

    def __init__(self, reader):
        self.timestamp = reader.int32(NUMBER_TYPE.UNSIGNED)
        self.type_code = reader.int8(NUMBER_TYPE.UNSIGNED)
        self.server_id = reader.int32(NUMBER_TYPE.UNSIGNED)
        self.event_length = reader.int32(NUMBER_TYPE.UNSIGNED)
        self.next_position = reader.int32(NUMBER_TYPE.UNSIGNED)
        self.flags = reader.int16(NUMBER_TYPE.UNSIGNED)

    def print_header(self):
        msg = ('EventHeader: Timestamp=%s type_code=%s server_id=%s '
               'event_length=%s next_position=%s flags=%s' %
              (self.timestamp, self.type_code, self.server_id,
               self.event_length, self.next_position, self.flags))
        print msg

def get_bitmap_new(bit_map):
    """
       get table column bit map.
       1=NULL
       0=NOT NULL
    """
    global reader
    format = '<' + str(bit_map) + 'B'
    bit_map_list = struct.unpack(format,reader.stream.read(bit_map))
    num = 0
    size = 0
    while (num < bit_map):
        size = size + (bit_map_list[num] << 8*num)
        num = num + 1
    bp = list(str(bin(size))[2:])
    while len(bp) < bit_map*8:
        bp.insert(0, '0')
    return bp


def get_byte_by_length(length):
    """
        get varchar/text/blog column size
    """
    k=0
    while (length /256) != 0:
        k = k+1
        length = length/256
    return k+1


def column_info (host, user, passowrd, port, dbname, schema_name, table_name):
    """
    get column type
    """
    sql = ("select * from information_schema.columns where table_schema='%s'"
           " and table_name='%s'")
    sql = sql % (schema_name, table_name)
    conn = getConn(host, user, passowrd, port, dbname)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    return results

def get_time_format(timevalue):
    """
    change dataformat . example : 20140521230431 == 2014-05-21 23-04-31
    """
    datetime=str(timevalue)
    year=datetime[0:4]
    month=datetime[4:6]
    day=datetime[6:8]
    hour=datetime[8:10]
    minute=datetime[10:12]
    second=datetime[12:14]
    return "'"+year+'-'+month+'-'+day+' '+hour+'-'+minute+'-'+second+"'"

def get_date_format(bitmap):
    year = int(''.join(str(v) for v in bitmap[0:15]), 2)
    month = int(''.join(str(v) for v in bitmap[15:19]), 2)
    day = int(''.join(str(v) for v in bitmap[19:24]), 2)
    return "'"+str(year)+'-'+str(month)+'-'+str(day)+"'"


def check_primary_key(results):
    """
    check primary key
    """
    for result in results:
        if result['COLUMN_KEY'] == 'PRI':
            return True
    return False

def convert(number):
    if number == '1':
        return '0'
    elif number == '0':
        return '1'

def decimal2int(bytes, type):
    byte_list = []
    while bytes !=0 :
        byte_list = byte_list + get_bitmap_new(1)
        bytes = bytes - 1
    if type == NUMBER_TYPE.INTEGER:
        if byte_list[0] == '1':
            # positive number
            return str(int(''.join(str(v) for v in byte_list[1:]), 2))
        elif byte_list[0] == '0':
            # negative number
            return '-'+str(int(''.join(str(convert(v)) for v in byte_list[1:]), 2))
    elif type == NUMBER_TYPE.FRACTIONAL:
        return str(int(''.join(str(v) for v in byte_list), 2))


def get_decimal(a):
    b=0
    if a == 0 :
        return 0
    while a > 0:
        if a > 9 :
            a = a -9
            b = b +4
        elif a <= 9 and a >= 7 :
            a = a - 9
            b = b +4
            continue
        elif a <= 6 and a >=5 :
            a = a - 6
            b= b +3
            continue
        elif a <= 4 and a >=3:
            a = a - 4
            b = b + 2
            continue
        elif a <= 2 and a >=1:
            a = a - 2
            b = b + 1
            continue
    return b

def check_mysqlversion(host, user, password, port, dbname):
    sql="show global variables where Variable_name  in ('version')"
    conn=getConn(host, user, password, port, dbname)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchone()
    conn.close()
    if '5.6' in results.get('Value'):
        return False
    else:
        return True

def execute_command(sql, host, user, password, port, dbname):
    conn = getConn(host, user, password, port, dbname)
    cursor = conn.cursor()
    cursor.execute("set session sql_log_bin=0 ")
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def parse_sql(column_info, type_code):
    from datetime import datetime
    global reader
    column_type = column_info.get('DATA_TYPE')
    column_length = column_info.get('CHARACTER_OCTET_LENGTH')
    is_primary = column_info.get('COLUMN_KEY') == 'PRI'
    column_name = column_info.get('COLUMN_NAME')
    NUMERIC_PRECISION = column_info.get('NUMERIC_PRECISION')
    NUMERIC_SCALE = column_info.get('NUMERIC_SCALE')
    sql = ''
    number_bytes = 0
    sql_list = []
    if column_type == 'bigint':
        number_bytes = int(number_bytes)+8
        value=reader.int64()
    elif column_type == 'int':
        number_bytes = int(number_bytes)+4
        value=reader.int32()
    elif (column_type == 'varchar' or column_type == 'text'
          or column_type == 'longtext' or column_type == 'mediumtext'
          or column_type == 'mediumblob' or column_type == 'blob'):
        size = 0
        lens = get_byte_by_length(column_length)
        if lens == 1:
            size = reader.int8(NUMBER_TYPE.UNSIGNED)
        elif lens == 2:
            size = reader.int16(NUMBER_TYPE.UNSIGNED)
        elif lens == 3:
            size = reader.uint24()
        elif lens == 4:
            size = reader.int32(NUMBER_TYPE.UNSIGNED)
        if size == 0:
            value = "'"+"'"
        else:
            value = "x'"+reader.chars(size).encode('hex') + "'"
            #value = "'"+reader.chars(size)+"'"
        number_bytes = int(number_bytes)+size+lens

    elif column_type == 'float':
        value = reader.int64_float()
        number_bytes = int(number_bytes)+4
    elif column_type == 'double':
        value = reader.int64_double()
        number_bytes = int(number_bytes)+8
    elif column_type == 'decimal':
        integer=get_decimal(int(NUMERIC_PRECISION) - int(NUMERIC_SCALE) )
        fractional = get_decimal(int(NUMERIC_SCALE))
        value = decimal2int(integer, NUMBER_TYPE.INTEGER) + '.' + decimal2int(fractional, NUMBER_TYPE.FRACTIONAL)
        number_bytes = int(number_bytes)+integer + fractional
    elif column_type == 'datetime':
        datetime = reader.int64(NUMBER_TYPE.UNSIGNED)
        value = get_time_format(datetime)
        number_bytes = int(number_bytes)+8
    elif column_type == 'date':
        value=get_date_format(get_bitmap_new(3))
        number_bytes = int(number_bytes)+3
    elif column_type == 'timestamp':
        timestamp = reader.int32(NUMBER_TYPE.UNSIGNED)
        value = "'" + str(datetime.fromtimestamp(timestamp)) + "'"
        number_bytes = int(number_bytes)+4
    elif column_type == 'smallint':
        value=reader.int16()
        number_bytes = int(number_bytes)+2
    elif column_type == 'tinyint':
        value = reader.int8()
        number_bytes = int(number_bytes)+1
    else:
        print inred("UNKOWN COLUMN TYPE , COLUMN TYPE : %s " % column_type)
        sys.exit(1)

    if type_code == EventType.DELETE_ROWS_EVENT:
        if is_primary:
            sql = ' %s = %s' % (column_name ,str(value))+' AND'
        else:
            sql = ''
    elif type_code == EventType.WRITE_ROWS_EVENT:
        sql = str(value)+','
    sql_list.append(sql)
    sql_list.append(number_bytes)
    return sql_list

def unpack_record (schema_name, table_name, column, bit_map, type_code, column_info):
    #print bit_map
    _sql_list = []
    number_count = 0
    results = column_info
    k=0
    sizex = len(bit_map)-1
    if type_code == EventType.DELETE_ROWS_EVENT and check_primary_key(results) :
        sql_command = "DELETE FROM %s.%s WHERE" % (schema_name,table_name)
    elif type_code == EventType.WRITE_ROWS_EVENT:
        sql_command = "REPLACE INTO %s.%s SELECT " % (schema_name,table_name)
    else:
        print inred("THIS IS A DELETE COMMAND , BUT CANT'T FIND PRIMARY KEY, EXIT...")
        sys.exit()

    while k < column:
        column_type = results[k].get('DATA_TYPE')
        is_primary = results[k].get('COLUMN_KEY') == 'PRI'
        if int(bit_map[sizex-k]) == 0:

            if column_type == 'bigint':
                sql_list = parse_sql(results[k],type_code)
                sql_command = sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type == 'int':
                sql_list = parse_sql(results[k],type_code)
                sql_command = sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type == 'varchar' or column_type == 'text' or column_type == 'longtext' \
                    or column_type == 'mediumtext' or column_type == 'mediumblob' or column_type == 'blob'\
                    or column_type == 'longblob' :
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='float':
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='double':
                sql_list = parse_sql(results[k],type_code)
                sql_command = sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='decimal':
                sql_list = parse_sql(results[k],type_code)
                sql_command = sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='date':
                sql_list = parse_sql(results[k],type_code)
                sql_command = sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='datetime':
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count = number_count+int(sql_list[1])
            elif column_type =='timestamp':
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count=number_count+int(sql_list[1])
            elif column_type =='smallint':
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count=number_count+int(sql_list[1])
            elif column_type =='tinyint':
                sql_list = parse_sql(results[k],type_code)
                sql_command=sql_command+sql_list[0]
                number_count=number_count+int(sql_list[1])
            else:
                print inred("ERROR: UNKOWN COLUMN TYPE, COLUMN TYPE : %s" % column_type)
                sys.exit()
        elif int(bit_map[sizex-k]) ==1 and is_primary==0 :
            if type_code==EventType.DELETE_ROWS_EVENT and is_primary==0:
                k=k+1
                continue
            else:
                sql_command=sql_command+"NULL"+','
        else :
            return False
        k = k+1
    if type_code==EventType.DELETE_ROWS_EVENT:
        sql_command=sql_command[:-3]
    elif type_code==EventType.WRITE_ROWS_EVENT:
        sql_command=sql_command[:-1]
    _sql_list.append(sql_command)
    _sql_list.append(number_count)
    return _sql_list


def hanle_error(binlogfile, start_position, stop_position, host, user, password, port, dbname):
    global reader
    reader = BinFileReader(binlogfile)

    if check_mysqlversion(host,user,password,port,dbname):
        pass
    else:
        print inred("[WARNINT] MYSQL VERSION IS BIGGER THAN MYSQL 5.6.2, NOT TESTED ....")
        EventType.WRITE_ROWS_EVENT = 30
        EventType.UPDATE_ROWS_EVENT = 31
        EventType.DELETE_ROWS_EVENT = 32


    binlog_header = reader.chars(EVENT_LEN.BINLOG_HEADER_LEN)
    if binlog_header != EVENT_LEN.BINLOG_HEADER:
        print inred("THIS IS NOT A BINLOG FILE, EXIT...")
        sys.exit()

    start_pos = start_position
    stop_pos = stop_position
    reader.seek(start_pos-EVENT_LEN.BINLOG_HEADER_LEN, 1)

    schema_name = ''
    table_name = ''
    table_id = 0
    while start_pos < stop_pos:

        header = EventHeader(reader)
        if header.event_length == 0:
            print inred("EVENT PARASE ERROR , EVENT LENGTH IS NULL , POSITION : %s" % (start_position))
            sys.exit()
        start_pos = reader.stream.tell()
        if header.type_code == EventType.TABLE_MAP_EVENT:
            table_id = reader.uint48()
            reader.seek(2, 1)
            schema_size = reader.int8(NUMBER_TYPE.UNSIGNED)
            schema_name = reader.chars(schema_size)
            reader.seek(1, 1)
            table_size = reader.int8(NUMBER_TYPE.UNSIGNED)
            table_name = reader.chars(table_size)
            reader.seek(header.event_length-(EVENT_LEN.EVENT_HEADER_LEN+EVENT_LEN.TABLE_ID+2+1+schema_size+1+1+table_size),1)
            continue
        elif header.type_code == EventType.WRITE_ROWS_EVENT:
            type_code_contrary = EventType.DELETE_ROWS_EVENT
            if reader.uint48() != table_id:
                print inred("ERROR: METADATA WRONG, TABLE_ID IS NOT RIGHT , EXIT...")
                sys.exit()
            reader.seek(2, 1)
            columns = reader.int8(NUMBER_TYPE.UNSIGNED)
            if columns > 250:
                print inred("ERROR: COLUMNS IS TOO LONG , MAX IS 250 , EXIT ...")
                sys.exit(1)
            table_map = ((columns+7)/8)
            reader.seek(table_map, 1)
            column_results = column_info(host, user, password, port, dbname, schema_name, table_name)
            extra = (EVENT_LEN.EVENT_HEADER_LEN+EVENT_LEN.TABLE_ID+2+1+table_map)
            remain = header.event_length-extra
            while remain != 0:
                bit_map = get_bitmap_new(table_map)
                _sql_list = unpack_record(schema_name, table_name, columns, bit_map, type_code_contrary, column_results)
                sql_command = _sql_list[0]
                execute_command(sql_command, host, user, password, port, dbname)
                remain = remain-int(_sql_list[1])-table_map
                print "SQL COMMAND IS :", sql_command

            continue
        elif header.type_code == EventType.UPDATE_ROWS_EVENT:
            #print "THIS IS A UPDATE ROW EVENTS"
            type_code_contrary = EventType.WRITE_ROWS_EVENT
            if reader.uint48() != table_id:
                print "ERROR: METADATA WRONG, TABLE_ID IS NOT RIGHT , EXIT..."
                sys.exit()
            reader.seek(2, 1)
            columns = reader.int8(NUMBER_TYPE.UNSIGNED)
            if columns > 250:
                print "ERROR: COLUMNS IS TOO MUCH , MAX IS 250 , EXIT ..."
                sys.exit(1)
            table_map = ((columns+7)/8)
            reader.seek(table_map*2, 1)
            table_map_size = table_map*2
            extra = (EVENT_LEN.EVENT_HEADER_LEN+EVENT_LEN.TABLE_ID+2+1+table_map_size)
            remain = header.event_length-extra
            column_results = column_info(host, user, password, port, dbname, schema_name, table_name)
            while remain != 0:
                #OLD RECORD
                bit_map = get_bitmap_new(table_map)
                _sql_list_old = unpack_record(schema_name, table_name, columns, bit_map, type_code_contrary, column_results)
                #NEW RECORD
                bit_map = get_bitmap_new(table_map)
                _sql_list_new = unpack_record(schema_name, table_name, columns, bit_map, type_code_contrary, column_results)
                remain = remain-int(_sql_list_old[1])-int(_sql_list_new[1])-table_map*2
                sql_command_old = _sql_list_old[0]
                sql_command_new = _sql_list_new[0]
                print "SQL COMMAND IS :", sql_command_old
                execute_command(sql_command_old, host, user, password, port, dbname)
            continue
        elif header.type_code == EventType.DELETE_ROWS_EVENT:
            #print "THIS IS A DELETE ROW EVENTS"
            type_code_contrary = EventType.WRITE_ROWS_EVENT
            if reader.uint48() != table_id:
                print "ERROR: METADATA WRONG, TABLE_ID IS NOT RIGHT , EXIT..."
                sys.exit()
            reader.seek(2, 1)
            columns = reader.int8(NUMBER_TYPE.UNSIGNED)
            if columns > 250:
                print "ERROR: COLUMNS IS TOO MUCH , MAX IS 250 , EXIT ..."
                sys.exit(1)
            table_map = ((columns+7)/8)
            reader.seek(table_map,1)
            table_map_size = table_map
            column_results = column_info(host, user, password, port, dbname, schema_name, table_name)
            extra = (EVENT_LEN.EVENT_HEADER_LEN+EVENT_LEN.TABLE_ID+2+1+table_map_size)
            remain = header.event_length-extra
            while remain != 0:
                bit_map = get_bitmap_new(table_map)
                _sql_list = unpack_record(schema_name, table_name, columns, bit_map, type_code_contrary, column_results)
                sql_command = _sql_list[0]
                execute_command(sql_command, host, user, password, port, dbname)
                remain = remain-int(_sql_list[1])-table_map
                print "SQL COMMAND IS :", sql_command
            continue
        elif header.type_code == EventType.ROTATE_EVENT:

            """
                unsupport a transaction cross two log .
                todo: start next relay log , rollback , until commit event.
            """
            print inred("THIS IS A ROTATE_EVENT , MAYBE A TRANSACTION ACROSS TWO RELAY LOG, NOT SUPPORT NOW,EXIT... ")
            #print inred("THIS IS A ROTATE_EVENT, GO ON NEXT ONE RELAY LOG UNTIL COMMIT EVENT.... ")
            reader.close()
            sys.exit()
        else:
            reader.seek(header.event_length-EVENT_LEN.EVENT_HEADER_LEN, 1)

    reader.close()


def check_slave(host, user, password, port):
    slaveinfo={}
    sql = "show slave status"
    conn = getConn(host, user, password, port, 'mysql')
    if conn is False:
        sys.exit()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchone()
    conn.close()
    Seconds_Behind_Master = results.get('Seconds_Behind_Master')
    Last_SQL_Errno = results.get('Last_SQL_Errno')
    Last_SQL_Error = results.get('Last_SQL_Error')
    Relay_Log_File =  results.get('Relay_Log_File')
    Relay_Log_Pos = results.get('Relay_Log_Pos')
    Exec_Master_Log_Pos = results.get('Exec_Master_Log_Pos')
    # if Seconds_Behind_Master is not None:
    #     return False
    # else:
    if Seconds_Behind_Master is None:
        slaveinfo['Last_SQL_Errno'] = Last_SQL_Errno
        slaveinfo['Last_SQL_Error'] = Last_SQL_Error
        slaveinfo['Relay_Log_File'] = Relay_Log_File
        slaveinfo['Relay_Log_Pos'] = Relay_Log_Pos
        slaveinfo['Exec_Master_Log_Pos'] = Exec_Master_Log_Pos
        return slaveinfo
    else:
        return False

def check_python_version():
    if sys.version_info < (2, 6):
        print('At least Python 2.6 is required')
        sys.exit()

def get_datadir(host, user, password, port):
    sql = "show global variables where Variable_name  in ('datadir')"
    conn = getConn(host, user, password, port, 'mysql')
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchone()
    conn.close()
    return results.get('Value')

def sigint_handler(signum, frame):
  global is_sigint_up
  is_sigint_up = True

signal.signal(signal.SIGINT, sigint_handler)


def inred(s):
    return"%s[31;2m%s%s[0m"%(chr(27), s, chr(27))

def usage():
    print("Usage:%s [-h %s -u %s -p %s -P %s ]" %
          (sys.argv[0] , 'localhost' ,'root' ,'xxxxxxx','3306'))
    print '''arguments:
    -h               host/ip. must have.
    -u<user>         username. must have
    -p<string>       password
    -P<NUM>          prot
    -l<string>       log/tmpfile dir
    -t<NUM>          every NUM s  to check slave , default once.
    -h               help
    '''

if __name__ == '__main__' :

    host = ''
    user = ''
    password = ''
    port = ''
    sleep = 5
    logdir = ''
    check_python_version()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:u:p:P:t:m:l:",
                                  ["host=","user=","password=","port=","sleep=","logdir=","help"])
        for opt, arg in opts:
            if opt in ('-h', '--host'):
                host = arg
            elif opt in ('-u', '--user'):
                user = arg
            elif opt in ('-p', '--password'):
                password = arg
            elif opt in ('-P', '--port'):
                port = int(arg)
            elif opt in ('-t', '--sleep'):
                sleep = int(arg)
            elif opt in ('-l', '--logdir'):
                logdir = arg
            elif opt in ('--help'):
                usage()
                sys.exit()
            else:
                usage()
                sys.exit()
    except getopt.GetoptError:
        usage()
        sys.exit()

    if host is '' or user is '':
        print inred("ERROR: HOST/USERNAME IS REQUIRED...")
        sys.exit()


    error_num = 0
    while True:
        slaveinfo = check_slave(host, user, password, port)
        if slaveinfo is not False:
            error_num = 0
            datadir = get_datadir(host, user, password, port)
            Last_SQL_Errno = int(slaveinfo.get('Last_SQL_Errno'))
            Last_SQL_Error = slaveinfo.get('Last_SQL_Error')
            Relay_Log_File = slaveinfo.get('Relay_Log_File')
            Relay_Log_Pos = int(slaveinfo.get('Relay_Log_Pos'))
            Exec_Master_Log_Pos = int(slaveinfo.get('Exec_Master_Log_Pos'))

            if Last_SQL_Errno == 1032 or Last_SQL_Errno == 1062:
                #1032 : key not found, 1062 : duplicate key
                binlogfile = datadir+Relay_Log_File
                end_log_pos = int(Last_SQL_Error.split(' ')[-1])
                start_position = Relay_Log_Pos
                stop_position = Relay_Log_Pos + (end_log_pos - Exec_Master_Log_Pos)
                print "*"*100
                print " "*10 + "ERROR FOUND !!! STRAT TO REPAIR RECORD..."
                print "*"*100
                print "RELAYLOG FILE : %s " % (binlogfile)
                print "START POSITION : %s . STOP POSITION : %s " % (start_position, stop_position)
                print "ERROR MESSAGE : %s" % (Last_SQL_Error)
                hanle_error(binlogfile, start_position, stop_position, host, user, password, port, 'mysql')
                sql = "stop slave"
                execute_command(sql, host, user, password, port, 'mysql')
                sql = "start slave"
                execute_command(sql, host, user, password, port, 'mysql')

            else:
                print inred("ERROR TYPE IS NOT RIGHT, RESTART SLAVE MAY RESOLVE PROBLEM !")
                print Last_SQL_Error
                sys.exit()

        else:
            error_num = error_num + 1
            print "SLAVE CHECK OK !!!, SKIP..."
            if error_num > 100:
                print "RUN 100 Times ALL OK, EXIT..."
                #sys.exit()
        if is_sigint_up:
            print '-'*50
            time.sleep(1)
            print inred("[WARNING] : Exit the Program .....")
            break
        else:
            time.sleep(sleep)

