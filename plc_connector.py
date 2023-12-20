import pymcprotocol
import logging
import utils
import struct

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def connect(connection_data):
    try:
        # if you use iQ-R series PLC,
        pymc3e = pymcprotocol.Type3E(plctype="iQ-R")
        # If you use ascii byte communication, (Default is "binary")
        pymc3e.setaccessopt(commtype="binary")
        pymc3e.connect(connection_data['ip'], connection_data['port'])
        return pymc3e
    except Exception as e:
        logging.exception(e)



def read_tags_value(tags_info, pymc3e):
    tags_info_parsed = {}
    tags_info_parsed['Bit_word'] = {}

    #Parsed tags in a dict of dicts
    for tag_type in tags_info:
        tags_info_parsed[tag_type] = {}
        for tag in tags_info[tag_type]:
            tags_info_parsed[tag_type][tag['address']] = {'id': tag['id'], 'type': tag['type'], 'metric_type': tag['type_id']}
    try:
        # The tags of D memory can't be reading as bit, we need read that like words
        for bit in tags_info_parsed['Bit']:
            if '.' in bit:
                if bit.split('.')[0] not in tags_info_parsed['Bit_word']:
                    tags_info_parsed['Bit_word'][bit.split('.')[0]] = {}
        tags_info_parsed = read_words(tags_info_parsed, pymc3e)
        tags_info_parsed = read_string(tags_info_parsed, pymc3e)
        tags_info_parsed = read_bits(tags_info_parsed, pymc3e)
        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise e


def read_words(tags_info_parsed, pymc3e):
    words = []
    dwords = []
    word_values = []
    dword_values = []
    try:
        # Creating a list of words
        if 'Word [Signed]' in tags_info_parsed:
            for tag in tags_info_parsed['Word [Signed]']:
                words.append(tag)
        # The part not decimal of this bits must be read like words
        if 'Bit_word' in tags_info_parsed:
            for tag in tags_info_parsed['Bit_word']:
                words.append(tag)
        # Creating a list of double words
        if 'Double Word [Signed]' in tags_info_parsed:
            for tag in tags_info_parsed['Double Word [Signed]']:
                dwords.append(tag)
        # Reading words in group of 95, maximun number of protocol
        for group in utils.chunker(words, 95):
            word_values = word_values + pymc3e.randomread(word_devices=group, dword_devices=[])[0]
        # Reading doubles words in group of 95, maximun number of protocol
        for group in utils.chunker(dwords, 95):
            dword_values = dword_values + pymc3e.randomread(word_devices=[], dword_devices=group)[1]
        # assings values to dict of tags
        for tag, value in zip(words, word_values):
            if 'Bit_word' in tags_info_parsed and tag in tags_info_parsed['Bit_word']:
                tags_info_parsed['Bit_word'][tag]['value'] = value
            elif 'Word [Signed]' in tags_info_parsed and tag in tags_info_parsed['Word [Signed]']:
                tags_info_parsed['Word [Signed]'][tag]['value'] = value
        for tag, value in zip(dwords, dword_values):
            tags_info_parsed['Double Word [Signed]'][tag]['value'] = value
        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise


def read_string(tags_info_parsed, pymc3e):
    try:
        for tag_type in tags_info_parsed:
            if 'String' in tag_type:
                for tag in tags_info_parsed[tag_type]:
                    wordunits_values = pymc3e.batchread_wordunits(headdevice=tag, readsize=int(tag_type.split('(')[1].replace(')', '')))
                    string = ""
                    for word in wordunits_values:
                        string = string + word.to_bytes(2, 'little').decode("utf-8", 'ignore')
                    tags_info_parsed[tag_type][tag]['value'] = string.replace('\x00','')
        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise


def read_bits(tags_info_parsed, pymc3e):
    bits_to_read = []
    try:
        for tag in tags_info_parsed['Bit']:
            if 'D' not in tag:
                if bits_to_read == [] or (tag[0]==bits_to_read[-1][0] and int(tag[1:]) < int(bits_to_read[-1][1:]) + 100):
                    bits_to_read.append(tag)
                    if tag == list(tags_info_parsed['Bit'].keys())[-1]:
                        bits_values = read_bits_plc(bits_to_read, pymc3e)
                        tags_info_parsed = parse_bits(bits_values=bits_values, bits_to_read=bits_to_read, tags_info_parsed=tags_info_parsed)
                else:
                    bits_values = read_bits_plc(bits_to_read, pymc3e)
                    tags_info_parsed = parse_bits(bits_values=bits_values, bits_to_read=bits_to_read, tags_info_parsed=tags_info_parsed)
                    bits_to_read = []
                    bits_to_read.append(tag)
        parse_bit_word(tags_info_parsed)
        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise


def read_bits_plc(bits_to_read, pymc3e):
    try:
        size = int(bits_to_read[-1][1:]) - int(bits_to_read[0][1:])
        bits_values = pymc3e.batchread_bitunits(headdevice=bits_to_read[0], readsize=size + 1)
        return bits_values
    except Exception as e:
        logging.exception(e)
        return []


def parse_bits(bits_values, bits_to_read, tags_info_parsed):
    try:
        for bit in bits_to_read:
            #tags_info_parsed['Bit'][bit]['value'] = bits_values[int(bits_to_read[0][1:]) - int(bit[1:])]
            tags_info_parsed['Bit'][bit]['value'] = bits_values[-(int(bits_to_read[0][1:]) - int(bit[1:]))]

        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise


def parse_bit_word(tags_info_parsed):
    try:
        get_bin = lambda x, count=8: "".join(map(lambda y:str((x>>y)&1), range(count-1, -1, -1)))
        for bit_word in tags_info_parsed['Bit_word']:
            bin_value = get_bin(tags_info_parsed['Bit_word'][bit_word]['value'], 16)
            for bit in range(16):
                tags_info_parsed['Bit'][f'{bit_word}.{hex(bit).replace("0x", "").upper()}']['value'] = bin_value[bit]
        return tags_info_parsed
    except Exception as e:
        logging.exception(e)
        raise