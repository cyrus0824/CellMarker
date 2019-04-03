import os
import uuid
import copy

from biothings.utils.dataload import open_anyfile, dict_sweep
from csv import DictReader

from collections import defaultdict

def parse_field(field_string):
    _ret = []
    _cluster = None
    for _obj in field_string.split(', '):
        if _obj.startswith('['):
            _cluster = [_obj.lstrip('[')]
        elif _obj.endswith(']'):
            _cluster.append(_obj.rstrip(']'))
            _ret.append(_cluster)
            _cluster = None
        else:
            if _cluster:
                _cluster.append(_obj)
            else:
                _ret.append(_obj)
    return _ret

def generate_id(doc):
    _id = uuid.uuid1()
    _id = str(_id).replace('-', '')
    return _id

def load_data(data_folder):
    input_file = os.path.join(data_folder, "all_cell_markers.txt")
    assert os.path.exists(input_file), "Can't find input file '{}'".format(input_file)

    with open_anyfile(input_file) as in_f:
        header = next(in_f).strip().split('\t')
        reader = DictReader(in_f, fieldnames=header, delimiter='\t')

        for row in reader:
            for field in ['cellMarker', 'geneID', 'geneSymbol', 'proteinID', 'proteinName']:
                row[field] = parse_field(row[field])
            if not all([
                len(row['cellMarker']) == len(row['geneID']),
                len(row['cellMarker']) == len(row['geneSymbol']),
                len(row['cellMarker']) == len(row['proteinID']),
                len(row['cellMarker']) == len(row['proteinName'])]):
                # handle weird cases...
                pass
            else:
                # they all match
                for index in range(len(row['cellMarker'])):
                    r = copy.copy(row)
                    for field in ['cellMarker', 'geneID', 'geneSymbol', 'proteinID', 'proteinName']:
                        r[field] = r[field][index]
                    _id = generate_id(r)
                    yield {"_id": _id,
                            "CellMarker": r} 
