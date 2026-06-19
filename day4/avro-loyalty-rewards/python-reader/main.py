from avro.datafile import DataFileReader
from avro.io import DatumReader

# Open the file in binary read mode
with open("../purchase.avro", "rb") as f:
    reader = DataFileReader(f, DatumReader())

    # The reader is an iterator yielding records as Python dicts
    for record in reader:
        print(record)

    reader.close()
