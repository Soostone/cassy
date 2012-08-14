from pycassa.types import *
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily


def create_ks():
    # create test keyspace
    sys = SystemManager()
    comparator = CompositeType(LongType(), BytesType())
    sys.create_column_family("testing", "testing", comparator_type=comparator)



pool = ConnectionPool('testing')
cf = ColumnFamily(pool, 'testing')


# Check the column added by the Haskell test script
# print [k for k in cf.get_range()]
# cf.insert("row2", {(125, 'oklahoma'): 'asdf'})

print cf.get('row1')
# should see: OrderedDict([((125, 'oklahoma'), 'asdf')])

