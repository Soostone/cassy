from pycassa.types import *
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

# create test keyspace
sys = SystemManager()
comparator = CompositeType(LongType(), BytesType())
sys.create_column_family("testing", "testing", comparator_type=comparator)

# create test ColumnFamily
pool = ConnectionPool('testing')
cf = ColumnFamily(pool, 'testing')

# Check the column added by the Haskell test script
cf.get('row1')


# should see: OrderedDict([((125, 'oklahoma'), 'asdf')])
