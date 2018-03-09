from aws import utils,aurora

table = aurora.get_table()
print table
snapshots = aurora.list_snapshots()
snapshots.sort(key=lambda k: k['SnapshotCreateTime'])
for s in snapshots:
    print s['DBClusterSnapshotIdentifier'],s['SnapshotCreateTime']
