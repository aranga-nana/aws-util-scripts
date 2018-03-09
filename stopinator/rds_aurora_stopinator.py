from aws import utils,aurora

#aurora.add_db_info({})

#s =aurora.get_most_reason_snapshot('acnonline-prod-1')
#print s
aurora.start_db('start-db-acnonline-prod-2018-03-09-11-03','acnonline-prod')
#aurora.create_instance()
#for s in snapshots:
#    print s['DBClusterSnapshotIdentifier'],s['SnapshotCreateTime']
