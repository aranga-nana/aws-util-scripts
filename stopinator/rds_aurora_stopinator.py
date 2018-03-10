from aws import utils,aurora

#aurora.add_db_info({})

#s =aurora.get_most_reason_snapshot('acnonline-prod-1')
#print s
#aurora.start_db('start-db-acnonline-prod-2018-03-09-11-03','acnonline-prod')
#result = aurora.list_cluster()

#print result[0]
#for rr in result:
#    print rr['Tags'],rr['AvailableUpdate']
aurora.init_table()
def lambda_handler(event, context):
    tz = utils.get_tz(event)
    current = utils.current_time(tz)
    clusters = aurora.list_cluster()
    for cs in clusters:
        print cs['DBClusterIdentifier']
    return "OK"

lambda_handler({"timezone":"Australia/NSW"},{})
