import aurora
def start_aurora(env):
    # start db clister

    list =aurora.list_rds_schedule(StartTime=-1,Matcher=env)
    for s in list:
        tags =s.get(aurora.CONST_FIELD_TAGS)
        print "starting cluster :"+s.get(aurora.CONST_FIELD_CLUSTER_NAME)+" ..."
        print s.get(aurora.CONST_FIELD_SECURITY_GROUP_IDS)
        tags =s.get(aurora.CONST_FIELD_TAGS)
        utils.update_tags(tags,{"Key":utils.CONST_STOPINATOR_START_TIME,"Value":current[2]})
        #print tags

        success = aurora.start_db(
            SnapshotName=s.get(aurora.CONST_FIELD_SNAPSHOT),
            ClusterName=s.get(aurora.CONST_FIELD_CLUSTER_NAME),
            SubnetGroupName=s.get(aurora.CONST_FIELD_SUBNET_GROUP),
            SecurityGroupIds=s.get(aurora.CONST_FIELD_SECURITY_GROUP_IDS),
            Tags=tags
        )
def start_mysql(env):

    for db in dbs:
        tags = db['Tags']
        identifier = db['DBInstanceIdentifier']
        status = db['DBInstanceStatus']
        current = utils.current_time(tz)
        if status != 'available' and utils.can_start(current,tags):
            print "STARTING DB INSTANCE: "+identifier
            mysql.start_instance(db,tz)
