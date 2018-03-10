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

    ##starting Instance
    startlist = aurora.list_rds_schedule(StartTime=current[0])
    for s in startlist:
        tags =s.get("tags")
        time_s =  utils.get_time("time:start",tags)
        time_e =   utils.get_time("time:end",tags)

        print current[0],current[1]
        print "start", time_s[0],time_s[1]
        if utils.can_start(current,time_s,time_e,tags):
            print "starting.."
            print s.get("cluster_name")
            print aurora.start_db(
                SnapshotName=s.get("stopinator:snapshot"),
                ClusterName=s.get("cluster_name"),
                SubnetGroupName=s.get("subnet_group"),
                Tags=s.get("tags")
            )
            tags.append({"Key":"stopinator:start:time","Value":current[2]})
            s['tags'] =tags
            aurora.update_progress(s,Progress='starting')


    ##updating cluster information
    clusters = aurora.list_cluster()
    for cs in clusters:
        c_name = cs['DBClusterIdentifier']
        cstatus = cs['Status']

        info = cs['InstanceInfo']
        i_name= info['DBInstanceIdentifier']
        print "Analysing db cluster: "+c_name+"["+info.get('Status')+"]"
        if cs['Status']== 'available' and info['Status'] == 'available':
            aurora.sync_metadata(cs)
        else:
            print cs['Status'],info['Status']
        if info.get('Status') == 'available' and info.get('DBClusterParameterGroupStatus') == 'pending-reboot':
            aurora.reboot(i_name)
            print 'rebooting :'+i_name
        else:
            tags = info.get("Tags")

    #check cluster need STARTING
    #
    stoplist = [] #aurora.list_rds_schedule(StopTime=current[0])
    for s in stoplist:
        print "Analysing Cluster to Stop :"+ s['cluster_name']
        c = aurora.list_cluster(ClusterIdentifier=s['cluster_name'])
        snapshot = s.get('stopinator:snapshot')
        progress = s.get('stopinator:progress')


        if len(c) == 1:
            cs = c[0]
            c_name = cs['DBClusterIdentifier']
            info = cs.get('InstanceInfo')
            if cs['Status']== 'available' and info['Status'] == 'available':
                ct = utils.current_time(tz)
                ts = ct[2].replace("T","-")
                name = 'stopinator-'+c_name+'-'+ts
                ss = s.get('stopinator:snapshot')
                print ss;
                if not s.get('stopinator:snapshot'):
                    print "creating snapshot :"+name
                    name = name.replace(":","-")
                    aurora.update_progress(s,SnapshotName=name,Progress='create-snapshot')
                    aurora.create_snapshot(c_name,name)
                else:
                    if s.get('stopinator:progress') == 'create-snapshot':
                        print "creating snapshot:"+s.get('stopinator:snapshot')
                        sstatus = aurora.check_status_snapshot(s.get('stopinator:snapshot'))
                        if sstatus == 'available':
                            aurora.update_progress(s,SnapshotName=name,Progress='mark-delete')



    delete = []#aurora.list_rds_schedule(Deleted=True)
    print len(delete)
    for d in delete:
        print "about to delete "+d['cluster_name']
        aurora.update_progress(d,Progress='deleted')
        aurora.delete(d['db_instance_name'])

    return "OK"

lambda_handler({"timezone":"Australia/NSW"},{})
