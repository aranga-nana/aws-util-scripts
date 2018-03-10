from aws import utils,aurora

aurora.init_table()
def lambda_handler(event, context):

    tz = utils.get_tz(event)
    current = utils.current_time(tz)

    ##starting Instance
    startlist = aurora.list_rds_schedule(StartTime=current[0])
    for s in startlist:
        tags =s.get("tags")

        time_s =  utils.get_time("time:start",tags)
        time_e =   utils.get_time("time:stop",tags)

        #print current,time_s,time_e
        #print "start", time_s[0],time_s[1],s.get('stopinator:progress')
        if  s.get('stopinator:progress') == 'deleted' and utils.can_start(current,time_s,time_e,tags):
            print "starting cluster :"+s.get("cluster_name")+" ..."
            print s.get('security_group_ids')
            success = aurora.start_db(
                SnapshotName=s.get("stopinator:snapshot"),
                ClusterName=s.get("cluster_name"),
                SubnetGroupName=s.get("subnet_group"),
                SecurityGroupIds=s.get('security_group_ids'),
                Tags=s.get("tags")
            )
            if success:
                tags.append({"Key":"stopinator:start:time","Value":current[2]})
                s['tags'] =tags
                s['stopinator:snapshot']=None
                aurora.update_progress(s,Progress='starting')
            else:
                print "starting cluster :"+s.get("cluster_name")+" Failed!!!!"
        else:
            css = aurora.list_cluster(ClusterIdentifier=s['cluster_name'])
            if len(css) == 1:
                cs =css[0]
                info = cs['InstanceInfo']
                if s.get('stopinator:progress') == 'starting':
                    if cs['Status']== 'available' and info['Status'] == 'available' and info.get('DBClusterParameterGroupStatus') != 'pending-reboot':
                        aurora.modify_cluster_group(s['cluster_name'],s.get('cluster_parameter_group'))
                        aurora.update_progress(s,Progress='witing-update')
                else:
                    if s.get('stopinator:progress') == 'witing-update' and info.get('DBClusterParameterGroupStatus') == 'in-sync':
                        dt = utils.current_time(tz)
                        s['stopinator:start-completed'] = ds[2]
                        aurora.update_progress(s,Progress='started')


    ##updating cluster information
    clusters = aurora.list_cluster()
    for cs in clusters:
        #print cs
        c_name = cs['DBClusterIdentifier']
        cstatus = cs['Status']

        info = cs['InstanceInfo']
        i_name= info['DBInstanceIdentifier']
        print "Analysing db cluster: "+c_name+"["+info.get('Status')+"]"
        if cs['Status']== 'available' and info['Status'] == 'available':
            aurora.sync_metadata(cs)
        else:
            print "DBCluster :"+c_name+", Cluster-Status:"+cs['Status']+", Instance-status:"+info['Status']
        if info.get('Status') == 'available' and info.get('DBClusterParameterGroupStatus') == 'pending-reboot':
            aurora.reboot(i_name)
            print 'rebooting :'+i_name
        else:
            tags = info.get("Tags")

    #check cluster need STARTING
    #
    stoplist =aurora.list_rds_schedule(StopTime=current[0])
    for s in stoplist:
        print "Analysing Cluster to Stop :"+ s['cluster_name']
        c = aurora.list_cluster(ClusterIdentifier=s['cluster_name'])
        snapshot = s.get('stopinator:snapshot')
        progress = s.get('stopinator:progress')

        if len(c) == 1:
            cs = c[0]
            c_name = cs['DBClusterIdentifier']
            info = cs.get('InstanceInfo')
            tags = info.get('Tags')
            time_e= utils.get_time('time:stop',tags)
            time_b= utils.get_time('time:start',tags)

            if utils.can_stop(current,time_b,time_e) and cs['Status']== 'available' and info['Status'] == 'available':
                ct = utils.current_time(tz)
                ts = ct[2].replace(":","-")
                ts = ts.replace("T","-")
                name = 'stopinator-'+c_name+'-'+ts
                ss = s.get('stopinator:snapshot')
                print ss;
                if not s.get('stopinator:snapshot'):
                    print "creating snapshot :"+name
                    name = name.replace(":","-")
                    aurora.update_progress(s,SnapshotName=name,Progress='create-snapshot')
                    aurora.create_snapshot(c_name,name)
            if s.get('stopinator:progress') == 'create-snapshot' and cs['Status']== 'available' and info['Status'] == 'available':
                print "creating snapshot:"+s.get('stopinator:snapshot')
                sstatus = aurora.check_status_snapshot(s.get('stopinator:snapshot'))
                print "creating snapshot status: "+sstatus
                if sstatus == 'available':
                    aurora.update_progress(s,SnapshotName=name,Progress='mark-delete')
            if s.get('stopinator:progress') == 'mark-delete':
                print "pending delete..waiting"



    delete = aurora.list_rds_schedule(MarkDelete=True)
    print len(delete)
    for d in delete:
        print "about to delete "+d['cluster_name']
        aurora.update_progress(d,Progress='deleted')
        aurora.delete(d['db_instance_name'])

    return "OK"

lambda_handler({"timezone":"Australia/NSW"},{})
