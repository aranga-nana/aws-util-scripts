from aws import utils,aurora
import time
aurora.init_table()
def lambda_handler(event, context):

    tz = utils.get_tz(event)
    pattern = utils.get_pattern(event)

    current = utils.current_time(tz)
    print "Matcher",pattern

    ##starting Instance

    startlist = aurora.list_rds_schedule(StartTime=current[0],Matcher=pattern)
    for s in startlist:
        tags =s.get(aurora.CONST_FIELD_TAGS)

        time_s =  utils.get_time(utils.CONST_KEY_TIME_START,tags)
        time_e =   utils.get_time(utils.CONST_KEY_TIME_STOP,tags)

        #print current,time_s,time_e
        #print "start", time_s[0],time_s[1],s.get(aurora.CONST_FIELD_PROGRESS)
        if  s.get(aurora.CONST_FIELD_PROGRESS) == aurora.CONST_PROGRESS_STATUS_DELETED  and utils.can_start(current,tags):
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
            if success:
                ts = utils.current_time(tz)
                aurora.update_progress(s,Progress=aurora.CONST_PROGRESS_STATUS_STARTING)
                time.sleep(.200)
            else:
                print "starting cluster :"+s.get(aurora.CONST_FIELD_CLUSTER_NAME)+" Failed!!!!"
        else:
            css = aurora.list_cluster(ClusterIdentifier=s[aurora.CONST_FIELD_CLUSTER_NAME])
            #print css
            if len(css) == 1:
                cs =css[0]
                info = cs['InstanceInfo']
                if s.get(aurora.CONST_FIELD_PROGRESS) == 'starting':
                    if cs['Status']== 'available' and info['Status'] == 'available' and info.get('DBClusterParameterGroupStatus') != 'pending-reboot':
                        print "updating db cluster parameter group.."
                        aurora.modify_cluster_group(s[aurora.CONST_FIELD_CLUSTER_NAME],s.get('cluster_parameter_group'))
                        aurora.update_progress(s,Progress=aurora.CONST_PROGRESS_STATUS_MODIFY_PARAM_GROUP)
                else:
                    if s.get(aurora.CONST_FIELD_PROGRESS) == aurora.CONST_PROGRESS_STATUS_MODIFY_PARAM_GROUP and info.get('DBClusterParameterGroupStatus') == 'in-sync':
                        dt = utils.current_time(tz)
                        s['stopinator:start-completed'] = dt[2]
                        s['stopinator:snapshot'] = None
                        aurora.update_progress(s,Progress='started')
                        print "Cluster:"+s[aurora.CONST_FIELD_CLUSTER_NAME]+" started.."


    ##updating cluster information
    print "----------- Sync meta data ---------------"
    print "\n"
    clusters = aurora.list_cluster()
    for cs in clusters:
        #print cs
        c_name = cs['DBClusterIdentifier']
        cstatus = cs['Status']
        info = cs['InstanceInfo']
        i_name= info['DBInstanceIdentifier']

        if (len(cs.get('DBClusterMembers')) > 0):

            if cs['Status']== 'available' and info['Status'] == 'available':
                #print "re-sync metadat, cluster:"+c_name
                #print info.get("Tags")
                print "Cluster :",c_name+"["+cstatus+"], Instance :"+i_name+"["+info['Status']+"]"

                print "updating dynamodb with current cluster info ..."
                print "\n"

                aurora.sync_metadata(cs)
                print "\n"
            else:
                print "Cluster :",c_name+"["+cstatus+"], Instance :"+i_name+"["+info['Status']+"]"

            if info.get('Status') == 'available' and info.get('DBClusterParameterGroupStatus') == 'pending-reboot':
                aurora.reboot(i_name)
                print 'rebooting :'+i_name
                print "\n"
            else:
                tags = info.get("Tags")
        else:
            print "no matching cluster infor. "


    #check cluster need STARTING
    #
    stoplist =aurora.list_rds_schedule(StopTime=current[0],Matcher=pattern)
    print "----------- analysing cluster schedule to stop"
    for s in stoplist:

        c = aurora.list_cluster(ClusterIdentifier=s[aurora.CONST_FIELD_CLUSTER_NAME])
        snapshot = s.get('stopinator:snapshot')
        progress = s.get('stopinator:progress')

        if len(c) == 1:
            cs = c[0]
            c_name = cs['DBClusterIdentifier']
            info = cs.get('InstanceInfo')
            tags = info.get('Tags')
            time_e= utils.get_time('time:stop',tags)
            time_b= utils.get_time('time:start',tags)

            if utils.can_stop(current,tags) and cs['Status']== 'available' and info['Status'] == 'available':
                print "Stopiing Cluster "+ s['cluster_name']+" ..."
                ct = utils.current_time(tz)
                ts = ct[2].replace(":","-")
                ts = ts.replace("T","-")
                name = 'stopinator-'+c_name+'-'+ts
                print "progres :",s.get('stopinator:progress')
                if s.get('stopinator:progress') != 'create-snapshot' and s.get('stopinator:progress') != 'mark-delete':
                    print "creating snapshot :"+name
                    sname=aurora.create_snapshot(c_name,name)
                    s['stopinator:snapshot']=sname
                    aurora.update_progress(s,SnapshotName=sname,Progress='create-snapshot')

            if s.get(aurora.CONST_FIELD_PROGRESS) == 'create-snapshot' and cs['Status']== 'available' and info['Status'] == 'available':
                print "creating snapshot in progress :"+s.get(aurora.CONST_FIELD_SNAPSHOT)
                sstatus = aurora.check_status_snapshot(s.get(aurora.CONST_FIELD_SNAPSHOT))
                print "checking snapshot progress status: "+sstatus
                if sstatus == 'available':
                    aurora.update_progress(s,Progress=aurora.CONST_PROGRESS_STATUS_MARK_FOR_DELETE)
            else:
                if s.get(aurora.CONST_FIELD_PROGRESS) == 'create-snapshot':
                    print "creating snapshot in progress :"+s.get(aurora.CONST_FIELD_SNAPSHOT)
                    sstatus = aurora.check_status_snapshot(s.get(aurora.CONST_FIELD_SNAPSHOT))
                    print "checking snapshot progress status: "+sstatus

            if s.get(aurora.CONST_FIELD_PROGRESS) == aurora.CONST_PROGRESS_STATUS_MARK_FOR_DELETE:
                print "pending delete..waiting"
        else:
            print "nothing to stop"


    print "----------- analysing cluster marked for deletion"
    delete = aurora.list_rds_schedule(MarkDelete=True)
    #print "delete list"+`len(delete)`
    for d in delete:
        print "About to delete cluster : "+d[aurora.CONST_FIELD_CLUSTER_NAME]+" ..."
        aurora.update_progress(d,Progress='deleted')
        aurora.delete(d['db_instance_name'])

    return "OK"

lambda_handler({"timezone":"Australia/NSW","pattern":["bau*","ac*"]},{})
