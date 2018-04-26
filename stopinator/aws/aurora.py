import time
import boto3
import utils
from boto3.dynamodb.conditions import Key, Attr

CONST_TABLE="dev_aurora_db_meta"
CONST_FIELD_SNAPSHOT="stopinator:snapshot"
CONST_FIELD_PROGRESS="stopinator:progress"
CONST_FIELD_CLUSTER_NAME="cluster_name"
CONST_FIELD_TIME_STOP_NUM="time_stop_hh"
CONST_FIELD_TIME_START_NUM="time_start_hh"
CONST_FIELD_TAGS="tags"
CONST_FIELD_CLUSTER_PARAMETER_GROUP="cluster_parameter_group"
CONST_FIELD_SUBNET_GROUP="subnet_group"
CONST_FIELD_SECURITY_GROUP_IDS="security_group_ids"

CONST_PROGRESS_STATUS_STARTING="starting"
CONST_PROGRESS_STATUS_DELETED="deleted"
CONST_PROGRESS_STATUS_MODIFY_PARAM_GROUP="witing-update"
CONST_PROGRESS_STATUS_STARTED="started"
CONST_PROGRESS_STATUS_MARK_FOR_DELETE="mark-delete"


rds = boto3.client('rds',region_name="ap-southeast-2")
dynamodb = boto3.resource('dynamodb',region_name="ap-southeast-2")



def init_table():

    try:
        dbclient = boto3.client('dynamodb',region_name="ap-southeast-2")
        table = dbclient.create_table(
            TableName=CONST_TABLE,
            KeySchema=[
                {
                    'AttributeName': CONST_FIELD_CLUSTER_NAME,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': CONST_FIELD_CLUSTER_NAME,
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits':2
            }
        )
        print table
        time.sleep(.500)
        print "dynamodb table created:"+CONST_TABLE
    except Exception as e:
        #print e
        pass
def check_status_snapshot(snapshot):
    response = rds.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=snapshot
    )
    s = response['DBClusterSnapshots']
    if len(s)==1:
         return s[0]['Status']
    else:
        return 'pending'

def cleanup(identifier):
    response = rds.delete_db_cluster(
        DBClusterIdentifier=identifier,
        SkipFinalSnapshot=True
    )
def delete(identifier):
    response = rds.delete_db_instance(
        DBInstanceIdentifier=identifier,
        SkipFinalSnapshot=True,

    )
    print response

def sync_metadata(cs):

    cluster_name = cs.get('DBClusterIdentifier')
    info = cs.get('InstanceInfo')
    tags = info.get('Tags')
    hhmm = utils.get_time(utils.CONST_KEY_TIME_STOP,tags)
    sshh = utils.get_time(utils.CONST_KEY_TIME_START,tags)
    #print hhmm,sshh
    #extract security groups
    sgs = cs['VpcSecurityGroups']
    #print sgs
    securityGroupIds =[]
    for sg in sgs:
        securityGroupIds.append(sg['VpcSecurityGroupId'])

    table = dynamodb.Table(CONST_TABLE)

    Found = False
    try:
        response = table.query( KeyConditionExpression=Key(CONST_FIELD_CLUSTER_NAME).eq(cluster_name))
        #print "Working with existing metadat.."
        Found = True
    except Exception as e:
        pass

    item={
            CONST_FIELD_CLUSTER_NAME :cluster_name,
            CONST_FIELD_TIME_STOP_NUM:hhmm[0],
            CONST_FIELD_TIME_START_NUM:sshh[0],
            CONST_FIELD_TAGS:tags,
            CONST_FIELD_CLUSTER_PARAMETER_GROUP:cs.get('DBClusterParameterGroup'),
            CONST_FIELD_SUBNET_GROUP:cs.get('DBSubnetGroup'),
            CONST_FIELD_SECURITY_GROUP_IDS:securityGroupIds,
            "db_instance_name":info.get('DBInstanceIdentifier'),

    }
    if Found and len(response.get('Items'))>0:
       item = response['Items'][0]
       item[CONST_FIELD_CLUSTER_NAME] = cluster_name
       item[CONST_FIELD_TIME_STOP_NUM]=hhmm[0]
       item[CONST_FIELD_TIME_START_NUM]=sshh[0]
       item[CONST_FIELD_TAGS]=tags
       item[CONST_FIELD_CLUSTER_PARAMETER_GROUP]=cs.get('DBClusterParameterGroup')
       item[CONST_FIELD_SUBNET_GROUP] = cs.get('DBSubnetGroup')
       item[CONST_FIELD_SECURITY_GROUP_IDS] =securityGroupIds
       item["db_instance_name"] = info.get('DBInstanceIdentifier')


    response = table.put_item(Item = item)
    print "dynmaod response",response
    time.sleep(.120)

## update the progress -diffrent stage of the start/delete cycle
def update_progress(item, **kwargs):
    snapshot = kwargs.get('SnapshotName')
    progress = kwargs.get('Progress')
    if not kwargs.get('SnapshotName'):
        print snapshot
    else:
        print "updating snapshot: "+kwargs.get('SnapshotName')
        item['stopinator:snapshot'] = kwargs.get('SnapshotName')
    if not progress:
        print progress
    else:
        item['stopinator:progress'] = progress

    table = dynamodb.Table(CONST_TABLE)
    response = table.put_item(Item=item)

def list_rds_schedule(**kwargs):
    table = dynamodb.Table(CONST_TABLE)
    r =[]
    ManualStart = False
    try:
        if kwargs.get('StopTime') >0:

            response = table.scan(
                FilterExpression=Attr('time_stop_hh').eq(kwargs.get('StopTime'))
            )
            r = response['Items']

        if kwargs.get('MarkDelete'):
            response = table.scan(
                FilterExpression=Attr('stopinator:progress').eq('mark-delete')
            )
            r = response['Items']

        if kwargs.get('StartTime') >=0:
            response = table.scan(
                FilterExpression=Attr('time_start_hh').eq(kwargs.get('StartTime'))
            )
            r = response['Items']
        if kwargs.get('Deleted'):
            response = table.scan(
                FilterExpression=Attr('stopinator:progress').eq('deleted')
            )
            r = response['Items']
        if kwargs.get('Matcher') and kwargs.get('StartTime') == -1:
            response = table.scan(
                KeyConditionExpression=Key('cluster_name').contain(kwargs.get('Matcher'))
            )
            r = response['Items']
            ManualStart = True

    except Exception as e:
        pass
    if ManualStart:
        return r
    #filter out any non mange aurora instances
    r = list(filter(lambda x:utils.get_tag_val("stopinator",x['tags'])=="true",r))
    #print "before matcher :",r
    if not kwargs.get('Matcher'):
        return r
    print "check length"
    if  len(kwargs.get('Matcher'))==0:
        return r
    #print "checking matcher ",kwargs.get('Matcher')
    r = utils.pattern_filter(SourceList=r,Matcher=kwargs.get('Matcher'),Key="cluster_name")
    return r



#list db instace infor based on identifier
#Helper to list_cluster()
def list_member_info(identifier):
    response = rds.describe_db_instances(DBInstanceIdentifier=identifier)
    if len(response['DBInstances'][0]) >0:
        return response['DBInstances'][0]
    return None

## list db cluster
##Note modify original response with db instance information as well
def list_cluster(**kwargs):
    response = {}
    if not kwargs.get("ClusterIdentifier"):
        response = rds.describe_db_clusters()
    else:
        try:
            response = rds.describe_db_clusters(DBClusterIdentifier=kwargs.get("ClusterIdentifier"))
        except Exception as e:
            pass


    res=[]
    if not response.get('DBClusters'):
        return res
    if len(response.get('DBClusters'))==0:
        return res
    for c in response['DBClusters']:
        if len(c.get('DBClusterMembers')) > 0:
            m= c['DBClusterMembers'][0]
            #print m['DBInstanceIdentifier']

            i =list_member_info(m['DBInstanceIdentifier'])
            #print i
            extra={}
            extra['DBClusterParameterGroupStatus'] = m['DBClusterParameterGroupStatus']
            extra['Instance'] = i
            extra['Status'] = i['DBInstanceStatus']
            extra['DBInstanceIdentifier'] = i['DBInstanceIdentifier']

            tags = rds.list_tags_for_resource(ResourceName=i['DBInstanceArn'])['TagList']
            extra['Tags'] = tags

            #print c['InstanceInfo']
            c['InstanceInfo'] = extra
            #only add manged rds instances
            if utils.get_tag_val("stopinator",tags) == "true" or utils.get_tag_val("stopinator",tags) == "false":
                res.append(c)
        else:
            #cleanup ophen cluster (instance are deleted)
            if c['Status']== 'available':
                print "cluster: "+c['DBClusterIdentifier']+", cleanup initiated.... "
                cleanup(c['DBClusterIdentifier'])


    #print res
    return res

def start_db(**kwargs):
    if not kwargs.get("SnapshotName"):
        print "snapshot empty"
        return False
    if not kwargs.get("ClusterName"):
        print "cluster  empty"
        return False
    if not kwargs.get("SubnetGroupName"):
        print "SubnetGroupName  empty"
        return False
    if not kwargs.get("Tags"):
        print "Tags  empty"
        return False
    if not kwargs.get("SecurityGroupIds"):
        print "SecurityGroupIds  empty"
        return False
    cluster_name = kwargs.get("ClusterName")
    snapshot =  kwargs.get("SnapshotName")

    response = rds.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=cluster_name,
            SnapshotIdentifier=snapshot,
            Engine='aurora',
            DBSubnetGroupName=kwargs.get("SubnetGroupName"),
            VpcSecurityGroupIds=kwargs.get("SecurityGroupIds"),
            Tags=kwargs.get("Tags")
    )
    r= response['DBCluster']
    print r.get('Status')
    response = rds.create_db_instance(
            DBInstanceIdentifier=cluster_name+"-instance",
            DBInstanceClass="db.t2.small",
            Engine='aurora',
            DBClusterIdentifier=cluster_name,
            Tags=kwargs.get("Tags")
    )
    r= response['DBInstance']
    print r.get('DBInstanceStatus')

    return True

def modify_cluster_group(cluster,param_name):
    response = rds.modify_db_cluster(
    DBClusterIdentifier=cluster,
    ApplyImmediately=True,
    DBClusterParameterGroupName=param_name
    )
def create_snapshot(clusterIdentifier,name):
    response = rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=name,
                DBClusterIdentifier=clusterIdentifier
    )

    r= response['DBClusterSnapshot']
    print r.get('Status')
    return r.get('DBClusterSnapshotIdentifier')


def reboot(identifier):
    response = rds.reboot_db_instance(
        DBInstanceIdentifier=identifier

    )
    i= response['DBInstance']
    print i.get('DBInstanceStatus')


def get_most_reason_snapshot(clusterIdentifier):
    response = rds.describe_db_cluster_snapshots(
        DBClusterIdentifier=clusterIdentifier,
        SnapshotType='manual'

    )
    slist = response['DBClusterSnapshots']

    slist = list(filter(lambda x: x['DBClusterSnapshotIdentifier'].startswith('start-db'), slist))
    slist.sort(key=lambda k: k['SnapshotCreateTime'],reverse=True)
    return slist[0]
