import time
import boto3
import utils
from boto3.dynamodb.conditions import Key, Attr

CONST_TABLE="dev_aurora_db_meta"
rds = boto3.client('rds',region_name="ap-southeast-2")
dynamodb = boto3.resource('dynamodb',region_name="ap-southeast-2")

def init_table():

    try:
        dbclient = boto3.client('dynamodb',region_name="ap-southeast-2")
        table = dbclient.create_table(
            TableName=CONST_TABLE,
            KeySchema=[
                {
                    'AttributeName': 'cluster_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'cluster_name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits':2
            }
        )
        time.sleep(.500)
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
def delete(identifier):
    response = rds.delete_db_instance(
        DBInstanceIdentifier=identifier,
        SkipFinalSnapshot=True,

    )
    print response

def sync_metadata(cs):

    cluster_name = cs.get('DBClusterIdentifier')
    info = cs.get('InstanceInfo')
    hhmm = utils.get_time('time:stop',info.get('Tags'))
    sshh = utils.get_time('time:start',info.get('Tags'))
    tags = info.get('Tags')
    table = dynamodb.Table(CONST_TABLE)

    Found = False
    try:
        response = table.query( KeyConditionExpression=Key('cluster_name').eq(cluster_name))

    except Exception as e:
        pass

    item={
            "cluster_name" :cluster_name,
            "time_stop_hh":hhmm[0],
            "time_start_hh":sshh[0],
            "tags":tags,
            "cluster_parameter_group":cs.get('DBClusterParameterGroup'),
            "subnet_group":cs.get('DBSubnetGroup'),
            "option_group":"default:aurora-5-6",
            "security_group_name":"rds-launch-wizard",
            "db_instance_name":info.get('DBInstanceIdentifier'),
            "time:start":utils.get_tag_val('time:start',tags),
            "time:end":utils.get_tag_val('time:stop',tags)
    }
    if len(response.get('Items'))>0:
       item = response['Items'][0]
       item["cluster_name"] = cluster_name
       item["time_stop_hh"]=hhmm[0]
       item["tags"]=tags
       item["cluster_parameter_group"]=cs.get('DBClusterParameterGroup')
       item["subnet_group"] = cs.get('DBSubnetGroup')
       item["option_group"] = "default:aurora-5-6"
       item["security_group_name"] = "rds-launch-wizard"
       item["db_instance_name"] = info.get('DBInstanceIdentifier')
       item["time:start"] = utils.get_tag_val('time:start',tags)
       item["time:end"] = utils.get_tag_val('time:stop',tags)

    response = table.put_item(Item = item)
    time.sleep(.120)
def update_progress(item, **kwargs):
    snapshot = kwargs.get('SnapshotName')
    progress = kwargs.get('Progress')
    if not snapshot:
        print ""
    else:
        item['stopinator:snapshot'] = snapshot
    if not progress:
        print ""
    else:
        item['stopinator:progress'] = progress
    table = dynamodb.Table(CONST_TABLE)
    response = table.put_item(Item=item)

def list_rds_schedule(**kwargs):
    table = dynamodb.Table(CONST_TABLE)
    r =[]
    if kwargs.get('StopTime') >0:
        response = table.scan(
            FilterExpression=Attr('time_stop_hh').eq(kwargs.get('StopTime'))
        )
        r = response['Items']

    if kwargs.get('Deleted'):
        response = table.scan(
            FilterExpression=Attr('stopinator:progress').eq('mark-delete')
        )
        r = response['Items']

    if kwargs.get('StartTime') >0:
        response = table.scan(
            FilterExpression=Attr('time_start_hh').eq(kwargs.get('StartTime'))
        )
        r = response['Items']

    return r


def get_most_reason_snapshot(clusterIdentifier):
    response = rds.describe_db_cluster_snapshots(
        DBClusterIdentifier=clusterIdentifier,
        SnapshotType='manual'

    )
    slist = response['DBClusterSnapshots']

    slist = list(filter(lambda x: x['DBClusterSnapshotIdentifier'].startswith('start-db'), slist))
    slist.sort(key=lambda k: k['SnapshotCreateTime'],reverse=True)
    return slist[0]

def list_member_info(identifier):
    response = rds.describe_db_instances(DBInstanceIdentifier=identifier)
    if len(response['DBInstances'][0]) >0:
        return response['DBInstances'][0]
    return None


def list_cluster(**kwargs):
    response = None
    if not kwargs.get("ClusterIdentifier"):
        response = rds.describe_db_clusters()
    else:
        response = rds.describe_db_clusters(DBClusterIdentifier=kwargs.get("ClusterIdentifier"))

    res=[]
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
            #print tags
            if i['DBInstanceStatus'] == 'available':
                v = utils.get_tag_val('stopinator:restore:status',tags)
                if (v == 'false'):
                    c['AvailableUpdate'] = True

            #print c['InstanceInfo']
            c['InstanceInfo'] = extra
            res.append(c)

    #print r
    return res

def start_db(**kwargs):
    if not kwargs.get("SnapshotName"):
        return False
    if not kwargs.get("ClusterName"):
        return False
    if not kwargs.get("SubnetGroupName"):
        return False
    if not kwargs.get("Tags"):
        return False
    cluster_name = kwargs.get("ClusterName")
    snapshot =  kwargs.get("SnapshotName")

    response = rds.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=cluster_name,
            SnapshotIdentifier=snapshot,
            Engine='aurora',
            DBSubnetGroupName=kwargs.get("SubnetGroupName"),
            VpcSecurityGroupIds=[
                'sg-b208d4cb',
            ],
            Tags=kwargs.get("Tags")
    )
    print response
    print "================ cluster creation =============================="
    response = rds.create_db_instance(
            DBInstanceIdentifier=cluster_name+"-instance",
            DBInstanceClass="db.t2.small",
            Engine='aurora',
            DBClusterIdentifier=cluster_name,
            Tags=kwargs.get("Tags")
    )
    print response
    print "================ cluster instance creation =============================="
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

    print response


def reboot(identifier):
    response = rds.reboot_db_instance(
        DBInstanceIdentifier=identifier
)
