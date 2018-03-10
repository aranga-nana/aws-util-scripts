import time
import boto3
import utils

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
                },
                {
                    'AttributeName': 'time_start_hh',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'cluster_name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'time_start_hh',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits':2
            }
        )

    except Exception as e:
        print e
        pass


def add_db_info(info):

    table = dynamodb.Table(CONST_TABLE)
    response = table.put_item(
            Item={
            "cluster_name" :"bauuat1-db",
            "time_start_hh":"9",
            "tags":{
                "Name":"bauuat-test",
                "Environment":"uat",
                "owner":"aranga@linear.com.au"
            },
            "parameter_group":"bauuat1-db-rdsdbclusterparametergroup-19uta94ubs4k5",
            "subnet_group":"bauuat-rdsstack-aurora-subnetgroup-a19289138",
            "option_group":"default:aurora-5-6",
            "security_group_name":"rds-launch-wizard",
            "cluster_name":"bau-aurora1-cluster",
            "db_instance_name":"bau-aurora1-instance1",
            "time:start":"12:30",
            "stopinator:time:start":None

        }
    )
    print response

def list_rds_schedule():
    table = dynamodb.Table(CONST_TABLE)
    table.query(
        KeyConditionExpression=Key('time:start').gt
    )

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



def list_cluster():
    response = rds.describe_db_clusters()
    c = response['DBClusters']
    res=[]
    for c in response['DBClusters']:
        m= c['DBClusterMembers'][0]
        #print m['DBInstanceIdentifier']

        i =list_member_info(m['DBInstanceIdentifier'])
        #print i
        extra={}
        extra['DBClusterParameterGroupStatus'] = m['DBClusterParameterGroupStatus']
        extra['Instance'] = i
        extra['Status'] = i['DBInstanceStatus']

        tags = rds.list_tags_for_resource(ResourceName=i['DBInstanceArn'])['TagList']
        c['Tags'] = tags
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

def start_db(snapshot,cluster_name):

    response = rds.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=cluster_name,
            SnapshotIdentifier=snapshot,
            Engine='aurora',
            DBSubnetGroupName='bauuat-rdsstack-mysql-subnetgroup',
            VpcSecurityGroupIds=[
                'sg-b208d4cb',
            ],
            Tags=[
                    {
                        'Key': 'stopinator:restore:status',
                        'Value': 'false'
                    },
                    {
                        'Key': 'Name',
                        'Value': cluster_name
                    },
                    {
                        'Key': 'snaphost:name',
                        'Value': snapshot
                    }
            ]
    )
    print response
    print "================ cluster creation =============================="
    response = rds.create_db_instance(
            DBInstanceIdentifier=cluster_name+"-instance",
            DBInstanceClass="db.t2.small",
            Engine='aurora',
            DBClusterIdentifier=cluster_name,
            Tags=[
                    {
                        'Key': 'stopinator:restore:status',
                        'Value': 'false'
                    },
                    {
                        'Key': 'Name',
                        'Value': cluster_name
                    },
                    {
                        'Key': 'snaphost:name',
                        'Value': snapshot
                    }
            ]
    )
    print response
    print "================ cluster instance creation =============================="

def modify_cluster_group(cluster,param_name):
    response = rds.modify_db_cluster(
    DBClusterIdentifier=cluster,
    ApplyImmediately=True,
    DBClusterParameterGroupName=param_name
)
