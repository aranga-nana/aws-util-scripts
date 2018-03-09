import time
import boto3
import utils

CONST_TABLE="dev_aurora_db_meta"
rds = boto3.client('rds',region_name="ap-southeast-2")
dynamodb = boto3.client('dynamodb',region_name="ap-southeast-2")
def get_table():

    try:
        table = dynamodb.create_table(
            TableName=CONST_TABLE,
            KeySchema=[
                {
                    'AttributeName': 'cluster_name',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'time:start',
                    'KeyType': 'RANGE'
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

    except Exception as e:
        print e
        pass


def add_db_info(info):
    dynamodb = boto3.resource('dynamodb',region_name="ap-southeast-2")
    table = dynamodb.Table(CONST_TABLE)
    response = table.put_item(
            Item={
            "cluster_name" :"bauuat1-db",
            "time:start":"9:39",
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
            "time:start":"12:30"

        }
    )
    print response


def get_most_reason_snapshot(clusterIdentifier):
    response = rds.describe_db_cluster_snapshots(
        DBClusterIdentifier=clusterIdentifier,
        SnapshotType='manual'

    )
    slist = response['DBClusterSnapshots']

    slist = list(filter(lambda x: x['DBClusterSnapshotIdentifier'].startswith('start-db'), slist))
    slist.sort(key=lambda k: k['SnapshotCreateTime'],reverse=True)
    return slist[0]

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

        DBSnapshotIdentifier='start-db-acnonline-prod-2018-03-09-11-03',
        LicenseModel='string',
        Engine='aurora'

    )

    print response
