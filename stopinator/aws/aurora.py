import time
import boto3
import utils

rds = boto3.client('rds',region_name="ap-southeast-2")
dynamodb = boto3.client('dynamodb',region_name="ap-southeast-2")
def get_table():
    table =None
    try:
        table = dynamodb.create_table(
            TableName='dev_aurora_metadata',
            KeySchema=[
                {
                    'AttributeName': 'cluster_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'TAGS',
                    'AttributeType': 'SS'
                },
                {
                    'AttributeName': 'start_time',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

    except Exception as e:
        table =  dynamodb.Table('dev_aurora_metadata')
        pass

    return table
def list_snapshots():
    response = rds.describe_db_cluster_snapshots(

        SnapshotType='manual'

    )
    return response['DBClusterSnapshots']

def start_aurora(snapshot):
    response = client.restore_db_cluster_from_snapshot(

        DBClusterIdentifier='bauuat-linearbpms-cluster',
        SnapshotIdentifier=snapshot,
        Engine='string',
        EngineVersion='string',
        Port=123,
        DBSubnetGroupName='bauuat-rdsstack-mysql-subnetgroup',
        DatabaseName='linearbpms',
        OptionGroupName='string',
        VpcSecurityGroupIds=[
            'bauuat-RdsStack-SecurityGroup-12121',
        ],
        Tags=[
            {
                'Key': 'stopinator:restore:status',
                'Value': 'false'
            },
        ]
    )

    print response
