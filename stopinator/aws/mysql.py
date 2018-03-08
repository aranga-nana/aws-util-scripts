import boto3
import utils
rds = boto3.client('rds',region_name="ap-southeast-2")
filters = [{'Name':'tag:stopinator','Values':['true']}];
def list_mysql():
    list=[]
    response = rds.describe_db_instances()
    instances = response['DBInstances']
    for i in instances:
        arn =i['DBInstanceArn']
        print "STATUS", i['DBInstanceStatus']
        tags = rds.list_tags_for_resource(ResourceName=arn)
        print
        i['Tags']=tags['TagList']
        list.append(i)
    return list
def start_instance(dbinstance,tz):
    identifier = dbinstance['DBInstanceIdentifier']
    arn = dbinstance['DBInstanceArn']
    current = utils.current_time(tz)

    rds.add_tags_to_resource(ResourceName=arn,Tags=[{"Key":"stopinator:start:time","Value":current[2]}])
    response = rds.start_db_instance(
        DBInstanceIdentifier=identifier
    )


def stop_instance(dbinstance,tz):
    identifier = dbinstance['DBInstanceIdentifier']
    arn = dbinstance['DBInstanceArn']
    current = utils.current_time(tz);
    
    rds.add_tags_to_resource(ResourceName=arn,Tags=[{"Key":"stopinator:stop:time","Value":current[2]}])

    response = rds.stop_db_instance(
        DBInstanceIdentifier=identifier
    )
    print response
