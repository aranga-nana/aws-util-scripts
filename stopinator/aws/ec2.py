
import pytz
import boto3
import datetime
import time
import calendar

asgclient = boto3.client('autoscaling',region_name="ap-southeast-2")
ec2 = boto3.client('ec2',region_name="ap-southeast-2")

## stop instance
def suspend_asg(name):
    print "suspending asg:"+name
    response = asgclient.suspend_processes(
    	AutoScalingGroupName=name
    )
    print response

def resume_asg(name):
    print "Resuming asg :"+name
    response = asgclient.resume_processes(
    	AutoScalingGroupName=name,
    )
    print response
    print "remove schedule"
    response = asgclient.delete_tags(
    	Tags=[{
            "ResourceId": name,
            "ResourceType":"auto-scaling-group",
            "Key": CONST_ASG_RESUME_KEY,
        }]
    )
    print response

def generate_asg_instance(tz):
    asgList={"1":"name"}

    print "creating autoscaling instance map"

    response = asgclient.describe_auto_scaling_groups()

    #print response
    #nextToken = response['NextToken']
    asgs = response['AutoScalingGroups']
    for asg in asgs:
        name = asg['AutoScalingGroupName']
        tags = asg["Tags"]
        ## starting suspended asgs based on tiem
        v = get_time(CONST_ASG_RESUME_KEY,tags)
        if not v:
           print "no asg schedule(nothing to resume)"
        else:
           c = current_time(tz)
           if c[0] > v[0]:
              resume_asg(name)
           if c[0]==v[0] and c[1] >= v[1]:
              resume_asg(name)
        # end asg stuff

        #print asg['AutoScalingGroupName'],'\n'
        for instance in asg['Instances']:
            iid= instance['InstanceId']
            asgList[iid] = name
    return asgList

def stop_instance(instance,asglist,tz):
    iid = instance.get("InstanceId")
    current = current_time(tz)
    ec2.create_tags(Resources=[iid], Tags=[{"Key":"stopinator:stop:time","Value":current[2]}])
    if iid in asglist:
        print "has associated asg.need to suspend it first"
        asg = asglist[iid]
        suspend_asg(asg)
    print ec2.stop_instances(InstanceIds=[iid])



##start instanc
def start_instance(instance,asglist,tz):
    iid = instance.get("InstanceId")
    print ec2.start_instances(InstanceIds=[iid])
    ctime = current_time(tz)
    cdate = ctime[2].split("T")


    current = get_time("time:start",instance.get("Tags"))
    hh=""
    if current[0] < 10:
       hh="0"+`current[0]`
    else:
       hh=""+`current[0]`
    cdatetime = cdate[0]+"T"+hh+":"

    if current[1] < 10:
       cdatetime = cdatetime + "0"+`current[1]`
    else:
       cdatetime = cdatetime+`current[1]`

    ec2.create_tags(Resources=[iid], Tags=[{"Key":CONST_STOPINATOR_START_TIME,"Value":cdatetime}])
    #schedule asg resume if instance is part of ASG (need to make sure it resume after all the instance are on)
    if iid in asglist:
       name = asglist[iid];
       ctime[1] = ctime[1]+4
       lt = ""+`ctime[0]`+":"+`ctime[1]`
       print "asg schedule time:"+lt
       response = asgclient.create_or_update_tags(
    		Tags=[{
            	    "ResourceId": name,
                    "Key": CONST_ASG_RESUME_KEY,
                    "Value":lt,
                    "ResourceType":"auto-scaling-group",
                    "PropagateAtLaunch": False
               }]
       )
       print response
def list_instance(**kwargs):
    if not kwargs.get("Matcher") or len(kwargs.get("Matcher"))==0:
        r = ec2.describe_instances()
        return r
    filters = utils.instance_filter(kwargs.get("Matcher"))
    r = ec2.describe_instances(Filters=filters)
    return r
