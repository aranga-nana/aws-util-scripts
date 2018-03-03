#AWS LAmbda function to stop/start EC2 instance
#Note: this will disable Auto Scaling group
import pytz
import boto3
import datetime
import time
import calendar


#TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


#initalising
default_timezone = "Australia/NSW"


d={"1":"name"}
def current_time(event):
  current=[0,0,""]
  tz = default_timezone

  if not event:
     tz = default_timezone
     print "Loading default timezone`"
  else:
     tz = event.get("timezone")
     if not tz:
        tz = default_timezone
        print "Loading default timezone "+default_timezone

  print tz
  timezone = pytz.timezone (tz)

  datetime_with_tz = datetime.datetime.now(timezone)
  current[0] = datetime_with_tz.hour
  current[1] = datetime_with_tz.minute
  current[2] = datetime_with_tz.strftime("%Y-%M-D %h:%m")
  return current


## get tag value by key
def get_tag_val(arg,tags):
    val = ""
    for x in tags:
       if x['Key'] == arg:
          val = x['Value']
    return val
 
## get start/end time defined in ec2 tags
## time:start /time:stop 
def start_end_time(arg,tags):
   v =""
   
   
   r=[111,211]
   
   for x in tags:
	if x['Key'] == arg:
	   v = x['Value']
           print v
           timepart= v.split(':')
           r[0]= int(timepart[0])
           r[1]=int(timepart[1])

   return r


## stop instance
def stop_instance(ec2,instance,event):
    iid = instance.get("InstanceId")
    #print('stopiing instance ',iid)
    print ec2.stop_instances(InstanceIds=[iid])
    current = current_time(event)
     
    ec2.create_tag(Resources[iid],Tags=[{"Key":"stopinator:stop:time","Value":current[2]}]) 

##start instanc
def start_instance(ec2,instance,event):
    iid = instance.get("InstanceId")
    print ec2.start_instances(InstanceIds=[iid])

    current = current_time(event)
    ec2.create_tag(Resources=[iid], Tags=[{"Key":"stopinator:start:time","Value":current[2]}])

## load auto scaling load to a map
def initaliseall():


    print "loading autoscale groups"
    client = boto3.client('autoscaling',region_name='ap-southeast-2')
    response = client.describe_auto_scaling_groups()

    #print response
    #nextToken = response['NextToken']
    asgs = response['AutoScalingGroups']
    for asg in asgs:
        #print asg['AutoScalingGroupName'],'\n'
        for instance in asg['Instances']:
            iid= instance['InstanceId']
            d[iid] = asg['AutoScalingGroupName']




def can_start(ch,cm, time_b,time_e):
 
  can = False
  print "ch",ch,"cm",cm,time_b,time_e
  if ch > time_b[0] and ch < time_e[0]:
     print "cond1-start"
     can = True
     
  if time_b[0] == ch and time_e[0] == ch and cm >= time_b[1]  and time_e[1] < cm:
     print "cond2-start"
     can = True
     
  if time_b[0] == ch and cm >= time_b[1]:
     print "cond3-start"
     can = True
  
  return can  



     
def can_stop(ch,cm,time_b,time_e):

  can = False

  if ch > time_e[0]:
     can = True
  if time_e[0] == ch and cm >= time_e[1]:
     can = True

  return can   


def instance_filter(event):
    df =["linear*","linear-dev*"]
    pattern = df
    filters = [{'Name':'tag:stopinator','Values':['true']}]
    if not event:
       print "loading default pattern ",f
    else:
        pattern = event.get("target")
        if not pattern:
           pattern = df
    f={'Values':pattern,'Name':'tag:Name'}   
   
    filters.append(f)     
    return filters;
 
def lambda_handler(event, context):
    
    print("event content:",event)
     
    initaliseall()
    #curent time calc (diffrent time zone support)
    current = current_time(event)
    
    ch = current[0]
    cm = current[1]
    awstime = "Execute Time: "+`ch`+":"+`cm`+" hours."

    ec2 = boto3.client('ec2',region_name='ap-southeast-2')
    filters = instance_filter(event)
    print "Using filters",filters
    reservations=ec2.describe_instances(Filters=filters)
    print "found  "+`len(reservations['Reservations'])`+" Reservation matches"

    for r in reservations['Reservations']:
        #print(r,"\n\n")
        #print(r,"=======================================")
        for i in r['Instances']:
            iid=i['InstanceId']
            print('checking instance id',iid)
            date= i.get('LaunchTime')
            tags =i['Tags']
            print "analysing instance :"+`iid`+" - "+get_tag_val("Name",tags)

            ## start /end time extraction from tags
            time_e = start_end_time('time:stop',tags)
            time_b = start_end_time('time:start',tags)
            print awstime           
            print "time:stop  (HH:mm)"+`time_e[0]`+":"+`time_e[1]`
            print "time:start (HH:mm)"+`time_b[0]`+":"+`time_b[1]` 
            stated = i['State']
            stateId = stated.get('Code')
            print "current instance SateId:"+`stateId`
            executeStop = False
            #stop condition
            if stateId == 16:
               if can_stop(ch,cm,time_b,time_e):
                  print "STOPING INSTANCE",iid
                  if iid in d:
                     suspendAsg(d[iid])
                     time.sleep(0.300)
                  stop_instance(ec2,i,event)
                  executeStop = True
            #start condition
            if not executeStop:
               if stateId == 80:
                  print "stateID",stateId 
                  if can_start(ch,cm,time_b,time_e):
                     print "STARTING INSTANCE:"+iid
                     start_instance(ec2,i,event)
                             
    return "OK"

