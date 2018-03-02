#AWS LAmbda function to stop/start EC2 instance
#Note: this will disable Auto Scaling group
import pytz
import boto3
import datetime
import time
import calendar


#TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


#initalising
aus = pytz.timezone ("Australia/NSW")
d={"1":"name"}

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
def stopInstance(ec2,iid):
    #print('stopiing instance ',iid)
    print ec2.stop_instances(InstanceIds=[iid])

##start instance
def startInstance(ec2,iid):
    print ec2.start_instances(InstanceIds=[iid])

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
  if ch > time_b[0] and ch <= time_e[0]:
     print "cond1-start"
     can = True
     
  if time_b[0] == ch and time_e[0] == ch and cm >= time_b[1]  and time_e[1] < cm:
     print "cond2-start"
     can = True
     
  if time_b[0] == ch and time_b[1] >= cm:
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
 
 


def lambda_handler(event, context):
    
    print("event content:",event)
    initaliseall()
    #curent time calc (diffrent time zone support)
    datetime_with_tz = datetime.datetime.now(aus) # No daylight saving time
    ch= datetime_with_tz.hour
    cm= datetime_with_tz.minute
    print "AWS Current Time (HH:MM)",ch,";",cm
    


    ec2 = boto3.client('ec2',region_name='ap-southeast-2')
    filters = [{'Name': 'tag:Name', 'Values': ['lin*'] },{'Name':'tag:stopinator','Values':['true']}]
    reservations=ec2.describe_instances(Filters=filters)
    for r in reservations['Reservations']:
        #print(r,"\n\n")
        #print(r,"=======================================")
        for i in r['Instances']:
            iid=i['InstanceId']
            print('checking instance id',iid)
            date= i.get('LaunchTime')
            tags =i['Tags']
            print "analysing instance :",iid," -",get_tag_val("Name",tags)

            ## start /end time extraction from tags
            time_e = start_end_time('time:stop',tags)
            time_b = start_end_time('time:start',tags)
            
            print "time:stop (HH:mm)",time_e[0],";",time_e[1]
            stated = i['State']
            stateId = stated.get('Code')
            print stateId
            executeStop = False
            #stop condition
            if stateId == 16 or stateId == 0:
               if can_stop(ch,cm,time_b,time_e):
                  print "STOPING INSTANCE",iid
                  if iid in d:
                     suspendAsg(d[iid])
                     time.sleep(0.300)
                  stopInstance(ec2,iid)
                  executeStop = True
            #start condition
            print "time:start (HH:mm)",time_b[0],";",time_b[1]
            if not executeStop:
               if stateId == 80:
                  print "stateID",stateId 
                  if can_start(ch,cm,time_b,time_e):
                     print "STARTING INSTANCE",iid," TIME ",datetime_with_tz
                     startInstance(ec2,iid)
                             
    return "OK"

