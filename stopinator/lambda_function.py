#AWS LAmbda function to stop/start EC2 instance
#Note: this will disable Auto Scaling group
import pytz
import boto3
import datetime
import time
import calendar
from aws import utils,ec2




CONST_TZ = "Australia/NSW"

#asgclient = boto3.client('autoscaling',region_name="ap-southeast-2")
#ec2 = boto3.client('ec2',region_name='ap-southeast-2')





## load auto scaling load to a map



def lambda_handler(event, context):
    tz = CONST_TZ
    print("event content:",event)
    pattern = ["122acn.*"]
    asg_instance = ec2.generate_asg_instance(tz)
    if not event:
        print "Loading default"
    else:
        tz = event.get('timezone')
        pattern = event.get("pattern")
        if not tz:
            tz = CONST_TZ
            print "loading default timezone:"+tz

        if not pattern:
            pattern =["unspec.*"]



    current = utils.current_time(tz)

    #filters = utils.instance_filter(pattern)
    #print "Using filters",filters
    reservations=ec2.list_instances(Matcher=pattern)
    print "found  "+`len(reservations['Reservations'])`+" Reservation matches"

    for r in reservations['Reservations']:
        #print(r,"\n\n")
        #print(r,"=======================================")
        for i in r['Instances']:
            iid=i['InstanceId']
            print('checking instance id',iid)
            date= i.get('LaunchTime')
            tags =i['Tags']
            print "analysing instance :"+`iid`+" - "+utils.get_tag_val("Name",tags)

            ## start /end time extraction from tags
            time_e = utils.get_time(utils.CONST_KEY_TIME_STOP,tags)
            time_b = utils.get_time(utils.CONST_KEY_TIME_START,tags)
            tt = current[2].split("T")
            print "current time :"+tt[1]+"("+tz+")"

            print "time:stop  (HH:mm)"+`time_e[0]`+":"+`time_e[1]`
            print "time:start (HH:mm)"+`time_b[0]`+":"+`time_b[1]`
            stated = i['State']
            stateId = stated.get('Code')
            print "current instance SateId:"+`stateId`
            executeStop = False
            #stop condition
            if stateId == 16:
               if utils.can_stop(current,tags):
                  print "STOPING INSTANCE",iid
                  #update_stopinator_status(iid,tz,tags)
                  print "hello"
                  ec2.stop_instance(i,asg_instance,tz)
                  executeStop = True
            #start condition
            if not executeStop:
               print "trying to start"
               if stateId == 80:
                  print "Instance stateId:"+`stateId`
                  if utils.can_start(current,tags):
                     print "STARTING INSTANCE:"+iid
                     ec2.start_instance(i,asg_instance,tz)

    return "OK"
