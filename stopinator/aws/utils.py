#AWS LAmbda function to stop/start EC2 instance
#Note: this will disable Auto Scaling group
import pytz
import boto3
import datetime
import time
import calendar

CONST_KEY_TIME_START="time:start"
CONST_KEY_TIME_STOP="time:stop"

CONST_ASG_RESUME_KEY="stopinator:resume:time"
CONST_STOPINATOR_STOP_TIME="stopinator:stop:time"
CONST_STOPINATOR_START_TIME="stopinator:start:time"

#initalising
default_timezone = "Australia/NSW"
asgclient = boto3.client('autoscaling',region_name="ap-southeast-2")
ec2 = boto3.client('ec2',region_name="ap-southeast-2")


def get_hh_mm(str):
    r=[0,0]
    m= str.split(":")
    r[0] =int(m[0])
    r[1] =int(m[1])
    return r
def get_tz(event):
    tz=default_timezone
    #print event
    if not event:
        tz = default_timezone
    else:
        tz = event.get("timezone")
        if not tz:
            tz = default_timezone
    return tz

def get_pattern(event):
    p=None
    #print event
    if not event:
        p = None
    else:
        p = event.get("pattern")
        if not p:
            p = []
    print event
    print p
    return p
## get tag value by key
def get_tag_val(arg,tags):
    val = ""
    for x in tags:
       if x['Key'] == arg:
          val = x['Value']
    return val

def current_time(tz):
  current=[0,0,""]
  tz = default_timezone

  if not tz:
     tz = default_timezone
     print "Loading default timezone`"
  #print tz
  timezone = pytz.timezone (tz)

  datetime_with_tz = datetime.datetime.now(timezone)
  current[0] = datetime_with_tz.hour
  current[1] = datetime_with_tz.minute
  current[2] = datetime_with_tz.strftime("%Y-%m-%dT%H:%M")
  return current

## return true if not weekend
## return true tags configure to start on weekend
def start_on_weekend(current, tags):

    now = datetime.datetime.strptime(current[2], '%Y-%m-%dT%H:%M')
    weekend = now.weekday() == 5 or now.weekday() == 6
    print "Is Weekend",weekend
    if not weekend:
        return True
    if get_tag_val("time:weekend",tags) == "true":
        return True
    return False

## get tag value by key
def get_tag_val(arg,tags):
    val = ""
    for x in tags:
       if x['Key'] == arg:
          val = x['Value']
    return val
def update_tags(tags,tag):
    m = -1
    i =0
    for t in tags:
        if t['Key'] == tag.get('Key'):
            m = i
        i=i+1
    if (m == -1):
        tags.append(tag)
    else:
        tags[m] = tag

## get start/end time defined in ec2 tags
## time:start /time:stop
def get_time(arg,tags):
   v =""
   r=[111,211]
   for x in tags:
	if x['Key'] == arg:
	   v = x['Value']
           #print v
           timepart= v.split(':')
           r[0]= int(timepart[0])
           r[1]=int(timepart[1])

   return r


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


def can_start(current, tags):
  ch = current[0]
  cm = current[1]
  time_b = get_time(CONST_KEY_TIME_START,tags)
  time_e = get_time(CONST_KEY_TIME_STOP,tags)
  #dealing with manual stopping (make sure schedular not going to start it again)
  current_dtm = current[2]
  same_day = False
  last_start = get_tag_val(CONST_STOPINATOR_START_TIME,tags)

  print "Last:start:time",last_start

  if not last_start:
     same_day = False
  else:
     date = current_dtm.split("T")
     ls_date = last_start.split("T")
     print "last start date:"+ls_date[0]+", current date:"+date[0]
     if date[0] == ls_date[0]:
        #mark schedular is already started it.. cancel the starting operation if it see it should
        same_day = True

  can = False

  if ch > time_b[0] and ch < time_e[0]:
     #print "cond1-start"
     can = True

  if time_b[0] == ch and time_e[0] == ch and cm >= time_b[1]  and time_e[1] < cm:
     #print "cond2-start"
     can = True

  if time_b[0] == ch and cm >= time_b[1]:
     #print "cond3-start"
     can = True

  # check not same day
  can = can and not same_day
  # chek weekend
  can = can and start_on_weekend(current,tags)

  #print "can",can
  return can

def validate(pattern,key,x):
    if "*" in pattern:
        p = pattern.replace("*","")
        return x[key].startswith(p)
    return x[key] == pattern

def pattern_filter(**kwargs):
    result = []
    if not kwargs.get('SourceList'):
        return []

    if not kwargs.get('Matcher'):
        return kwargs.get('SourceList')
    if len(kwargs.get('Matcher')) == 0:
        return kwargs.get('SourceList')
    if not kwargs.get('Key'):
        return kwargs.get('SourceList')
    key = kwargs.get('Key')
    sourceList = kwargs.get('SourceList')
    patterns =  kwargs.get('Matcher')
    for p in patterns:
        res = list(filter(lambda x:validate(p,key,x),sourceList))
        if len(res) > 0:
            result.extend(res)
    return result

def can_stop(current,tags):
  time_b = get_time(CONST_KEY_TIME_START,tags)
  time_e = get_time(CONST_KEY_TIME_STOP,tags)
  can = False
  ch = current[0]
  cm = current[1]
  if ch > time_e[0]:
     can = True
  if time_e[0] == ch and cm >= time_e[1]:
     print "cond-2-stop"
     can = True

  return can


def instance_filter(pattern):
    df =["acn01*","acn-dev*"]

    filters = [{'Name':'tag:stopinator','Values':['true']}]
    if not pattern:
        print "loading default pattern ",df
        pattern =df;
    else:
        print "loading pattern :",pattern

    f={'Values':pattern,'Name':'tag:Name'}

    filters.append(f)
    return filters
