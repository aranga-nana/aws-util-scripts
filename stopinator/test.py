from aws import utils,ec2,aurora
import datetime

mylist = [{"name":"acnonline.prod-appxx"},{"name":"acnonline.prod-cron"},{"name":"acnonline.dev-appxx"},{"name":"acnonline.test-appxx"},{"name":"linear.test-appxx"}]
print "Test pattern with *"
patterns=["acnonline*"]
print utils.pattern_filter(SourceList=mylist,Patterns=patterns,Key="name")

print "Test pattern with * -2"
patterns=["acnonline.prod*"]
print utils.pattern_filter(SourceList=mylist,Patterns=patterns,Key="name")


print "Test Weekend and check the mark as weekend=true"
current=[8,59,"2018-03-17T8:59"]
tags=[{"Key":"time:weekend","Value":"true"},{"Key":"time:start","Value":"8:29"}]
print utils.start_on_weekend(current,tags)

print "Test Weekend and check the mark as weekend=false"
tags=[{"Key":"time:weekend","Value":"false"},{"Key":"time:start","Value":"8:29"}]
current=[8,59,"2018-03-17T8:59"]
print utils.start_on_weekend(current,tags)

print "Test Weekend and missing tag weekend=true"
tags=[{"Key":"time:start","Value":"8:29"}]
current=[8,59,"2018-03-17T8:59"]
print utils.start_on_weekend(current,tags)

print "Test not a weekend"
tags=[{"Key":"time:weekend","Value":"false"},{"Key":"time:start","Value":"8:29"}]
current=[23,59,"2018-03-16T23:59"]
print utils.start_on_weekend(current,tags)


print "Test can start - success"
tags=[{"Key":"time:weekend","Value":"true"},{"Key":"time:start","Value":"8:00"},{"Key":"time:stop","Value":"8:50"}]
current=[8,1,"2018-03-16T8:1"]
print utils.can_start(current,tags)

print "Test can start already started"
tags=[{"Key":"time:weekend","Value":"true"},{"Key":"time:start","Value":"8:00"},{"Key":"time:stop","Value":"8:50"},{"Key":"stopinator:start:time","Value":"2018-03-16T8:05"}]
current=[8,1,"2018-03-16T8:10"]
print utils.can_start(current,tags)

print "Test can start - system started previous day "
tags=[{"Key":"time:weekend","Value":"true"},{"Key":"time:start","Value":"8:00"},{"Key":"time:stop","Value":"11:50"},{"Key":"stopinator:start:time","Value":"2018-03-16T8:05"}]
current=[8,1,"2018-03-17T12:10"] #current date
print utils.can_start(current,tags)


clusters = aurora.list_cluster()
#print clusters

current = utils.current_time("Australia/Melbourne")
print current
startlist = aurora.list_rds_schedule(StartTime=current[0],Matcher="stgy-test*")
print "start time", startlist
