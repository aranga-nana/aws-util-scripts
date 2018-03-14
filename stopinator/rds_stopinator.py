from aws import mysql
from aws import utils


def lambda_handler(event, context):
    tz = utils.get_tz(event)
    pattern=utils.get_pattern(event)
    dbs = mysql.list_mysql(Matcher=pattern)
    for db in dbs:
        tags = db['Tags']
        identifier = db['DBInstanceIdentifier']
        status = db['DBInstanceStatus']
        st = utils.get_time('time:start',tags)
        et = utils.get_time('time:stop',tags)
        print "Analysing MYSQL Instance :",identifier
        print "current status:",status
        print "time:start (hh:mm) "+`st[0]`+":"+`st[1]`
        print "time:stop (hh:mm) "+`et[0]`+":"+`et[1]`
        stoped = False
        current = utils.current_time(tz)
        if status == 'available' and utils.can_stop(current,st,et):
            print "STOPING DB INSTANCE: "+identifier
            stoped =True
            mysql.stop_instance(db,tz)
        if status != 'available' and utils.can_start(current,st,et,tags):
            print "STARTING DB INSTANCE: "+identifier
            mysql.start_instance(db,tz)

    return "OK"
lambda_handler({}, {})
