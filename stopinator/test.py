from aws import utils

mylist = [{"name":"acnonline.prod-appxx"},{"name":"acnonline.prod-cron"},{"name":"acnonline.dev-appxx"},{"name":"acnonline.test-appxx"},{"name":"linear.test-appxx"}]
print "Test 1"
patterns=["acnonline*"]
print utils.pattern_filter(SourceList=mylist,Patterns=patterns,Key="name")

print "Test 2"
patterns=["acnonline.prod*"]
print utils.pattern_filter(SourceList=mylist,Patterns=patterns,Key="name")

print "Test 2"
patterns=["acnonline.prod-appxx","linear.test-appxx"]
print utils.pattern_filter(SourceList=mylist,Patterns=patterns,Key="name")
