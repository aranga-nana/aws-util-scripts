from datetime import datetime
import pytz
now = datetime.utcnow()
print  now.year, now.month, now.day, now.hour, now.minute
utc = pytz.utc
print utc.zone
eastern = pytz.timezone('US/Eastern')
print eastern.zone
amsterdam = pytz.timezone('Australia/NSW')
fmt = '%Y-%m-%d %H:%M:%S %Z%z'

loc_dt = amsterdam.localize(datetime(now.year, now.month, now.day, now.hour, now.minute, 0))
print loc_dt.strftime(fmt)

loc_dt = eastern.localize(datetime(now.year, now.month, now.day, now.hour, now.minute, 0))
print loc_dt.strftime(fmt)
