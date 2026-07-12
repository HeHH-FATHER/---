#!/usr/bin/env python3
import redis, json, sys
from datetime import datetime

ods_files = int(sys.argv[1]) if len(sys.argv) > 1 else 400
dirty_users = int(sys.argv[2]) if len(sys.argv) > 2 else 0
dirty_files = dirty_users * 2

r = redis.Redis(host='Middleware', decode_responses=True)
now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
r.set('abyss:last_batch', now)
bn = r.incr('abyss:batch_count')
r.hincrby('abyss:file_stats', 'ods_total', ods_files)
r.hincrby('abyss:file_stats', 'dirty_total', dirty_files)
ods_t = int(r.hget('abyss:file_stats', 'ods_total') or 0)
dirty_t = int(r.hget('abyss:file_stats', 'dirty_total') or 0)
# 每批独立清洗率 + 累计清洗率
cr_per = round((ods_files - dirty_files) / ods_files * 100, 2) if ods_files > 0 else 0
cr_cum = round((ods_t - dirty_t) / ods_t * 100, 2) if ods_t > 0 else 0
batch = json.dumps({
    'batch': bn, 'time': now,
    'ods_files': ods_files, 'dirty_users': dirty_users,
    'dirty_files': dirty_files, 'clean_rate': cr_per,
    'cum_rate': cr_cum
})
r.lpush('abyss:batch_history', batch)
r.ltrim('abyss:batch_history', 0, 99)
print(f'Redis OK: batch={bn} ods={ods} dirty={dirty} rate={cr}%')
