import re
import collections
from datetime import datetime

AllocationRequest           = collections.namedtuple    ('AllocationRequest'            ,     ['p', 'ts', 's'])
DeallocationRequest         = collections.namedtuple    ('DeallocationRequest'          ,     ['p', 'ts'])
DeallocationRequestWithSize = collections.namedtuple    ('DeallocationRequestWithSize'  ,     ['p', 'ts', 's'])
format = '%Y-%m-%d %H:%M:%S'

class GdkMalloc:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKmalloc\((\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2))
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class GdkZalloc:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKzalloc\((\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2))
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class GdkFree:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKfree\((\w+)\)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        ptr = int(matchObj.group(2), 0)

        result.append(DeallocationRequest(ptr, ts))

        return result

class GDKrealloc:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKrealloc\((\w+),(\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        old_ptr = int(matchObj.group(2), 0)
        s = int(matchObj.group(3))
        new_ptr = int(matchObj.group(4), 0)

        result.append(DeallocationRequest(old_ptr, ts))
        result.append(AllocationRequest(new_ptr, ts, s))

        return result

class GDKstrdup:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKstrdup\((\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2)) + 1
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class GDKstrndup:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKstrndup\((\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2)) + 1
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class GDKmmap:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKmmap\(.*,.*,(\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2))
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class GDKmunmap:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).*GDKmunmap\((\w+),(\d+)\).*'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        ptr = int(matchObj.group(2), 0)
        s = int(matchObj.group(3))

        result.append(DeallocationRequestWithSize(ptr, ts, s))

        return result

class malloc:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).* malloc\((\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        s = int(matchObj.group(2))
        ptr = int(matchObj.group(3), 0)

        result.append(AllocationRequest(ptr, ts, s))

        return result

class free:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).* free\((\w+)\)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        ptr = int(matchObj.group(2), 0)

        result.append(DeallocationRequest(ptr, ts))

        return result

class realloc:
    def build(line):
        trace_pattern = r'(\d+-\d+-\d+ \d+:\d+:\d+).* realloc\((\w+),(\d+)\) -> (\w+)'
        result = []

        matchObj = re.match(trace_pattern, line, re.A)
        if matchObj is None:
            return result

        ts = datetime.strptime(matchObj.group(1), format)
        old_ptr = int(matchObj.group(2), 0)
        s = int(matchObj.group(3))
        new_ptr = int(matchObj.group(4), 0)

        result.append(DeallocationRequest(old_ptr, ts))
        result.append(AllocationRequest(new_ptr, ts, s))

        return result

def factory(line):
        request = GdkMalloc.build(line)
        if request:
            return request
        request = GdkFree.build(line)
        if request:
            return request
        request = GDKrealloc.build(line)
        if request:
            return request
        request = GDKstrdup.build(line)
        if request:
            return request
        request = GDKstrndup.build(line)
        if request:
            return request
        request = GDKmmap.build(line)
        if request:
            return request
        request = GDKmunmap.build(line)
        if request:
            return request
        request = malloc.build(line)
        if request:
            return request
        request = free.build(line)
        if request:
            return request
        request = realloc.build(line)
        if request:
            return request
        else:
            return []
