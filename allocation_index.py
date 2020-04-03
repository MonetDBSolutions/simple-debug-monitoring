import collections
from deserializer import AllocationRequest, DeallocationRequest, DeallocationRequestWithSize, factory

class MultiAllocationIndex(dict):

    def insert(self, p, s):
        allocation_index = self.setdefault(p, collections.deque())
        allocation_index.append(s)

    def find_size(self, p):
        allocation_index = self.setdefault(p, collections.deque())

        if (len(allocation_index) == 0):
            return None

        return allocation_index.pop()

class Graph(dict):
    def __init__(self):
        self.t = list()
        self.c = list()

class AllocationIndex(dict):
    def __init__(self):
        self.free_recovery_index = MultiAllocationIndex()


    def insert(self, request):
        if type(request) is DeallocationRequest:
            s = self.free_recovery_index.find_size(request.p)
            if s is None:
                return
            delta = -1
        elif type(request) is AllocationRequest:
            p = request.p
            s = request.s
            self.free_recovery_index.insert(p, s)
            delta = +1
        elif type(request) is DeallocationRequestWithSize:
            p = request.p
            s = request.s
            # still have to remove a pointer from the free_recovery_index.
            # TODO: treat GDKmmap and GDKmalloc differently.
            self.free_recovery_index.find_size(request.p)
            delta = -1

        graph = self.setdefault(s,Graph())

        if len(graph.t):
            if graph.t[-1]  == request.ts:
                # group delta with accumulator of existing tick.
                graph.c[-1] = graph.c[-1] + delta
            if graph.t[-1]  < request.ts:
                graph.t.append(request.ts)
                graph.c.append(graph.c[-1] + delta)
            else:
                # timestamp is out of order. Just ignore it (for now)
                return
        else:
            graph.t.append(request.ts)
            graph.c.append(delta)

        if not hasattr(self, 'min_ts'):
            self.min_ts = request.ts
            self.max_ts = request.ts

        if self.min_ts > request.ts:
            self.min_ts = request.ts

        if self.max_ts < request.ts:
            self.max_ts = request.ts

def file_len(fname):
    with open(fnqame) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def create_graphs(TRACE_FILE):
    index = AllocationIndex()
    with open(TRACE_FILE, 'r') as trace_file:
        for line in trace_file:
            requests = factory(line)
            for request in requests:
                index.insert(request)

    return index
