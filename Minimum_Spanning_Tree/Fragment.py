import threading
import math

run = 0
mst = {}
mst_mutex = threading.Lock()
all_Edges = {}


class Edge:
    def __init__(self,state = 0,weight = 0,node=None):
        self.state = state
        self.weight = weight
        self.node = node

class Message:
    def __init__(self, id = 0, arglist = [], weight= 0):
        self.id = id
        self.arglist = arglist
        self.weight = weight

class Fragment:
    def __init__(self, numEdges = 0, weightList = [], nodeList = []):
        self.state = 0
        self.edgeList = []
        self.index = 0#
        self.id = math.inf#
        self.numEdges = numEdges
        for i in range(numEdges):
            e = Edge(state=0, weight= weightList[i], node = nodeList[i])
            self.edgeList.append(e)
        self.level = -1 #initially all the nodes are at level 0
        self.bestEdge = None
        self.bestWeight = math.inf
        self.testEdge = None
        self.parent = None
        self.findCount = -1
        self.message_queue = []
        self.mutex = threading.Lock()

    def addMessage(message):
        self.mutex.acquire()
        self.message_queue.append(message)
        self.mutex.release()
    
    def report():
        k = 0
        for i in range(self.numEdges):
            if self.edgeList[i].state == 1 and i!=self.parent:
                k+=1
        
        if self.findCount == k and self.testEdge == -1:
            self.state = 2
            message = Message(id=5, arglist=[self.bestWeight,None,None],weight=self.edgeList[self.parent].weight)
            self.edgeList[self.parent].node.addMessage(message)

    def test():
        min = math.inf
        min_ind = -1
        for i in range(self.numEdges):
            if self.edgeList[i].state == 0 and min > self.edgeList[i].weight:
                min = self.edgeList[i].weight
                min_ind = i
        
        if min_ind >=0: 
            self.testEdge = min_ind
            message = Message(id=2, arglist= [self.level, self.id, None], weight=self.edgeList[min_ind].weight)
            self.edgeList[min_ind].node.addMessage(message)
        else:
            self.testEdge = -1
            report()


    def connect_request(level, ind):
        if(level < self.level):
            self.edgeList[ind].state = 1
            message = Message(id=1, arglist=[self.level, self.id, self.state], weight=self.edgeList[j].weight)
            self.edgeList[ind].node.addMessage(message)

        elif self.edgeList[ind].state == 0:
            message = Message(id=0, arglist= [level,None,None], weight=self.edgeList[ind].weight)
            self.addMessage(message)

        else:
            message = Message(id=1, arglist=[self.level+1, self.edgeList[ind].weight,1], weight=self.edgeList[ind].weight)
            self.edgeList[ind].node.addMessage(message)


    def initiate(arglist = [], ind=0):
        self.level = arglist[0]
        self.id = arglist[1]
        self.state = arglist[2]
        self.parent = ind
        self.bestEdge = None
        self.bestWeight = math.inf
        for i in range(self.numEdges):
            if i != ind and self.edgeList[i].state == 1:
                message = Message(id=1, arglist=arglist, weight=self.edgeList[i].weight)
                self.edgeList[i].node.addMessage(message)

        if arglist[2] == 1:
            self.findCount = 0
            self.test()


    def test_message(arglist, ind):
        if arglist[0] < self.level:
            message = Message(id= 2, arglist=[arglist[0],arglist[1],None],weight=self.edgeList[ind].weight)
            self.addMessage(message)
        
        elif arglist[1] == self.id:
            if self.edgeList[ind].state == 0:
                self.edgeList[ind].state = 2
            if ind != self.testEdge:
                message = Message(id=4,weight=self.edgeList[ind].weight)
                self.edgeList[ind].node.addMessage(message)
            else:
                test()
        
        else:
            message = Message(id=3, weight= self.edgeList[ind].weight)
            self.edgeList[ind].node.addMessage(message)

    
    def accept(ind):
        self.testEdge = -1
        if self.edgeList[ind].weight < self.bestWeight:
            self.bestEdge = ind
            self.bestWeight = self.edgeList[ind].weight

        self.report()


    def reject(ind):
        if self.edgeList[ind].state == 0:
            self.edgeList[ind].state = 2
    
        self.test()


    def change_root():
        if self.edgeList[self.bestEdge].state == 1:
            message = Message(id=6,weight=self.edgeList[self.bestEdge].weight)
            self.edgeList[self.bestEdge].node.addMessage(message)
        else:
            message = Message(id=0, arglist=[self.level,None,None],weight=self.edgeList[self.bestEdge].weight)
            self.edgeList[self.bestEdge].node.addMessage(message)
            self.edgeList[self.bestEdge].state = 1
            mst_mutex.acquire()
            mst.add(self.edgeList[self.bestEdge].weight,1)
            mst_mutex.release()


    def print_output():
        print("Hi i am in print_output")


    def report_message(arglist, ind):
        if ind != self.parent:
            if arglist[0] < self.bestWeight:
                this.bestEdge = ind
                this.bestWeight = arglist[0]
            self.findCount +=1
            report()
        
        else:
            if self.state == 1:
                message = Message(id= 5, arglist=[arglist[0],None,None],weight=self.edgeList[ind].weight)
                self.addMessage(message)

            elif weight > self.bestWeight:
                self.change_root()
            
            elif weight == math.inf and self.bestWeight == math.inf:
                print_output()
                run = 0 #### See to this what is this 


    def change_root_message(ind):
        self.change_root()

    def readMessage():
        if len(self.message_queue) ==0:
            return 
        else:
            self.mutex.acquire()
            message = self.message_queue[0]
            self.message_queue.pop(0)
            i = 0
            while (self.edgeList[i].weight != message.weight):
                i+=1
            self.mutex.release()
            if self.state == 0:
                self.wakeup()
            
            if message.id == 0:
                self.connect_request(message[0],i)
            elif message.id == 1:
                self.initiate(message.arglist,i)
            elif message.id == 2:
                self.test_message(message.arglist,i)
            elif message.id == 3:
                self.accept(i)
            elif message.id == 4:
                self.reject(i)
            elif message.id == 5:
                self.report_message(message.arglist, i)
            elif message.id == 6:
                self.change_root_message(i)
            elif message.id == 7:
                self.wakeup()
            
