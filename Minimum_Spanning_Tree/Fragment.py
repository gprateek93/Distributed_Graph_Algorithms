import threading
import math
import sys
from utils import *

global run
global mst
global mst_mutex
global all_edges
global status

status = "Start"

run = 0
mst = {}
mst_mutex = threading.Lock()
global_mutex = threading.Lock()
all_Edges = {}


class Fragment():
    def __init__(self, index = 0):
        # self.state = 0
        self.edgeList = []
        # self.index = index
        # self.id = math.inf
        # self.level = -1 
        # self.bestEdge = -1
        # self.bestWeight = math.inf
        # self.testEdge = -1
        # self.parent = -1
        # self.findCount = -1
        self.message_queue = []
        self.mutex = threading.Lock()
        self.args = {'state':0 , 'index':index, 'id':math.inf, 'level':-1,'parent':-1}
        self.temp = {'bestEdge':-1,'bestWeight':math.inf,'testEdge':-1,'rec':-1}

    
    def populate_edge_list(self,nodeList=[],nodes=[]):
        # self.numEdges = len(nodeList)
 

        self.args['numEdges'] = len(nodeList)
        for i in range(len(nodeList)):
            e = Edge(state=0, weight= nodeList[i][1], node = nodes[nodeList[i][0]])
            self.edgeList.append(e)
    
    def add_message(self,message):
        #print("adding message")
        self.mutex.acquire()
        self.message_queue.append(message)
        self.mutex.release()
    
    def report(self):
        #print("reporting")
        k = 0
        i = 0
        while i < self.args['numEdges'] and status == "Start":
            # if self.edgeList[i].state == 1 and i!=self.parent:
            if self.edgeList[i].get_state() == 1 and i!=self.args['parent'] and status == "Start":
                k+=1
            i+=1
        
        # if self.findCount == k and self.testEdge == -1:
        if self.temp['rec'] == k and self.temp['testEdge'] == -1 and status == "Start":
            # self.state = 2
            self.args['state'] = 2
            # message = Message(id=5, arglist=[self.bestWeight,None,None],weight=self.edgeList[self.parent].weight)
            # self.edgeList[self.parent].node.add_message(message)
            # message = createMessage(id=5, arglist=[self.temp['bestWeight'],None,None],weight=self.edgeList[self.args['parent']].weight)
            self.edgeList[self.args['parent']].get_node().add_message(createMessage(id=5, arglist=[self.temp['bestWeight'],None,None],weight=self.edgeList[self.args['parent']].weight))

    def test(self):
        #print("testing")
        min = math.inf
        min_ind = -1
        i = 0
        while i < self.args['numEdges'] and status == "Start":
 

            if self.edgeList[i].get_state() == 0 and min > self.edgeList[i].weight and status == "Start":
                min = self.edgeList[i].weight
                min_ind = i
            i+=1
        
        if min_ind >=0 and status == "Start": 
            # self.testEdge = min_ind
            self.temp['testEdge'] = min_ind
            # message = Message(id=2, arglist= [self.level, self.id, None], weight=self.edgeList[min_ind].weight)
 

            # message = createMessage(id=2, arglist= [self.args['level'], self.args['id'], None], weight=self.edgeList[min_ind].weight)
            self.edgeList[min_ind].get_node().add_message(createMessage(id=2, arglist= [self.args['level'], self.args['id'], None], weight=self.edgeList[min_ind].weight))
        else:
            # self.testEdge = -1
            self.temp['testEdge'] = -1
            self.report()


    def connect_request(self,arglist, ind):
        #print("Establishing connect request")
        #print(level,ind,self.level)
        # if(level < self.level):
        level = arglist[0]
        if(level < self.args['level']) and status == "Start":
            self.edgeList[ind].set_state(1)
            # message = Message(id=1, arglist=[self.level, self.id, self.state], weight=self.edgeList[ind].weight)
 

            # message = createMessage(id=1, arglist=[self.args['level'], self.args['id'], self.args['state']], weight=self.edgeList[ind].weight)
            self.edgeList[ind].get_node().add_message(createMessage(id=1, arglist=[self.args['level'], self.args['id'], self.args['state']], weight=self.edgeList[ind].weight))

        elif self.edgeList[ind].get_state() == 0 and status == "Start":
            # message = createMessage(id=0, arglist= [level,None,None], weight=self.edgeList[ind].weight)
            self.add_message(createMessage(id=0, arglist= [level,None,None], weight=self.edgeList[ind].weight))

        else:
            # message = Message(id=1, arglist=[self.level+1, self.edgeList[ind].weight,1], weight=self.edgeList[ind].weight)
            # message = createMessage(id=1, arglist=[self.args['level']+1, self.edgeList[ind].weight,1], weight=self.edgeList[ind].weight)
 

            self.edgeList[ind].get_node().add_message(createMessage(id=1, arglist=[self.args['level']+1, self.edgeList[ind].weight,1], weight=self.edgeList[ind].weight))


    def initiate(self,arglist = [], ind=0):
        #print("Initiating")
        # self.level = arglist[0]
        self.args['level'] = arglist[0]
        # self.id = arglist[1]
        self.args['id'] = arglist[1]
        # self.state = arglist[2]
        self.args['state'] = arglist[2]
        # self.parent = ind
        self.args['parent'] = ind
        # self.bestEdge = -1
        self.temp['bestEdge'] = -1
        # self.bestWeight = math.inf
        self.temp['bestWeight'] = math.inf
        i = 0
 

        while i < self.args['numEdges'] and status == 'Start':
            if i != ind and self.edgeList[i].get_state() == 1 and status == 'Start':
                # message = createMessage(id=1, arglist=arglist, weight=self.edgeList[i].weight)
                self.edgeList[i].get_node().add_message(createMessage(id=1, arglist=arglist, weight=self.edgeList[i].weight))
            i+=1

        if arglist[2] == 1 and status == "Start":
            # self.findCount = 0
            self.temp['rec'] = 0
            self.test()


    def test_message(self,arglist, ind):
        #print("servicing the test message")
        # if arglist[0] > self.level:
        if arglist[0] > self.args['level'] and status == "Start":
 

            # message = createMessage(id= 2, arglist=[arglist[0],arglist[1],None],weight=self.edgeList[ind].weight)
            self.add_message(createMessage(id= 2, arglist=[arglist[0],arglist[1],None],weight=self.edgeList[ind].weight))
        
        # elif arglist[1] == self.id:
        elif arglist[1] == self.args['id'] and status == "Start":
 

            if self.edgeList[ind].get_state() == 0:
                self.edgeList[ind].set_state(2)
            # if ind != self.testEdge:
            if ind != self.temp['testEdge'] and status == "Start":
 

                # message = createMessage(id=4,weight=self.edgeList[ind].weight)
                self.edgeList[ind].get_node().add_message(createMessage(id=4,weight=self.edgeList[ind].weight))
            else:
                self.test()
        
        else:
            # message = createMessage(id=3, weight= self.edgeList[ind].weight)
            self.edgeList[ind].get_node().add_message(createMessage(id=3, weight= self.edgeList[ind].weight))

    
    def accept(self,ind):
        #print("accepted")
        # self.testEdge = -1
        self.temp['testEdge'] = -1
        # if self.edgeList[ind].weight < self.bestWeight:
        if self.edgeList[ind].weight < self.temp['bestWeight'] and status == "Start":
            # self.bestEdge = ind
            self.temp['bestEdge'] = ind
            # self.bestWeight = self.edgeList[ind].weight
            self.temp['bestWeight'] = self.edgeList[ind].weight

        self.report()


    def reject(self,ind):
        #print("Rejected")
 

        if self.edgeList[ind].get_state() == 0 and status == "Start":
            self.edgeList[ind].set_state(2)
    
        self.test()


    def change_root(self):
        #print("Inside change root")
        # if self.edgeList[self.bestEdge].state == 1:
        #     message = Message(id=6,weight=self.edgeList[self.bestEdge].weight)
        #     self.edgeList[self.bestEdge].node.add_message(message)
        if self.edgeList[self.temp['bestEdge']].get_state() == 1 and status == "Start":
            # message = createMessage(id=6,weight=self.edgeList[self.temp['bestEdge']].weight)
            self.edgeList[self.temp['bestEdge']].get_node().add_message(createMessage(id=6,weight=self.edgeList[self.temp['bestEdge']].weight))
        else:
            # message = Message(id=0, arglist=[self.level,None,None],weight=self.edgeList[self.bestEdge].weight)
            # message = createMessage(id=0, arglist=[self.args['level'],None,None],weight=self.edgeList[self.temp['bestEdge']].weight)
            # self.edgeList[self.bestEdge].node.add_message(message)
            # self.edgeList[self.bestEdge].state = 1
            # mst_mutex.acquire()
            # mst[self.edgeList[self.bestEdge].weight] = 1
            # mst_mutex.release()
            self.edgeList[self.temp['bestEdge']].get_node().add_message( createMessage(id=0, arglist=[self.args['level'],None,None],weight=self.edgeList[self.temp['bestEdge']].weight))
            self.edgeList[self.temp['bestEdge']].set_state(1)
            mst_mutex.acquire()
            mst[self.edgeList[self.temp['bestEdge']].weight] = 1
            mst_mutex.release()


    def print_output(self):
        print("Hi i am in ##print_output")


    def report_message(self,arglist, ind):
        #print("Servicing the report message")
        # if ind != self.parent:
        if ind != self.args['parent'] and status == "Start":
            # if arglist[0] < self.bestWeight:
            if arglist[0] < self.temp['bestWeight'] and status == "Start":
                # self.bestEdge = ind
                self.temp['bestEdge'] = ind
                # self.bestWeight = arglist[0]
                self.temp['bestWeight'] = arglist[0]
            # self.findCount +=1
            self.temp['rec'] +=1
            self.report()
        
        else:
            weight = arglist[0]
            #print(weight,self.bestWeight)
            # if self.state == 1:
 

            if self.args['state'] == 1 and status == "Start":
                # message = createMessage(id= 5, arglist=[weight,None,None],weight=self.edgeList[ind].weight)
                self.add_message(createMessage(id= 5, arglist=[weight,None,None],weight=self.edgeList[ind].weight))

            # elif weight > self.bestWeight:
            elif weight > self.temp['bestWeight'] and status == "Start":
                self.change_root()
            
            # elif weight == math.inf and self.bestWeight == math.inf:
            elif weight == math.inf and self.temp['bestWeight'] == math.inf and status == "Start":
                self.print_output()
                #print("reached end")
                global run
                run = 0


    def change_root_message(self):
        #print("Servicing the change root message")
        self.change_root()

    
    def find_minimum_edge(self):
        #print("Getting into to find the minimum edge")
        min = math.inf
        index = 0
        i = 0
        while i < self.args['numEdges'] and status == "Start":
            if self.edgeList[i].weight < min and status == "Start":
                min = self.edgeList[i].weight
                index = i
            i+=1

        return index


    def wakeup(self):
        #print("Inside wakeup call")
 

        minEdge = self.find_minimum_edge()
        self.edgeList[minEdge].set_state(1)
        mst_mutex.acquire()
        mst[self.edgeList[minEdge].weight] = 1
        mst_mutex.release()
        # self.level = 0
        self.args['level'] = 0
        # self.state = 2
        self.args['state'] = 2
        # self.findCount = 0
        self.temp['rec'] = 0
        # message = createMessage(id=0,arglist=[0,None,None],weight= self.edgeList[minEdge].weight)
        #print(self.edgeList[minEdge].node.index)
        self.edgeList[minEdge].get_node().add_message(createMessage(id=0,arglist=[0,None,None],weight= self.edgeList[minEdge].weight))

    def readMessage(self):
        if len(self.message_queue) ==0 and status == "Start":
            return 
        else:
            #print("inside read_message")
            self.mutex.acquire()
            message = self.message_queue[0]
            i = 0
            while (i<self.args['numEdges'] and self.edgeList[i].weight != message.get_weight()) and status == "Start":
                i+=1
            self.message_queue.pop(0)
            self.mutex.release()
            #print(message.id)
            #print(i)
            # if self.state == 0:
            if self.args['state'] == 0 and status == "Start":
                self.wakeup()

            if message.get_ID() == 0 and status == "Start":
                self.connect_request(message.get_arglist(),i)
 

            elif message.get_ID() == 1 and status == "Start":
                self.initiate(message.get_arglist(),i)
            elif message.get_ID() == 2 and status == "Start":
                self.test_message(message.get_arglist(),i)
            elif message.get_ID() == 3 and status == "Start":
                self.accept(i)
            elif message.get_ID() == 4 and status == "Start":
                self.reject(i)
            elif message.get_ID() == 5 and status == "Start":
                self.report_message(message.get_arglist(), i)
            elif message.get_ID() == 6 and status == "Start":
                self.change_root_message()
            elif message.get_ID() == 7 and status == "Start":
                self.wakeup()