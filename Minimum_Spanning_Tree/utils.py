class Edge:
    def __init__(self,state = 0,weight = 0,node=None):
        self.state = state
        self.weight = weight
        self.node = node

    def get_node(self):
        return self.node
    
    def get_weight(self):
        return self.weight

    def get_state(self):
        return self.state
    
    def set_node(self,node):
        self.node = node
    
    def set_weight(self,weight):
        self.weight = weight

    def set_state(self,state):
        self.state = state


class Message:
    def __init__(self, id = 0, arglist = [], weight= 0):
        self.id = id
        self.arglist = arglist
        self.weight = weight

    def get_ID(self):
        return self.id
    
    def get_arglist(self):
        return self.arglist
        
    def get_weight(self):
        return self.weight

    def set_ID(self,id):
        self.id = id
    
    def set_arglist(self,arglist):
        self.arglist = arglist
        
    def set_weight(self,weight):
        self.weight =  weight
        

def createMessage(id=0,arglist = [], weight = 0):
    message = Message()
    message.set_ID(id)
    message.set_arglist(arglist)
    message.set_weight(weight)
    return message