import threading
import concurrent.futures
import sys
from Fragment import *
import time

# def create_adjacency_matrix(numNodes,edgeList):
#     adjacency_matrix = []
#     for i in range(numNodes):
#         row = []
#         for j in range(numNodes):
#             row.append(0)
#         adjacency_matrix.append(row)
    
#     for edge in edgeList:
#         adjacency_matrix[edge[0]][edge[1]] = edge[2]
#         adjacency_matrix[edge[1]][edge[0]] = edge[2]

#     return adjacency_matrix

def final_output(mst,edges):
    l = sorted(list(mst.keys()))
    for i in l:
        print("("+ str(edges[i][0])+", "+str(edges[i][1])+", "+str(i)+")")


def fetch_data(input_file):
    numNodes = 0
    neighbours = {}
    edgeList = {}
    numEdges = 0
    with open(input_file,'r') as f:
        edgeList = {}
        counter = 0
        for i in f.readlines():
            if counter == 0:
                numNodes = int(i.split('\n')[0])
                for i in range(numNodes):
                    neighbours[i] = []
            else:
                numEdges+=1
                j = i[1:].split(',')
                j = [int(j[0]),int(j[1]),int(j[2].split(')')[0])]
                edgeList[j[2]] = (j[0],j[1])
                neighbours[j[0]].append((j[1],j[2]))
                neighbours[j[1]].append((j[0],j[2]))
            counter+=1
    return numNodes,numEdges,neighbours,edgeList

def run_thread(node,numNodes):
    print("Starting")
    # print(threading.current_thread().getName())
    counter = 0
    global status
    while(len(mst)!=numNodes-1):
        # if counter >1000:
        #     break
        # print(status)
        node.readMessage()
        print(len(mst))
    global status
    status = "End"
    print("ending")



def main(arglist=[]):
    
    input_file = arglist[0]
    numNodes,numEdges,neighbours, edges = fetch_data(input_file)
    # adjacency_matrix = create_adjacency_matrix(numNodes,edges)
    nodes = []
    # weights = []
    for i in range(numNodes):
        new_node = Fragment(index=i)
        nodes.append(new_node)
    #     weights.append(0)
    
    # for i in range(numNodes):
    #     k = 0
    #     for j in range(numNodes):
    #         w = adjacency_matrix[i][j]
    #         if w != 0:
    #             weights[k] = w



    for i in range(numNodes):
        nodes[i].populate_edge_list(nodeList=neighbours[i],nodes=nodes)
        p = [i.node.args['index'] for i in nodes[i].edgeList]
        # print(p)
    # threads = []
    t_start = time.perf_counter()
    nodes[0].wakeup()
    global run
    run = 1
    # print(numNodes)

    with concurrent.futures.ThreadPoolExecutor(max_workers=numNodes) as executor:
        for node in nodes:
            executor.submit(run_thread,node,numNodes)

    t_end = time.perf_counter()

    # for i in range(numNodes):
    #     t = threading.Thread(target=run_thread,args=[nodes[i],numNodes])
    #     t.start()
    #     threads.append(t)

    # for t in threads:
    #     t.join()

    final_output(mst,edges)
    total_time = t_end - t_start
    print("Total time taken for execution: "+str(total_time)+" seconds")



if __name__ == "__main__":
    args = sys.argv
    main(args[1:])