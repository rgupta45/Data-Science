from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("DegreeOfSeperationsSelf")
sc = SparkContext(conf = conf)
startCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)

def startRdd():
    inputfile = sc.textFile("/Users/rohan.gupta/Desktop/SParkCourse/Marvel-Graph.txt")
    return inputfile.map(convertBfsMap)

def convertBfsMap(lines):
    fileds = lines.split()
    charecterId = fileds[0]
    associated = [int(i) for i in fileds[1:]]
    color = "WHITE"
    distance = 999999
    if charecterId == startCharacterID:
        color = "GRAY"
        distance = 0
    return (charecterId, (associated, distance, color))
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results

'''
def bfsMap(node):
    charecterId = node[0]
    data = node[1]
    connections, distance, color = data[0], data[1], data[2]
    result = []
    
    if color == "GRAY":
        for connection in connections:
            newCharecterId= connection
            newDistance = distance + 1
            newColor = "GRAY"
            if int(newCharecterId) == targetCharacterID:
                hitCounter.add(1)
            newEntry = (newCharecterId,([],newDistance,newColor))
            result.append(newEntry)
        color = "BLACK"
    result.append((charecterId,(connections,distance,color)))
    return result
'''
def bfsReduce(data1 , data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    distance = 999999
    color = color1
    edges = []
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)
    if (distance1 < distance):
        distance = distance1
    if (distance2 < distance):
        distance = distance2
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)

iterationRdd = startRdd()
for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))
    mapped = iterationRdd.flatMap(bfsMap)
    print("Processing " + str(mapped.count()) + " values.")
    print("<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break
    iterationRdd = mapped.reduceByKey(bfsReduce)
