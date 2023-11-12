import sqlite3

# some constants for the program
n = 4 # number of tables
inf = 10**18 # greater than the maximum join output size
clouds = ['Azure', 'GCP']
tables = ['customer', 'orders', 'lineitem', 'part']
cloud_config = {'customer':'Azure', 'orders':'GCP', 'lineitem':'Azure', 'part':'GCP'}


# n*n matrix where join_size[i][j] contains the intermediate table size when joining tables from i to j
join_size = [[0 for _ in range(n)] for _ in range(n)]

# network transfer cost factor from one cloud to another
transfer_cost_factor = {}
transfer_cost_factor['Azure to Azure'] = 0
transfer_cost_factor['GCP to GCP'] = 0
transfer_cost_factor['Azure to GCP'] = 1
transfer_cost_factor['GCP to Azure'] = 1

# dp memtable
# Cost[i-j-c] : Cost to join tables from index i to j where the final join occurs in cloud environment c
Cost = dict()
for i in range(n):
    for j in range(n):
        for c in clouds:
            Cost[str(i) + '-' + str(j) + '-' + c] = -1


SuboptimalCosts = list()

SuboptimalCostsStates = list()

# Table containing child states for every state 
Child = dict()

# Connect to the SQLite database
conn = sqlite3.connect('TPC-H.db')  # Replace with your database file

# Create a cursor to execute SQL queries
cursor = conn.cursor()

query = """
SELECT count(*)
FROM customer
"""

cursor.execute(query)

results = cursor.fetchall()

# print("[customer] table size:", results[0][0])

join_size[0][0] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM orders
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[orders] table size:", results[0][0])

join_size[1][1] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM lineitem
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[lineitem] table size:", results[0][0])

join_size[2][2] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM part
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[part] table size:", results[0][0])

join_size[3][3] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM customer AS c
JOIN orders AS o ON c.c_custkey = o.o_custkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[customer joins orders] table size:", results[0][0])

join_size[0][1] = results[0][0]


query = """
SELECT count(*)
FROM orders AS o
JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[orders joins lineitem] table size:", results[0][0])

join_size[1][2] = results[0][0]

query = """
SELECT count(*)
FROM lineitem AS l
JOIN part AS p ON l.l_partkey = p.p_partkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[lineitem joins part] table size:", results[0][0])

join_size[2][3] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM customer AS c
JOIN orders AS o ON c.c_custkey = o.o_custkey
JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[customer joins orders joins lineitem] table size:", results[0][0])

join_size[0][2] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM orders AS o
JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey
JOIN part AS p ON l.l_partkey = p.p_partkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[orders joins lineitem joins part] table size:", results[0][0])

join_size[1][3] = results[0][0]

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM customer AS c
JOIN orders AS o ON c.c_custkey = o.o_custkey
JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey
JOIN part AS p ON l.l_partkey = p.p_partkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

# print("[customer joins orders joins lineitem joins part] table size:", results[0][0])

join_size[0][3] = results[0][0]

def networkCost(table_size, cloud_start, cloud_end):
    return table_size * transfer_cost_factor[cloud_start + ' to ' + cloud_end]

print("Our current config: ")
for i in range(n):
    print(tables[i] + " : " + cloud_config[tables[i]])
print("")

# Filling up the Cost mem table with base cases
for i in range(n):
    for c in clouds:
        Cost[str(i) + '-' + str(i) + '-' + c] = networkCost(join_size[i][i], cloud_config[tables[i]], c)

# Filling up the Child state table with base cases
for i in range(n):
    key = str(i) + '-' + str(i) + '-' + cloud_config[tables[i]]
    Child[key] = [key, key]
    for c in clouds:
        if c != cloud_config[tables[i]]:
            Child[str(i) + '-' + str(i) + '-' + c] = [key, key]

# dp Algorithm for determining the optimal join order cost
def CostofJoin(starttable, endtable, cloud, n):
    if Cost[str(starttable) + '-' + str(endtable) + '-' + cloud] != -1:
        return Cost[str(starttable) + '-' + str(endtable) + '-' + cloud]
    Cost[str(starttable) + '-' + str(endtable) + '-' + cloud] = inf
    for i in range(starttable, endtable):
        cost = inf
        child = ['', '']
        for c1 in clouds:
            for c2 in clouds:
                if c1 == c2 and c1 != cloud:
                    continue
                currCost = CostofJoin(starttable, i, c1, n) + CostofJoin(i+1, endtable, c2, n) + \
                           networkCost(join_size[starttable][i], c1, cloud) +  \
                           networkCost(join_size[i+1][endtable], c2, cloud)
                if starttable == 0 and endtable == n-1:
                    SuboptimalCosts.append(currCost)
                    SuboptimalCostsStates.append([str(starttable) + '-' + str(i) + '-' + c1, str(i+1) + '-' + str(endtable) + '-' + c2])
                if currCost < cost:
                    cost = currCost
                    child = [str(starttable) + '-' + str(i) + '-' + c1, str(i+1) + '-' + str(endtable) + '-' + c2]
        if cost < Cost[str(starttable) + '-' + str(endtable) + '-' + cloud]:
            Cost[str(starttable) + '-' + str(endtable) + '-' + cloud] = cost
            Child[str(starttable) + '-' + str(endtable) + '-' + cloud] = child     
    return Cost[str(starttable) + '-' + str(endtable) + '-' + cloud]


cost = 0
par = '' 
n = 4
cost1 = CostofJoin(0, n-1, 'Azure', n)
cost2 = CostofJoin(0, n-1, 'GCP', n)
if cost1 < cost2:
    cost = cost1
    par = str(0) + '-' + str(n-1) + '-' + 'Azure'
else:
    cost = cost2
    par = str(0) + '-' + str(n-1) + '-' + 'GCP'

print("Optimal Cost(Million rows transferred):",round(float(cost/1e6), 2))
print("")

levelOrder = list()
for i in range(100):
    levelOrder.append([])

levelOrder[0].append(par)

def levelOrderJoinTree(par, level):
    if par == Child[par][0]:
       return 
    levelOrder[level+1].append(Child[par][0])
    levelOrder[level+1].append(Child[par][1])
    levelOrderJoinTree(Child[par][0], level+1)
    levelOrderJoinTree(Child[par][1], level+1)

levelOrderJoinTree(par, 0)

levels = len(levelOrder)

print("Join Tree with corresponding cloud where operation is performed: ")

for level in levelOrder:
    if len(level) == 0:
        break
    for state in level:
        print(state, end=' ')
    print("")
print(" ")        

max_cost = max(SuboptimalCosts)

print("Worst case suboptimal cost(Million rows transferred):",round(float(max_cost/1e6),2))
print("")

print("Cost for different strategies(including suboptimal): ")
for i in range(len(SuboptimalCosts)):
    print("Join Cost(Million rows transferred) :",round(float(SuboptimalCosts[i]/1e6),2))
    # print("Join Tree in level Order:")
    # printLevelOrder()

# Close the cursor and the connection
cursor.close()
conn.close()
