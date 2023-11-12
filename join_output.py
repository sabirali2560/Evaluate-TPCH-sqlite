import sqlite3

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

print("[customer] table size:", results[0][0])

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM orders
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

print("[orders] table size:", results[0][0])

# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM lineitem
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

print("[lineitem] table size:", results[0][0])


# Define the SQL query with the JOIN operations
query = """
SELECT count(*)
FROM part
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

print("[part] table size:", results[0][0])

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

print("[customer joins orders] table size:", results[0][0])


query = """
SELECT count(*)
FROM orders AS o
JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

print("[orders joins lineitem] table size:", results[0][0])

query = """
SELECT count(*)
FROM lineitem AS l
JOIN part AS p ON l.l_partkey = p.p_partkey
"""

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()

print("[lineitem joins part] table size:", results[0][0])

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

print("[customer joins orders joins lineitem] table size:", results[0][0])

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

print("[orders joins lineitem joins part] table size:", results[0][0])

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

print("[customer joins orders joins lineitem joins part] table size:", results[0][0])