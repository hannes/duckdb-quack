import subprocess
import os
import time
import math
import statistics

duckdb_binary = './build/release/duckdb'
print('method\tthreads\trows\tmedian_time_seconds')

for duckdb_socket in ['http://127.0.0.1:8080', 'https://127.0.0.1:8081']:
#, '/tmp/duckdb-rpc-socket']:
	server = subprocess.Popen([duckdb_binary, '-init', '/dev/null', '-cmd', f"CALL rpc_start('{duckdb_socket}')",  '-readonly', '~/nobackup/tpch-sf100.db'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
	# wait for socket to actually be open
	time.sleep(1)
	for threads in [1,2,4,8,10]:
		for rows in [int(math.pow(10,y)) for y in range(2, 8)] + [59986052]:
			timings = []
			for rep in range(3):
				start = time.time()
				client = subprocess.run([duckdb_binary,  '-init', '/dev/null',  '-c', f"SET enable_progress_bar = false; set threads={threads}; EXPLAIN ANALYZE FROM rpc_call('{duckdb_socket}','FROM lineitem LIMIT {rows}');"], stdout=subprocess.PIPE)
				timings.append(time.time() - start)
			median_time = round(statistics.median(timings), 3)
			print(f'{duckdb_socket}\t{threads}\t{rows}\t{median_time}')

	server.stdin.close()
	server.wait()