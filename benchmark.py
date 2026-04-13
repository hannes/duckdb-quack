import subprocess
import os
import time
import math
import statistics

duckdb_binary = './build/release/duckdb'
print('method\tthreads\trows\tmedian_time_seconds')

for duckdb_socket in ['quack:localhost:1234']:
#, '/tmp/duckdb-rpc-socket']:
	server = subprocess.Popen([duckdb_binary, '-init', '/dev/null', '-cmd', f"set rpc_default_token='asdf'; CALL rpc_start('{duckdb_socket}')",  '-readonly', '~/nobackup/tpch-sf100.db'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
	# wait for socket to actually be open
	time.sleep(1)
	for threads in [1,2,4,8]:
		for rows in [int(math.pow(10,y)) for y in range(2, 8)] + [59986052]:
			timings = []
			for rep in range(3):
				start = time.time()
				client = subprocess.run([duckdb_binary,  '-init', '/dev/null',  '-c', f"set rpc_default_token='asdf'; SET enable_progress_bar = false; set threads={threads}; EXPLAIN ANALYZE FROM rpc_call('{duckdb_socket}','FROM lineitem LIMIT {rows}');"], stdout=subprocess.PIPE)
				timings.append(time.time() - start)
			median_time = round(statistics.median(timings), 3)
			print(f'{duckdb_socket}\t{threads}\t{rows}\t{median_time}')

	server.stdin.close()
	server.wait()