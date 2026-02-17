
import time
import os
import sys
import json
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.getcwd())

from services.python_grpc.src.common.utils.async_disk_writer import (
    enqueue_json_write,
    stop_async_json_writer,
    flush_async_json_writes,
    get_async_json_writer
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def benchmark_blocking():
    # Setup
    tmp_dir = Path("benchmark_tmp")
    tmp_dir.mkdir(exist_ok=True)
    
    # Large payload to stress the queue/writer
    large_payload = {"data": "x" * 1024 * 1024} # 1MB string
    
    logger.info("Starting benchmark...")
    
    start_time = time.time()
    count = 0
    blocked_calls = 0
    total_enqueue_time = 0
    
    # Try to fill the queue (maxsize=2048) and trigger fallback
    # We'll send 3000 items. 
    # If the writer is slow, the queue should fill up around 2048.
    
    for i in range(3000):
        t0 = time.time()
        enqueue_json_write(
            str(tmp_dir / f"test_{i}.json"),
            large_payload
        )
        dt = time.time() - t0
        total_enqueue_time += dt
        
        # If enqueue takes longer than 0.1s, it's likely blocking (sync write or queue wait)
        if dt > 0.1: 
            blocked_calls += 1
            # logger.warning(f"Call {i} took {dt:.4f}s")
            
        count += 1
        if count % 500 == 0:
            logger.info(f"Processed {count} items. Avg enqueue time: {total_enqueue_time/count:.6f}s")

    end_time = time.time()
    
    logger.info(f"Benchmark finished in {end_time - start_time:.2f}s")
    logger.info(f"Total calls: {count}")
    logger.info(f"Blocked calls (>0.1s): {blocked_calls}")
    logger.info(f"Average enqueue time: {total_enqueue_time/count:.6f}s")
    
    # Cleanup
    stop_async_json_writer()
    # import shutil
    # shutil.rmtree(tmp_dir)

if __name__ == "__main__":
    try:
        benchmark_blocking()
    except KeyboardInterrupt:
        stop_async_json_writer()
