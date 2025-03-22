import multiprocessing
import os
import mmap
import math
from typing import List, Tuple, Dict
from dataclasses import dataclass

@dataclass
class City:
    min: int
    max: int
    sum: int
    count: int

# Use environment variable for file path or fallback to default
file_path = os.environ.get("INPUT_FILE", "testcase.txt")

def is_new_line(position: int, mm: mmap.mmap) -> bool:
    if position == 0:
        return True
    else:
        mm.seek(position - 1)
        return mm.read(1) == b"\n"

def next_line(position: int, mm: mmap.mmap) -> int:
    mm.seek(position)
    mm.readline()
    return mm.tell()

def process_chunk(chunk_start: int, chunk_end: int) -> Dict[bytes, City]:
    # Use dictionary for faster lookups instead of list with linear search
    result: Dict[bytes, City] = {}
    chunk_size = chunk_end - chunk_start
    
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), length=chunk_size, access=mmap.ACCESS_READ, offset=chunk_start)
        # if not starting at beginning, skip partial line
        if chunk_start != 0:
            next_line(0, mm)
            
        for line in iter(mm.readline, b""):
            if not line.strip():
                continue
                
            # Faster parsing without unnecessary checks
            semicolon_pos = line.find(b";")
            if semicolon_pos == -1:
                continue
                
            location = line[:semicolon_pos]
            temp_str = line[semicolon_pos+1:].strip()
            
            # Multiply measurement by 10 and convert to int
            measurement = int(float(temp_str) * 10)
            
            # Dictionary lookup is much faster than linear search
            city = result.get(location)
            if city:
                # update existing record
                if measurement < city.min:
                    city.min = measurement
                if measurement > city.max:
                    city.max = measurement
                city.sum += measurement
                city.count += 1
            else:
                result[location] = City(measurement, measurement, measurement, 1)
                
        mm.close()
        return result

def identify_chunks(num_processes: int) -> List[Tuple[int, int]]:
    chunk_results = []
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // num_processes
        # adjust chunk size to allocation granularity
        chunk_size += mmap.ALLOCATIONGRANULARITY - (chunk_size % mmap.ALLOCATIONGRANULARITY)
        start = 0
        while start < file_size:
            end = start + chunk_size
            if end < file_size:
                end = next_line(end, mm)
            if start == end:
                end = next_line(end, mm)
            if end > file_size:
                end = file_size
                chunk_results.append((start, end))
                break
            chunk_results.append((start, end))
            start = end  # Fix: Use end directly instead of incremental chunks
        mm.close()
        return chunk_results

def round_to_infinity(x: float) -> float:
    # x is the value in the original *10 int space divided back into float
    # Multiply by 10 then take ceiling and then adjust back
    return math.ceil(x * 10) / 10

def main() -> None:
    # Get optimal process count (use fewer processes for smaller files)
    file_size = os.path.getsize(file_path)
    num_processes = min(os.cpu_count() or 1, max(1, file_size // (50 * 1024 * 1024)))
    
    # Print progress information
    print(f"Processing file: {file_path} ({file_size/1024/1024:.2f} MB)")
    print(f"Using {num_processes} processes")
    
    chunk_results = identify_chunks(num_processes)
    with multiprocessing.Pool(num_processes) as pool:
        list_of_chunk_results = pool.starmap(process_chunk, chunk_results)

    # Merge results from all chunks
    merged_results: Dict[bytes, City] = {}
    for chunk_dict in list_of_chunk_results:
        for location, data in chunk_dict.items():
            city = merged_results.get(location)
            if city:
                if data.min < city.min:
                    city.min = data.min
                if data.max > city.max:
                    city.max = data.max
                city.sum += data.sum
                city.count += data.count
            else:
                merged_results[location] = data
    
    # Sort by the byte string location
    sorted_results = sorted(merged_results.items(), key=lambda kv: kv[0])

    # Write output
    with open("output.txt", "wb") as out_file:
        for location, measurements in sorted_results:
            # Adjust values: divide by 10 and apply ceiling rounding if needed
            min_val = round_to_infinity(measurements.min / 10)
            mean_val = round_to_infinity((measurements.sum / measurements.count) / 10)
            max_val = round_to_infinity(measurements.max / 10)
            
            # Build line as bytes without converting location to Unicode
            line = (location + b"=" +
                   f"{min_val:.1f}/{mean_val:.1f}/{max_val:.1f}".encode("utf8") +
                   b"\n")
            out_file.write(line) 
    
    print(f"Results written to output.txt")

if __name__ == "__main__":
    main()