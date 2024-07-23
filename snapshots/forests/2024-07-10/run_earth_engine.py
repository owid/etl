# Use this script to submit chunks of earth engine jobs to the earth engine API. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The
import subprocess

# STARTING_POINTS = ["1", "51", "101", "151", "201", "251", "301"]
CHUNK_SIZE = 30
starting_points = []
for i in range(1, 321, CHUNK_SIZE):
    i = str(i)
    starting_points.append(i)

for starting_point in starting_points:
    command = [
        "python",
        "snapshots/forests/2024-07-10/earth_engine.py",
        "--chunk_size",
        str(CHUNK_SIZE),  # Chunk size
        "--starting_point",
        starting_point,  # Starting point
    ]

    # Run the command using subprocess
    print("Running command: " + " ".join(command))
    result = subprocess.run(command, capture_output=True, text=True)
