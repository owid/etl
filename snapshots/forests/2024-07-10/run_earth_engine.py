# Use this script to submit chunks of earth engine jobs to the earth engine API. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The script will run the earth_engine.py script with different starting points. The starting point is the index of the country to start from. The chunk size is the number of countries to process in each chunk. The
import subprocess

starting_points = ["1", "51", "101", "151", "201", "251", "301"]
# Define the command and arguments

for starting_point in starting_points:
    command = [
        "python",
        "earth_engine.py",  # The name of your click script
        "--chunk_size",
        "50",  # Chunk size
        "--starting_point",
        starting_point,  # Example argument for starting_point
    ]

    # Run the command using subprocess
    result = subprocess.run(command, capture_output=True, text=True)
