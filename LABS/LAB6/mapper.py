import sys

# Check if correct arguments are passed
if len(sys.argv) != 4:
    print("Usage: python mapper.py <Director Name> <Year> <Rating>")
    sys.exit(1)

# Get dynamic inputs from command-line arguments
director_filter = sys.argv[1]
year_filter = sys.argv[2]
rating_filter = float(sys.argv[3])

# Process lines
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    columns = line.split("\t")
    if len(columns) < 6:
        continue

    movie_id = columns[0]
    movie_name = columns[1]
    year = columns[2]
    rating = float(columns[3])
    director = columns[4]
    language = columns[5]

    # Apply the filter
    if director == director_filter and year == year_filter and rating >= rating_filter:
        # Print filtered movie data
        print(f"filtered\t{director}\t{year}\t{rating}\t{movie_name}\t{language}")

    # Output language-wise movie count
    print(f"{language}\t1\t{movie_name}\t{year}\t{rating}\t{director}")
