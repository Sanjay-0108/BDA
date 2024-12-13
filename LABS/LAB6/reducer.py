import sys
from collections import defaultdict

# Create a defaultdict to accumulate counts and movie details
language_count = defaultdict(lambda: {'count': 0, 'movies': []})

# Read input line by line
for line in sys.stdin:
    # Strip leading/trailing whitespaces
    line = line.strip()
    
    # Split the input into parts
    parts = line.split("\t")
    
    if parts[0] == "filtered":
        # Handle filtered movies (based on director, year, rating)
        director = parts[1]
        year = parts[2]
        rating = float(parts[3])
        movie_name = parts[4]
        language = parts[5]
        # Output filtered movie details
        print(f"Filtered Movie: {movie_name}, {year}, {rating}, {director}, {language}")
    else:
        # Handle language-wise movie counting
        language = parts[0]
        count = int(parts[1])
        movie_name = parts[2]
        year = parts[3]
        rating = float(parts[4])
        director = parts[5]
        
        # Update the count and store the movie details
        language_count[language]['count'] += count
        language_count[language]['movies'].append(f"{movie_name} ({year}) - {rating} by {director}")
        
# Print language-wise results
for language, data in language_count.items():
    print(f"Language: {language}, Movie Count: {data['count']}")
    for movie in data['movies']:
        print(f"  {movie}")
    
# Summarize at the end
print("\nSummary:")
for language, data in language_count.items():
    print(f"Language: {language}, Total Movies: {data['count']}")
