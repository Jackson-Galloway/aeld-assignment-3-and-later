#!/bin/bash
# finder.sh - counts files and lines matching a search string in a directory tree

if [ $# -lt 2 ]; then
    echo "Error: missing arguments" >&2
    echo "Usage: $0 <filesdir> <searchstr>" >&2
    exit 1
fi

filesdir="$1"
searchstr="$2"

if [ ! -d "$filesdir" ]; then
    echo "Error: $filesdir is not a directory" >&2
    exit 1
fi

# find every regular file under filesdir, including subdirectories
numfiles=$(find "$filesdir" -type f | wc -l)

# grep -r searches recursively; -c would only count per-file, so we
# count matching lines across all files by piping to wc instead
numlines=$(grep -r "$searchstr" "$filesdir" | wc -l)

echo "The number of files are $numfiles and the number of matching lines are $numlines"

exit 0
