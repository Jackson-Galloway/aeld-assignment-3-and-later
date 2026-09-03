#!/bin/bash
# writer.sh - writes a string to a file, creating the path if needed

if [ $# -lt 2 ]; then
    echo "Error: missing arguments" >&2
    echo "Usage: $0 <writefile> <writestr>" >&2
    exit 1
fi

writefile="$1"
writestr="$2"

writedir=$(dirname "$writefile")

# make sure the directory path exists before we try to write the file
mkdir -p "$writedir"
if [ $? -ne 0 ]; then
    echo "Error: could not create directory $writedir" >&2
    exit 1
fi

echo "$writestr" > "$writefile"
if [ $? -ne 0 ]; then
    echo "Error: could not create file $writefile" >&2
    exit 1
fi

exit 0
