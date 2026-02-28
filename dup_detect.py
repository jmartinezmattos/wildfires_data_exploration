import sys

def find_consecutive_duplicates(file_path):
    with open(file_path, 'r') as file:
        lines = [line.strip() for line in file.readlines()]

    duplicates = []
    for i in range(len(lines) - 1):
        if lines[i] == lines[i + 1]:
            duplicates.append((i + 1, lines[i]))  # +1 for 1-based index

    return duplicates

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python find_duplicates.py <file.txt>")
        sys.exit(1)

    file_path = sys.argv[1]
    duplicates = find_consecutive_duplicates(file_path)

    if duplicates:
        print("Consecutive duplicate lines found:")
        for index, line in duplicates:
            print(line)
    else:
        print("No consecutive duplicate lines found.")