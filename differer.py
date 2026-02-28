def compare_files(file1_path, file2_path):
    with open(file1_path, 'r') as file1, open(file2_path, 'r') as file2:
        lines1 = file1.readlines()
        lines2 = file2.readlines()

    # Determine the maximum number of lines to compare
    max_lines = max(len(lines1), len(lines2))
    differing_indices = []


    for i in range(max_lines):
        # Handle cases where one file is shorter than the other
        line1 = lines1[i].strip() if i < len(lines1) else None
        line2 = lines2[i].strip() if i < len(lines2) else None

        if i in differing_indices:
            
            continue  # Skip already identified differing lines


        if line1 != line2:
            differing_indices.append(i + 1)  # +1 to make it 1-based index
            print(f"Difference at line {i + 1}:")
            break


    return differing_indices

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python compare_files.py <file1.txt> <file2.txt>")
        sys.exit(1)

    file1_path = sys.argv[1]
    file2_path = sys.argv[2]

    differing_indices = compare_files(file1_path, file2_path)

    if differing_indices:
        print("Lines with differences (1-based index):")
        for index in differing_indices:
            print(f"Line {index}")
    else:
        print("No differences found.")