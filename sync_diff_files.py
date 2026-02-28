from __future__ import annotations

from pathlib import Path


def sync_and_diff(file_a_path: Path, file_b_path: Path, output_path: Path | None = None) -> None:
    """
    Read two files line by line and report differences.
    When a difference is found, try to resynchronize by checking if the next lines match.
    """
    with open(file_a_path, encoding="utf-8") as f_a, open(file_b_path, encoding="utf-8") as f_b:
        lines_a = [line.rstrip("\n") for line in f_a]
        lines_b = [line.rstrip("\n") for line in f_b]
    
    output_lines = []
    idx_a = 0
    idx_b = 0
    
    def log_msg(msg: str) -> None:
        print(msg)
        output_lines.append(msg)
    
    log_msg(f"Starting comparison:")
    log_msg(f"  File A: {file_a_path.name} ({len(lines_a)} lines)")
    log_msg(f"  File B: {file_b_path.name} ({len(lines_b)} lines)")
    log_msg("")
    
    difference_count = 0
    
    while idx_a < len(lines_a) or idx_b < len(lines_b):
        # Handle end of one file
        if idx_a >= len(lines_a):
            log_msg(f"End of File A reached. Remaining in File B:")
            for i in range(idx_b, min(idx_b + 10, len(lines_b))):
                log_msg(f"  B[{i}]: {lines_b[i]}")
            if len(lines_b) - idx_b > 10:
                log_msg(f"  ... and {len(lines_b) - idx_b - 10} more lines")
            break
        
        if idx_b >= len(lines_b):
            log_msg(f"End of File B reached. Remaining in File A:")
            for i in range(idx_a, min(idx_a + 10, len(lines_a))):
                log_msg(f"  A[{i}]: {lines_a[i]}")
            if len(lines_a) - idx_a > 10:
                log_msg(f"  ... and {len(lines_a) - idx_a - 10} more lines")
            break
        
        # Compare current lines
        if lines_a[idx_a] == lines_b[idx_b]:
            # Lines match, advance both
            idx_a += 1
            idx_b += 1
        else:
            # Difference found
            difference_count += 1
            log_msg(f"[Difference #{difference_count}] at A[{idx_a}] vs B[{idx_b}]:")
            log_msg(f"  A[{idx_a}]: {lines_a[idx_a]}")
            log_msg(f"  B[{idx_b}]: {lines_b[idx_b]}")
            
            # Try to resynchronize
            # Check if next line of A matches current line of B
            resync_found = False
            if idx_a + 1 < len(lines_a) and lines_a[idx_a + 1] == lines_b[idx_b]:
                log_msg(f"  → Resync: A[{idx_a + 1}] == B[{idx_b}]")
                log_msg(f"    Skipping A[{idx_a}]: {lines_a[idx_a]}")
                idx_a += 1
                resync_found = True
            # Check if next line of B matches current line of A
            elif idx_b + 1 < len(lines_b) and lines_a[idx_a] == lines_b[idx_b + 1]:
                log_msg(f"  → Resync: A[{idx_a}] == B[{idx_b + 1}]")
                log_msg(f"    Skipping B[{idx_b}]: {lines_b[idx_b]}")
                idx_b += 1
                resync_found = True
            # Try further ahead
            else:
                for lookahead in range(1, min(5, len(lines_a) - idx_a, len(lines_b) - idx_b)):
                    if lines_a[idx_a + lookahead] == lines_b[idx_b]:
                        log_msg(f"  → Resync: A[{idx_a + lookahead}] == B[{idx_b}]")
                        for i in range(lookahead):
                            log_msg(f"    Skipping A[{idx_a + i}]: {lines_a[idx_a + i]}")
                        idx_a += lookahead
                        resync_found = True
                        break
                    elif lines_a[idx_a] == lines_b[idx_b + lookahead]:
                        log_msg(f"  → Resync: A[{idx_a}] == B[{idx_b + lookahead}]")
                        for i in range(lookahead):
                            log_msg(f"    Skipping B[{idx_b + i}]: {lines_b[idx_b + i]}")
                        idx_b += lookahead
                        resync_found = True
                        break
            
            if not resync_found:
                log_msg(f"  → No resync found, advancing both by 1")
                idx_a += 1
                idx_b += 1
            
            log_msg("")
    
    log_msg(f"Comparison complete. Total differences found: {difference_count}")
    
    # Write output to file if requested
    if output_path:
        output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
        print(f"\nOutput saved to {output_path}")


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    
    file_a = base_dir / "bucket_tests" / "processed_base_names.txt"
    file_b = base_dir / "bucket_tests" / "processed_bucket_names.txt"
    output_file = base_dir / "sync_diff_report.txt"
    
    if not file_a.exists():
        print(f"Error: {file_a} not found")
        return
    
    if not file_b.exists():
        print(f"Error: {file_b} not found")
        return
    
    sync_and_diff(file_a, file_b, output_file)


if __name__ == "__main__":
    main()
