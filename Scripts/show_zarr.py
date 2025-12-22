#!/usr/bin/env python3
"""
Display the structure and contents of a Zarr dataset.

This script provides a detailed view of a Zarr dataset, including:
- Global attributes
- All variables/arrays with their shapes, data types, and chunks
- Variable-specific attributes
- Sample values for small arrays
- Storage size information

Usage:
    python show_zarr.py [--run-where {local,leonardo}] [zarr_path]

If no path is provided, defaults based on --run-where:
    - local: ./zarr_file/DE374h_deliv.zarr
    - leonardo: /leonardo_scratch/fast/DE374_lot2/zarr_deliv/DE374h_deliv.zarr
"""

import argparse
import zarr
from pathlib import Path


def show_zarr_structure(zarr_path: Path) -> None:
    """
    Display the complete structure of a Zarr dataset.

    Parameters
    ----------
    zarr_path : Path
        Path to the Zarr dataset directory.

    Returns
    -------
    None
    """
    print(f"\nZarr Dataset: {zarr_path}")
    print("=" * 80)

    # Open the zarr store in read-only mode
    root = zarr.open(str(zarr_path), mode='r')

    # Display global attributes if present
    if root.attrs:
        print("\nGlobal Attributes:")
        print("-" * 80)
        for key, value in root.attrs.items():
            print(f"  {key}: {value}")

    # Display all arrays and variables in the dataset
    print("\nArrays/Variables:")
    print("-" * 80)

    for name in sorted(root.keys()):
        arr = root[name]
        print(f"\n{name}:")
        print(f"  Shape: {arr.shape}")
        print(f"  Dtype: {arr.dtype}")
        print(f"  Chunks: {arr.chunks}")

        # Display variable-specific attributes
        if arr.attrs:
            print(f"  Attributes:")
            for key, value in arr.attrs.items():
                print(f"    {key}: {value}")

        # Show statistics or sample values based on array dimensions
        if arr.ndim > 0:
            try:
                # For 1D arrays, show first few values
                if arr.ndim == 1:
                    sample = arr[:min(5, arr.shape[0])]
                    print(f"  Sample values: {sample}")
                # For 2D arrays, show min/max statistics
                elif arr.ndim == 2:
                    arr_data = arr[:]
                    print(f"  Min: {arr_data.min():.4f}, "
                          f"Max: {arr_data.max():.4f}")
                # For 3D/4D arrays, just indicate size
                elif arr.ndim >= 3:
                    print(f"  Data array (too large to sample)")
            except Exception as e:
                print(f"  Could not compute statistics: {e}")

    # Display storage information for each variable
    print("\n" + "=" * 80)
    print("Storage Information:")
    print("-" * 80)

    # Calculate total storage size
    total_size = 0
    for name in root.keys():
        arr = root[name]
        nbytes = arr.nbytes
        total_size += nbytes
        size_mb = nbytes / (1024 * 1024)
        print(f"  {name}: {size_mb:.2f} MB")

    # Display total size in both MB and GB
    total_mb = total_size / (1024 * 1024)
    total_gb = total_size / (1024 * 1024 * 1024)
    print(f"\nTotal size: {total_mb:.2f} MB ({total_gb:.2f} GB)")


def main() -> int:
    """
    Main function to parse arguments and display Zarr structure.

    Returns
    -------
    int
        Exit code (0 for success, 1 for error).
    """
    parser = argparse.ArgumentParser(
        description='Display the structure of a Zarr dataset'
    )
    parser.add_argument(
        '--run-where',
        type=str,
        default='local',
        choices=['local', 'leonardo'],
        help='Environment where the code is running (default: local)'
    )
    parser.add_argument(
        'zarr_path',
        type=str,
        nargs='?',
        default=None,
        help='Path to the Zarr dataset directory '
             '(default: depends on environment)'
    )

    args = parser.parse_args()
    
    # Set environment-specific default for zarr path
    if args.zarr_path is None:
        if args.run_where == 'leonardo':
            args.zarr_path = (
                '/leonardo_scratch/fast/DE374_lot2/zarr_deliv/'
                'DE374h_deliv.zarr'
            )
        else:  # local
            args.zarr_path = './zarr_file/DE374h_deliv.zarr'
    
    zarr_path = Path(args.zarr_path)

    # Check if the zarr dataset exists
    if not zarr_path.exists():
        print(f"Error: Zarr dataset not found: {zarr_path}")
        return 1

    # Display the zarr structure
    show_zarr_structure(zarr_path)
    return 0


if __name__ == '__main__':
    exit(main())
