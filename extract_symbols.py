#!/usr/bin/env python3

import json

def extract_symbols(input_file, output_file):
    # Read the JSON file
    with open(input_file, 'r') as f:
        data = json.load(f)

    # List to store all symbols
    symbols = []

    # Function to extract symbols from the data
    def collect_symbols(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "symbol" and isinstance(value, str):
                    symbols.append(value)
                elif isinstance(value, (dict, list)):
                    collect_symbols(value)
        elif isinstance(obj, list):
            for item in obj:
                collect_symbols(item)

    # Collect all symbols
    collect_symbols(data)
    
    # Sort symbols alphabetically
    symbols.sort()

    # Write the symbols list to a new file
    with open(output_file, 'w') as f:
        json.dump(symbols, f, indent=4)

if __name__ == "__main__":
    input_file = "symbols_with_slashes.json"
    output_file = "symbol_list.json"
    extract_symbols(input_file, output_file)
    print(f"Symbol list has been saved to {output_file}") 