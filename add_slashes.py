#!/usr/bin/env python3

import json
import re

def add_slashes_to_symbols(input_file, output_file):
    # Read the JSON file
    with open(input_file, 'r') as f:
        data = json.load(f)

    # Function to add slash between base and quote currencies
    def process_symbol(symbol):
        # Common quote currencies
        quote_currencies = ['USDT', 'BTC', 'ETH', 'BNB', 'BUSD', 'USD', 'USDC']
        
        # Sort quote currencies by length (longest first) to avoid wrong matches
        quote_currencies.sort(key=len, reverse=True)
        
        # Find the quote currency
        for quote in quote_currencies:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}/{quote}"
        
        # If no known quote currency is found, try to split at a reasonable point
        # Look for transition from letters to numbers or vice versa
        matches = re.finditer(r'(?<=[A-Z])(?=\d)|(?<=\d)(?=[A-Z])', symbol)
        split_points = [m.start() for m in matches]
        
        if split_points:
            # Use the last split point
            split_point = split_points[-1]
            return f"{symbol[:split_point]}/{symbol[split_point:]}"
        
        # If no clear split point, return original symbol
        return symbol

    # Process all symbols in the data
    def process_data(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "symbol" and isinstance(value, str):
                    obj[key] = process_symbol(value)
                elif isinstance(value, (dict, list)):
                    process_data(value)
        elif isinstance(obj, list):
            for item in obj:
                process_data(item)

    # Process the data
    process_data(data)

    # Write the modified JSON back to a new file
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    input_file = "symbols.json"
    output_file = "symbols_with_slashes.json"
    add_slashes_to_symbols(input_file, output_file)
    print(f"Processed symbols have been saved to {output_file}") 