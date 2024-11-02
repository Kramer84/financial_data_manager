import re

# File paths
input_file = 'interesting_symbols.md'
financial_symbols_file = 'financial_symbols.txt'
crypto_symbols_file = 'crypto_symbols.txt'

# Initialize lists for financial and cryptocurrency symbols
financial_symbols = []
crypto_symbols = []

# Read and parse the file
with open(input_file, 'r') as file:
    lines = file.readlines()
    is_financial = False
    is_crypto = False

    for line in lines:
        # Detect sections
        if "Traditional Finance Symbols" in line:
            is_financial = True
            is_crypto = False
        elif "Cryptocurrency Symbols" in line:
            is_crypto = True
            is_financial = False
        elif "---" in line:
            is_financial = False
            is_crypto = False

        # Extract symbols based on current section
        symbol_match = re.match(r"- \*\*(\S+)\*\*", line)
        if symbol_match:
            symbol = symbol_match.group(1)
            if is_financial:
                financial_symbols.append(symbol)
            elif is_crypto:
                crypto_symbols.append(symbol)

# Save financial symbols to file
with open(financial_symbols_file, 'w') as file:
    for symbol in financial_symbols:
        file.write(f"{symbol}\n")

# Save cryptocurrency symbols to file
with open(crypto_symbols_file, 'w') as file:
    for symbol in crypto_symbols:
        file.write(f"{symbol}\n")

print("Financial symbols and cryptocurrency symbols have been saved to text files.")
