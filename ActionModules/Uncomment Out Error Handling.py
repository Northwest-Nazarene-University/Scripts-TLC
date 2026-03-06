import os
import re


def uncomment_keywords_in_py_files(directory):
    ## List of (compiled_pattern, replacement) pairs
    patterns = [
        ## Uncomment "## try:" -> "    try:"
        (re.compile(r'^\s*##\s*(try\s*:)', re.MULTILINE), r'    \1'),

        ## Uncomment "## except ...:" -> "    except ...:"
        (re.compile(r'^\s*##\s*(except\b.*:\s*)', re.MULTILINE), r'    \1'),

        ## Uncomment "functionName" -> "    functionName"
        (re.compile(r'^\s*##\s*(    functionName\b)', re.MULTILINE), r'\1'),

        ## Uncomment "## errorHandler.sendError" -> "errorHandler.sendError"
        (re.compile(r'^\s*##\s*(errorHandler\.sendError\b)', re.MULTILINE), r'\1'),
    ]

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)

                with open(file_path, 'r') as f:
                    lines = f.readlines()

                modified_lines = []
                for line in lines:
                    modified_line = line
                    for pattern, replacement in patterns:
                        if pattern.search(modified_line):
                            modified_line = pattern.sub(replacement, modified_line)
                            ## You can break here if you guarantee only one pattern per line
                    modified_lines.append(modified_line)

                with open(file_path, 'w') as f:
                    f.writelines(modified_lines)

if __name__ == "__main__":
    target_directory = input("Enter the path to the target directory: ").strip()
    if os.path.isdir(target_directory):
        uncomment_keywords_in_py_files(target_directory)
        print(f"Reversion complete for directory: {target_directory}")
    else:
        print("Invalid directory path. Please try again.")
    input("Press Enter to exit...")