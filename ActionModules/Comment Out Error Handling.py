import os
import re

def comment_keywords_in_py_files(directory):
    patterns = {
        r'\bfunctionName\b': None,
        r'\btry\b:': r'try:\s*##\s*Irregular',
        r'\bexcept\b:': r'except:\s*##\s*Irregular',
        r'\bexcept Exception as error\b:': r'except\s+Exception\s+as\s+error:\s*##\s*Irregular',
        r'\berror_handler\b': r'def\s+error_handler'
    }

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)

                ## Skip the excluded file
                if 'comment out error handling' in file_path.lower():
                    continue

                with open(file_path, 'r') as f:
                    lines = f.readlines()

                modified_lines = []
                for line in lines:
                    modified_line = line
                    for pattern, exception in patterns.items():
                        if re.search(pattern, line):
                            ## Skip commenting if the exception pattern matches
                            if exception and re.search(exception, line):
                                continue

                            ## Comment the matched keyword
                            modified_line = re.sub(pattern, lambda m: f'# {m.group(0)}', modified_line)
                    modified_lines.append(modified_line)

                with open(file_path, 'w') as f:
                    f.writelines(modified_lines)

## Replace this with your actual path
if __name__ == "__main__":
    target_directory = input("Enter the path to the target directory: ").strip()
    if os.path.isdir(target_directory):
        comment_keywords_in_py_files(target_directory)
        print(f"Processing complete for directory: {target_directory}")
    else:
        print("Invalid directory path. Please try again.")
    
        input ("Press Enter to exit...")
