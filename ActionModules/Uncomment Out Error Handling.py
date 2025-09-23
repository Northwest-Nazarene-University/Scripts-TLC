import os
import re

def uncomment_keywords_in_py_files(directory):
    # Define the patterns to uncomment and their exceptions
    patterns = [
        r'#\s*(functionName)'
        , r'#\s*(try:)'
        , r'#\s*(except:)'
        , r'#\s*(except Exception as error:)'
        , r'#\s*(error_handler)'
        , r'#(functionName)'
        , r'#(try:)'
        , r'#(except:)'
        , r'#(except Exception as error:)'
        , r'#(error_handler)'
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
                    for pattern in patterns:
                        if re.search(pattern, line):
                            ## Remove the added comment symbol
                            modified_line = re.sub(pattern, lambda m: m.group(1), modified_line)
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