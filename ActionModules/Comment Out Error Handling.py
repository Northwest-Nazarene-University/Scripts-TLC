import os
import re


def comment_keywords_in_py_files(directory):
    # pattern -> {exception: <regex or None>, strip_indent: bool}
    patterns = {
        # Just comment the keyword wherever it appears
        r'\b    functionName\b': {
            'exception': None,
            'strip_indent': False,
        },

        # Comment any "    try:"-style line, but strip indent in the comment
        r'^\s*try\s*:': {
            'exception': r'try\s*:\s*##\s*Irregular',
            'strip_indent': True,
        },

        # Comment any "    except ...:" line, strip indent in the comment
        r'^\s*except\b.*:\s*$': {
            'exception': r'except\b.*:\s*##\s*Irregular',
            'strip_indent': True,
        },

        # Comment errorHandler.sendError usage
        r'\berrorHandler\.sendError\b': {
            'exception': r'def\s+errorHandler\.sendError',
            'strip_indent': False,
        },
    }

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py') and "Canvas_Report" not in file:
                file_path = os.path.join(root, file)

                # Skip the excluded file
                if 'comment out error handling' in file_path.lower():
                    continue

                with open(file_path, 'r') as f:
                    lines = f.readlines()

                modified_lines = []
                for line in lines:
                    modified_line = line
                    for pattern, cfg in patterns.items():
                        exception = cfg['exception']
                        strip_indent = cfg['strip_indent']

                        if re.search(pattern, line):
                            # Skip commenting if the exception pattern matches
                            if exception and re.search(exception, line):
                                continue

                            # Comment the matched keyword
                            def repl(m):
                                text = m.group(0)
                                if strip_indent:
                                    # Remove leading spaces from the matched text
                                    text = text.lstrip()
                                return f'## {text}'

                            modified_line = re.sub(pattern, repl, modified_line)
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
