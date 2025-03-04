import os
import fnmatch

def list_files(directory, output_dir):
    output_file_path = os.path.join(output_dir, 'code_contents.txt')
    excluded_dirs = [
        'venv', '__pycache__', 'node_modules', 'build', 'dist', 'downloads', 'eggs', 
        '.eggs', 'lib', 'lib64', 'parts', 'sdist', 'var', 'wheels', 'share/python-wheels', 
        'htmlcov', '.tox', '.nox', '.coverage', 'nosetests.xml', 'coverage.xml', 
        '.hypothesis', '.pytest_cache', 'docs/_build', '.pybuilder', 'target', 
        '.ipynb_checkpoints', 'profile_default', 'instance', '.webassets-cache', 
        '.scrapy', '.pdm.toml', '.pdm-python', '.pdm-build', '__pypackages__', 
        'celerybeat-schedule', 'cython_debug', '.mypy_cache', '.dmypy.json', 
        'dmypy.json', '.pyre', '.pytype','.next','ui', '.bc-aux', '.devcontainer'
    ]
    #also exclude any directory starting with .
    excluded_dirs += [d for d in os.listdir(directory) if d.startswith('.')]
    excluded_files = [
        '*.py[cod]', '*$py.class', '*.so', '*.manifest', '*.spec', 'pip-log.txt', 
        'pip-delete-this-directory.txt', '*.mo', '*.pot', '*.log', 'local_settings.py', 
        'db.sqlite3', 'db.sqlite3-journal', '.python-version', 'Pipfile.lock', 
        'poetry.lock', 'pdm.lock', '*.sage.py', '*.cover', '*.py,cover', 
        'celerybeat.pid', '.idea/', 'code_contents.txt'
    ]
    included_extensions = ['.py', '.js', '.css', '.html', '.env','.tsx']

    with open(output_file_path, 'w', encoding='utf-8') as out:
        for root, dirs, files in os.walk(directory):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if not any(excluded in os.path.join(root, d) for excluded in excluded_dirs)]
            
            for file in files:
                # Check if the file has one of the included extensions and is not in the excluded files
                if any(file.endswith(ext) for ext in included_extensions) and not any(fnmatch.fnmatch(file, pattern) for pattern in excluded_files):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, start=directory)
                    out.write(f"File: {rel_path}\nContents:\n")
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_content = f.read()
                            out.write(f"{file_content}\n" + ("-" * 40) + "\n")
                    except Exception as e:
                        out.write(f"Error reading file: {e}\n" + ("-" * 40) + "\n")
        
        print(f"All file contents have been written to '{output_file_path}'.")

if __name__ == "__main__":
    directory_to_scan = os.path.dirname(os.path.realpath(__file__))  # This scans the directory of the script
    output_directory = os.path.dirname(os.path.realpath(__file__))

    list_files(directory_to_scan, output_directory)
