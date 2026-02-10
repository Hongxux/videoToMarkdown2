import os
import ast
import sys

def get_imports(path):
    imports = set()
    with open(path, 'r', encoding='utf-8') as f:
        try:
            root = ast.parse(f.read(), filename=path)
        except SyntaxError:
            return imports
        
    for node in ast.walk(root):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
                
    return imports

def scan_directory(root_dir):
    all_imports = set()
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.py'):
                path = os.path.join(dirpath, filename)
                all_imports.update(get_imports(path))
    return all_imports

if __name__ == "__main__":
    dirs_to_scan = [
        r'd:\videoToMarkdownTest2\services\python_grpc',
        r'd:\videoToMarkdownTest2\apps'
    ]
    
    final_imports = set()
    for d in dirs_to_scan:
        if os.path.exists(d):
            final_imports.update(scan_directory(d))
            
    # Filter out standard library modules (approximate list or just print all)
    # For now, just print everything sorted
    print('\n'.join(sorted(final_imports)))
