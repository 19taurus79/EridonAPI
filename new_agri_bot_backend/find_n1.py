import ast

def find_n_plus_one_queries(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        source = f.read()

    tree = ast.parse(source)
    issues = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            for child in ast.walk(node):
                if isinstance(child, (ast.For, ast.AsyncFor, ast.While)):
                    # Check if there's an 'await' inside the loop
                    for loop_child in ast.walk(child):
                        if isinstance(loop_child, ast.Await):
                            # Try to see if wait is a DB call
                            is_db_call = False
                            for call_child in ast.walk(loop_child):
                                if isinstance(call_child, ast.Attribute) and call_child.attr in ('run', 'save', 'update', 'delete', 'select', 'insert'):
                                    is_db_call = True
                            if is_db_call or True: # report all awaits just in case
                                issues.append({
                                    'function': node.name,
                                    'line': loop_child.lineno,
                                    'loop_line': child.lineno
                                })
                                break
                            
    for issue in issues:
        print(f"Possible N+1 in function '{issue['function']}' (loop at {issue['loop_line']}, await at {issue['line']})")

if __name__ == "__main__":
    find_n_plus_one_queries(r"d:\Projects\EridonAPI\new_agri_bot_backend\main.py")
