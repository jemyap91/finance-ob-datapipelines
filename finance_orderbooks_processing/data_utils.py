import re
import os
from datetime import datetime

def list_volume_files(dbutils, volume_path, folder, pattern, extensions):
    """
    List all Excel files in the volume folder that match the prefix pattern.
    """
    folder_path = f"{volume_path}/{folder}" if folder else volume_path
    
    try:
        files = dbutils.fs.ls(folder_path)
        
        matching_files = []
        for file_info in files:
            filename = file_info.name
            file_ext = os.path.splitext(filename)[1].lower()
            
            # Check if file matches pattern and has valid extension
            if re.match(pattern, filename) and file_ext in extensions:
                matching_files.append({
                    'path': file_info.path,
                    'name': filename,
                    'size': file_info.size,
                    'mtime': datetime.fromtimestamp(file_info.modificationTime / 1000)
                })
        
        return matching_files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []