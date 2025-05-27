from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles # Added
from fastapi.responses import FileResponse # Added
from pydantic import BaseModel
from amazon_photos._api import AmazonPhotos # Ensure this import works
import os # For creating directory and ensuring media_downloads exists
import asyncio # For simulating work in placeholder

app = FastAPI()

# Create the media directory if it doesn't exist, for StaticFiles to mount
media_dir = "media_downloads"
os.makedirs(media_dir, exist_ok=True)

# Mount static files directory for downloaded media
app.mount("/media", StaticFiles(directory=media_dir), name="media")

# Global variable to store the AmazonPhotos instance
amazon_photos_instance: AmazonPhotos | None = None

# Global dictionary to store download status
download_status = {
    "is_running": False,
    "total_files": 0,
    "downloaded_files": 0,
    "current_file": "", # Name of the file currently being downloaded
    "error": None # Store any error message if download fails
}

class CookiesPayload(BaseModel):
    cookies: dict

# --- Endpoints ---

@app.get("/", response_class=FileResponse) # Modified
async def serve_frontend(): # Renamed for clarity
    return FileResponse('frontend.html')

@app.post("/api/set_cookies")
async def set_cookies(payload: CookiesPayload):
    global amazon_photos_instance
    try:
        # Assuming default db_path and tmp path are fine for now.
        # These might need to be configurable later.
        amazon_photos_instance = AmazonPhotos(cookies=payload.cookies)
        return {"message": "Cookies set and AmazonPhotos initialized successfully."}
    except Exception as e:
        # Log the exception for debugging
        print(f"Error initializing AmazonPhotos: {e}") # Or use proper logging
        raise HTTPException(status_code=500, detail=f"Failed to initialize AmazonPhotos: {str(e)}")

# Further endpoints will be added here

@app.get("/api/photos")
async def get_photos(offset: int = 0, limit: int = 20):
    global amazon_photos_instance
    if not amazon_photos_instance or amazon_photos_instance.db is None or amazon_photos_instance.db.empty:
        raise HTTPException(
            status_code=400,
            detail="Cookies not set or no photo data available. Please set cookies and ensure data is loaded/downloaded."
        )

    df = amazon_photos_instance.db
    # Assuming 'kind', 'type', 'id', 'name', 'createdDate' are columns in the DataFrame.
    # Adjust column names based on actual DataFrame structure from AmazonPhotos.db
    # The 'type' column might already be filtered by 'PHOTOS' or 'VIDEOS' by the library
    media_df = df[
        (df['kind'] == 'FILE') & (df['type'].isin(['PHOTO', 'VIDEO']))
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    if media_df.empty:
        return []

    # Apply pagination
    paginated_df = media_df.iloc[offset : offset + limit]

    result = []
    for index, row in paginated_df.iterrows():
        node_id = row['id']
        # Determine filename: 'name' column is likely the original filename.
        original_filename = row.get('name', 'untitled') # Default if 'name' column doesn't exist

        # The URL should reflect how we will serve it.
        photo_data = {
            "id": node_id,
            "filename": original_filename,
            "url": f"/media/{node_id}_{original_filename}", # This URL will be handled by a static file server
            "createdDate": row.get('createdDate'),
            "width": row.get('contentProperties.width'),
            "height": row.get('contentProperties.height'),
            "contentType": row.get('contentProperties.contentType'),
        }
        result.append(photo_data)

    return result

@app.get("/api/download_progress")
async def get_download_progress():
    global download_status
    return download_status

# This is the function that will run in the background
async def perform_download_task(instance: AmazonPhotos, node_ids: list[str], output_path: str = "media_downloads"):
    global download_status

    # Define the callback function to update global status
    def progress_updater(node_id, filename, status, error_message):
        global download_status # Ensure we're modifying the global dict
        
        # Increment files processed regardless of status, as an attempt was made
        # We rely on the final check to see if all were successful
        if download_status["downloaded_files"] < download_status["total_files"]:
             download_status["downloaded_files"] += 1

        current_progress_msg = f"{download_status['downloaded_files']}/{download_status['total_files']}"
        
        if status == "success":
            download_status["current_file"] = f"Successfully downloaded {filename} ({current_progress_msg})"
        elif status == "failure":
            error_detail = f"Failed to download {filename if filename else 'file for node ' + node_id}. Error: {error_message} ({current_progress_msg})"
            # Append to existing errors or set if first error
            if download_status["error"]:
                download_status["error"] += f"; {error_detail}"
            else:
                download_status["error"] = error_detail
            download_status["current_file"] = error_detail # Update current file to show the error
            print(f"Error downloading {node_id}{f': {filename}' if filename else ''}: {error_message}")

    try:
        # Ensure the output directory exists
        os.makedirs(output_path, exist_ok=True)

        download_status["total_files"] = len(node_ids)
        download_status["downloaded_files"] = 0 # Reset counter at the start
        download_status["current_file"] = "Initializing download process..."
        download_status["error"] = None # Reset errors at the start
        
        if not node_ids:
            download_status["current_file"] = "No files to download."
            download_status["is_running"] = False 
            return
        
        # Call the modified download method with the progress_updater callback
        # Note: instance.download is a synchronous method that internally runs asyncio.run()
        # This means it will block here until all its async tasks (individual file downloads) are complete.
        # If instance.download itself becomes async, this call would need `await`.
        instance.download(
            node_ids=node_ids,
            out=output_path,
            progress_callback=progress_updater
        )
        
        # Final status update after instance.download completes
        if download_status["error"]:
            download_status["current_file"] = f"Download process completed with errors. See error details. Processed: {download_status['downloaded_files']}/{download_status['total_files']}"
        elif download_status["downloaded_files"] < download_status["total_files"]:
            download_status["current_file"] = f"Download process finished, but not all files were processed. Processed: {download_status['downloaded_files']}/{download_status['total_files']}"
            if not download_status["error"]: # If no specific error was logged by a callback
                 download_status["error"] = "Some files may have failed silently or were not attempted."
        else:
            download_status["current_file"] = f"All {download_status['total_files']} files processed successfully."

    except Exception as e:
        print(f"Critical error during download task execution: {e}") # Log critical errors
        download_status["error"] = f"Critical task error: {str(e)}"
        download_status["current_file"] = f"A critical error occurred: {e}"
    finally:
        download_status["is_running"] = False
        # If after everything, no specific error is set but counts don't match, log a generic one.
        if not download_status["error"] and download_status["downloaded_files"] < download_status["total_files"]:
            download_status["error"] = "Download finished, but an inconsistency in file counts was detected without specific errors."
        
        # Final check on current_file if it's still showing a specific file from callback
        if "Successfully downloaded" in download_status["current_file"] or "Failed to download" in download_status["current_file"]:
            if download_status["error"]:
                 download_status["current_file"] = f"Download process completed with errors. Processed: {download_status['downloaded_files']}/{download_status['total_files']}"
            else:
                 download_status["current_file"] = f"All {download_status['total_files']} files processed."


@app.post("/api/start_download")
async def start_download(background_tasks: BackgroundTasks):
    global amazon_photos_instance, download_status

    if not amazon_photos_instance or amazon_photos_instance.db is None or amazon_photos_instance.db.empty:
        raise HTTPException(status_code=400, detail="Cookies not set or no data available. Please set cookies and sync data first.")

    if download_status["is_running"]:
        raise HTTPException(status_code=409, detail="A download process is already running.")

    df = amazon_photos_instance.db
    # Ensure correct filtering based on actual column names and values
    media_df = df[
        (df['kind'] == 'FILE') & (df['type'].isin(['PHOTO', 'VIDEO']))
    ]

    if media_df.empty:
        return {"message": "No photos or videos found in the database to download."}

    node_ids_to_download = media_df['id'].tolist()

    if not node_ids_to_download:
        # This case should ideally be covered by media_df.empty, but good to have a specific check
        return {"message": "No node IDs found for photos or videos to download."}
        
    # Reset status and add the background task
    download_status["is_running"] = True
    download_status["total_files"] = len(node_ids_to_download)
    download_status["downloaded_files"] = 0
    download_status["current_file"] = "Initializing download..."
    download_status["error"] = None
    
    output_directory = "media_downloads" 

    background_tasks.add_task(perform_download_task, amazon_photos_instance, node_ids_to_download, output_directory)

    return {"message": f"Download started for {len(node_ids_to_download)} items. Files will be saved to '{output_directory}' directory."}
