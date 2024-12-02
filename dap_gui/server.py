## NOTE
## The dependencies are NOT in pyproject.toml but in environment.yml

import base64
import patch_botocore # noqa
import os
from typing import Optional
import pandas as pd
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from itables import show
import itables
import psutil
import os
import boto3
from fastapi import HTTPException
from PIL import Image
from io import BytesIO
import io
import matplotlib.pyplot as plt
from functools import lru_cache


## MASS INSERT
# INSERT INTO bnp.dataset_location
# SELECT *
# FROM agdc.dataset_location
# WHERE uri_body ~* '(T33VVH|T33VVJ|T33VWH|T33VWJ)'
#   AND uri_body LIKE '%MSIL1C_%'
#   AND substring(uri_body FROM '.*MSIL1C_(\d{8})T.*')::date > '2024-05-01';


itables.init_notebook_mode(all_interactive=True)
# Initialize FastAPI application
app = FastAPI()

# Database Configuration
DB_HOST = os.getenv("BNP_DB_HOSTNAME", "datasource.main.rise-ck8s.com")
DB_PORT = os.getenv("BNP_DB_PORT", "30103")
DB_USER = os.getenv("BNP_DB_USERNAME", "bnp_db_rw")
DB_PASSWORD = os.getenv("BNP_DB_PASSWORD", "bnp_password")
DB_NAME = os.getenv("BNP_DB_DATABASE", "datacube")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url="https://s3.rise.safedc.net",
)

# Load stac as well when we have that :)
 
# -------------------------------------------------------------------------------------
# INTERNAL                  load_image_from_dest_path
# -------------------------------------------------------------------------------------
@lru_cache(maxsize=100)
def load_image_from_dest_path(s3_image_path: str) -> str:
    """
    Load an image from an S3 destination URI and return it as a base64-encoded string for embedding in HTML.

    :param dst_path: Full S3 path (e.g., "s3://bucket-name/path/to/image.jpg")
    :return: Base64-encoded image string for embedding in HTML
    """
    try:
        # Validate the S3 URI
        if not s3_image_path.startswith("s3://"):
            raise ValueError("Invalid S3 URI. Must start with 's3://'.")
   
        # Extract bucket name and key from the URI

        bucket_name, object_key = extract_bucket_and_key(s3_image_path)
        bucket_name = f'd49b125f138b4dd9b225925950e638bc:{bucket_name}' if 'd49b1' not in bucket_name else bucket_name
        
        # Fetch the image from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        image_data = response["Body"].read()
        
        # Load the image into a PIL Image object
        image = Image.open(BytesIO(image_data))
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        buffer.seek(0)
        
        # Base64 encode the image
        image_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
        return image_base64
    except Exception as e:
        print(f"Error loading image from S3: {type(e).__name__} {e}")
        return None
# -------------------------------------------------------------------------------------
#  INTERNAL                   extract_bucket_and_key
# -------------------------------------------------------------------------------------
def extract_bucket_and_key(s3_path: str):
    """
    Extract the bucket name and key from an S3 path.

    :param s3_path: Full S3 path (e.g., "s3://bucket/key")
    :return: Tuple of (bucket_name, object_key)
    """
    s3_path = s3_path[5:]  # Remove 's3://'
    bucket_name, *key_parts = s3_path.split("/")
    object_key = "/".join(key_parts)
    return bucket_name, object_key


reload = "<meta http-equiv='refresh' content='2'>"
table_options = {
    "justify": "center",
    "render_links": True,
    "escape": False,
    "index": False,
    "classes": [
        "table",
        "table-striped",
        "table-hover",
        "table-bordered",
    ],  # Use the appropriate Bootstrap classes here
}
css = (
    ".dataframe {"
    "  font-family: arial, sans-serif;"
    "  border-collapse: collapse;"
    "  width: 100%;"
    "}"
    "td, th {"
    "  border: 0px solid #dddddd;"
    "  text-align: left;"
    "  padding: 8px;"
    "}"
    "tr:nth-child(even) {"
    "  background-color: #dddddd;"
    "}"
    "th {"
    "  cursor: pointer;"
    "}"
    ".monospace {"
    "  font-family: Consolas, Courier, monospace;"
    "}"
    "h3 {"
    "  margin: 0;"  # Remove margin for h3 inside table cells
    "}"
)

# ------------------------------------------------------------------------------------------------------------------
# INTERNAL                                     convert_to_seconds
# ------------------------------------------------------------------------------------------------------------------
def convert_to_seconds(time_str):
    """
    Converts a time string in the format `minutes:seconds` to total seconds.
    Cleans irregular formats like '04:9.', ensuring robust conversion.
    """
    try:
        # Remove trailing or extraneous characters (like dots)
        clean_time_str = time_str.strip().replace('.', '')
        
        # Split into minutes and seconds
        parts = clean_time_str.split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1].zfill(2))  # Pad seconds to 2 digits
            return minutes * 60 + seconds
        else:
            raise ValueError(f"Invalid time format: {time_str}")
    except Exception as e:
        raise ValueError(f"Error processing time string '{time_str}': {e}")


# ------------------------------------------------------------------------------------------------------------------
# INTERNAL                                     get_histogram
# ------------------------------------------------------------------------------------------------------------------

def get_histogram(df: pd.DataFrame) -> str:
    """
    Generate a histogram for the 'total_execution_time' column in the DataFrame,
    encode it as a base64 image string, and return the encoded string.

    Args:
        df (pd.DataFrame): The input DataFrame with a 'total_execution_time' column.

    Returns:
        str: The base64-encoded image string of the histogram.
    """
    if "total_execution_time" not in df.columns:
        raise ValueError("The DataFrame must contain a 'total_execution_time' column.")
    
    # Reuse the existing conversion function
    def convert_to_seconds(time_str):
        """
        Converts a time string in the format `minutes:seconds` to total seconds.
        Handles single-digit seconds like `04:9.` by cleaning and padding.
        """
        try:
            # Clean up any irregularities in the time format
            clean_time_str = time_str.strip().replace('.', '')  # Remove dots
            parts = clean_time_str.split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1].zfill(2))  # Pad single-digit seconds with a leading zero
                return minutes * 60 + seconds
            else:
                raise ValueError(f"Invalid time format: {time_str}")
        except Exception as e:
            raise ValueError(f"Error processing time string '{time_str}': {e}")

    # Apply conversion to seconds
    df['total_execution_time_seconds'] = df['total_execution_time'].apply(convert_to_seconds)

    # Create the histogram using the 'total_execution_time_seconds' column
    plt.figure(figsize=(12, 6))
    counts, bins, patches = plt.hist(df['total_execution_time_seconds'], bins=50, alpha=0.7, edgecolor='black')

    # Convert bin edges to min:sec format
    def seconds_to_min_sec(seconds):
        minutes = seconds // 60
        seconds = seconds % 60
        return f"{int(minutes)}:{int(seconds):02d}"

    bin_labels = [seconds_to_min_sec(b) for b in bins]

    # Improve x-axis readability
    plt.xticks(
        ticks=bins,  # Set ticks at bin edges
        labels=bin_labels,  # Use custom labels
        fontsize=10,  # Increase font size
        rotation=45   # Rotate for better readability
    )

    # Add titles and labels
    plt.title('Total Execution Time Histogram', fontsize=14)
    plt.xlabel('Execution Time (min:sec)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.grid(axis='y', alpha=0.75)

    # Save the plot to a BytesIO buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()

    # Encode the image in base64
    image_base64 = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()

    return image_base64

# ------------------------------------------------------------------------------------------------------------------
# INTERNAL                                     get_navigation_table
# ------------------------------------------------------------------------------------------------------------------
def get_navigation_table() -> str:
    """
    Generate a navigation table for the FastAPI dashboard.

    Returns:
        str: HTML content for the navigation table.
    """
    return """
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <div class='container my-3'>
        <table class='table table-bordered'>
            <tbody>
                <tr>
                    <td><strong>Navigation:</strong></td>
                    <td><a href='/'>Home</a></td>
                    <td><a href='/status-summary'>Status Summary</a></td>
                    <td><a href='/workers'>Workers</a></td>
                    <td><a href='/products'>Products</a></td>
                    
                </tr>
            </tbody>
        </table>
    </div>
    """


# ------------------------------------------------------------------------------------------------------------------
# INTERNAL
# ------------------------------------------------------------------------------------------------------------------
def get_system_metrics():
    """
    Collects system metrics and returns them as a pandas DataFrame.
    """
    metrics = {}

    # CPU Usage
    metrics["CPU Usage (%)"] = psutil.cpu_percent(interval=1)

    # Memory Usage
    mem = psutil.virtual_memory()
    metrics["Total Memory (GB)"] = round(mem.total / (1024**3), 2)
    metrics["Available Memory (GB)"] = round(mem.available / (1024**3), 2)
    metrics["Used Memory (GB)"] = round(mem.used / (1024**3), 2)
    metrics["Memory Usage (%)"] = mem.percent

    # Swap Memory
    swap = psutil.swap_memory()
    metrics["Total Swap (GB)"] = round(swap.total / (1024**3), 2)
    metrics["Used Swap (GB)"] = round(swap.used / (1024**3), 2)
    metrics["Swap Usage (%)"] = swap.percent

    # Disk Usage
    disk = psutil.disk_usage("/")
    metrics["Total Disk Space (GB)"] = round(disk.total / (1024**3), 2)
    metrics["Used Disk Space (GB)"] = round(disk.used / (1024**3), 2)
    metrics["Free Disk Space (GB)"] = round(disk.free / (1024**3), 2)
    metrics["Disk Usage (%)"] = disk.percent

    # Load Average (Unix systems)
    if hasattr(psutil, "getloadavg"):
        load1, load5, load15 = psutil.getloadavg()
        metrics["Load Average (1 min)"] = round(load1, 2)
        metrics["Load Average (5 min)"] = round(load5, 2)
        metrics["Load Average (15 min)"] = round(load15, 2)

    # Network I/O
    net_io = psutil.net_io_counters()
    metrics["Bytes Sent (MB)"] = round(net_io.bytes_sent / (1024**2), 2)
    metrics["Bytes Received (MB)"] = round(net_io.bytes_recv / (1024**2), 2)

    # Create a DataFrame
    df = pd.DataFrame(list(metrics.items()), columns=["Metric", "Value"])
    return df


# -------------------------------------------------------------------------------------
# INTERNAL                       format_status
# -------------------------------------------------------------------------------------
def format_status(status):
    state_colormap = {
        "running": "darkorange",
        "canceled": "yellow",
        "failed": "red",
        "finished": "green",
    }

    res = f'<span style="color: {state_colormap.get(str(status),str(status))}">{str(status)}</span>'
    return res


# -------------------------------------------------------------------------------------
# INTERNAL                       get_table
# -------------------------------------------------------------------------------------
def get_table(query: str, parameters: dict = None):
    with engine.connect() as connection:
        result = connection.execute(text(query), parameters or {})
        rows = result.fetchall()
        return pd.DataFrame(rows, columns=result.keys())

# -------------------------------------------------------------------------------------
# INTERNAL                       esaL2A_from_desL2A
# -------------------------------------------------------------------------------------
def esaL2A_from_desL2A(dst_path:str)->str:
    # S2B_MSIL2A_20241030T103151_D001_R108_T33VWJ_20241130T174423.COG
    # /S2A_MSIL1C_20241109T103241_N0511_R108_T33VWF_20241109T123320.stac.json
    fields = dst_path.split('_')
    aq_date = fields[2]
    tile = fields[5]

    query = """SELECT uri_body AS uri
    FROM agdc.dataset_location
    WHERE uri_body ~ :regexp;
    """
    regex_pattern = {'regexp':f"MSIL2A.*{aq_date}.*{tile}"}
    result_df = get_table(query=query,parameters=regex_pattern)
    if result_df.size == 0:
        return None
    l2a_stac = result_df.iloc[0]["uri"] 
    # //d49b125f138b4dd9b225925950e638bc:eodata-sentinel2-s2msi2a-cog-01-2024/11/9/S2A_MSIL2A_20241109T103241_N0511_R108_T32UPG_20241109T144448.SAFE.COG/stac_item.json#
    # Corresponding SAFE is most probably:
    
    safe_path = "s3:" + l2a_stac.split('.COG')[0].replace('-cog-01','')
    prod_name = Path(safe_path).name.replace('.SAFE','')
    quicklook_tci_path = f"{safe_path}/{prod_name}-ql.jpg" 
    return quicklook_tci_path

# -------------------------------------------------------------------------------------
# INTERNAL                       get_dest_path_from_job_id
# -------------------------------------------------------------------------------------
def get_dest_path_from_job_id(job_id: str):
    query = "SELECT dst_path FROM bnp.process_executions WHERE id = :job_id;"
    parameters = {"job_id": job_id}
    dst_path_result = get_table(query, parameters)
    if dst_path_result.empty or not dst_path_result.iloc[0]["dst_path"]:
        return None
    return dst_path_result.iloc[0]["dst_path"]


# -------------------------------------------------------------------------------------
# API  GET                               /                                      -->root
# -------------------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root():
    """Landing page with navigation."""
    # Retrieve the 'power' variable from the 'bp.globals' table
    query = "SELECT value FROM bnp.globals WHERE variable_name = 'power';"
    df = get_table(query)
    if not df.empty:
        power_status = df.iloc[0]["value"]
    else:
        power_status = "off"  # Default to 'off' if not found

    # Determine which GIF and action to display
    if power_status == "on":
        gif_url = "https://media.giphy.com/media/fnkyJXcCXZngY/giphy.gif"  # GIF for 'on' state
        action_text = "SHUT DOWN"
        action_link = "/shutdown"
    else:
        gif_url = "https://media.giphy.com/media/1yYWGu3caE3m0/giphy.gif"  # GIF for 'off' state
        action_text = "START UP"
        action_link = "/startup"

    df = get_system_metrics()
    sys_info_table = itables.to_html_datatable(
        df,
        style="table-layout:auto;width:100%;",
        classes="table table-striped table-bordered",
        showIndex=False,
    )
    # Build the HTML content
    html_content = f"""
    <html>
        <head>
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        </head>
        <body>
            {get_navigation_table()}
            <center>
            <div class="container">
                <h1>BNP Supervision Dashboard</h1>
         
                <p>Welcome to the BNP Supervision Dashboard.
                   Use the navigation above to explore various views.
                   You may also starve the workers by shutting the feed off below:</p>
                <img src="{gif_url}" width="480" height="293" frameBorder="0"></iframe>
                <p><a href="{action_link}" class="btn btn-primary">{action_text}</a></p>
                {sys_info_table} </center>
            </div>
            
        </body>
    </html>
    """

    return HTMLResponse(content=html_content)


@app.get("/startup")
async def startup():
    try:
        query = """
        UPDATE bnp.globals
        SET value = '"on"'
        WHERE variable_name = 'power';
        """
        with engine.begin() as connection:
            connection.execute(text(query))
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# -------------------------------------------------------------------------------------
# API GET                          /shutdown                                -->shutdown
# -------------------------------------------------------------------------------------
@app.get("/shutdown")
async def shutdown():
    try:
        query = """
        UPDATE bnp.globals
        SET value = '"off"'
        WHERE variable_name = 'power';
        """
        with engine.begin() as connection:
            connection.execute(text(query))
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# -------------------------------------------------------------------------------------
# API GET                     /status_summary                         -->status_summary
# -------------------------------------------------------------------------------------
@app.get("/status-summary", response_class=HTMLResponse)
async def status_summary():
    """View summarizing the job statuses."""
    status_query = "SELECT status, COUNT(*) AS count FROM bnp.process_executions GROUP BY status ORDER BY status;"

    status_df = get_table(status_query)
    status_df["status"] = status_df["status"].map(lambda x: format_status(x))

    status_table = itables.to_html_datatable(
        status_df, style="table-layout:auto;width:100%;"
    )
    return HTMLResponse(
        content=f"""
        <html>
            <head>
                {reload}
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                <style>{css}</style>
            </head>
            <body>
                {get_navigation_table()}
                <div class="container">
                    <h1>Status Summary</h1>
                    <div class="table-responsive">{status_table}</div>
                </div>
            </body>
        </html>
        """
    )


# -------------------------------------------------------------------------------------
# API GET                       /workers                              ->workers_summary
# -------------------------------------------------------------------------------------
@app.get("/workers", response_class=HTMLResponse)
async def workers_summary():
    def get_worker_name_with_link(row):
        return (
            f'<a href="/products?worker_id={row["worker_id"]}">{row["worker_id"]}</a>'
        )

    query = "SELECT * from bnp.workers_view order by last_seen desc;"
    df = get_table(query)
    if df.empty:
        return HTMLResponse(
            content=f"""
            <html>
                <head>
                    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                </head>
                <body>
                   {get_navigation_table()}
                    <h1>No workers found</h1>
                </body>
            </html>
            """
        )
    df["worker_id"] = df.apply(get_worker_name_with_link, axis=1)
    html_table = itables.to_html_datatable(df, style="table-layout:auto;width:100%;")
    return HTMLResponse(
        content=f"""
        <html>
            <head>
                
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                <style>{css}</style>
            </head>
            <body>
               {get_navigation_table()}
                <div class="container">
                    <h1>Status Summary</h1>
                    <div class="table-responsive">{html_table}</div>
                </div>
            </body>
        </html>
        """
    )


# -------------------------------------------------------------------------------------
# INTERNAL                       get_product_from_job_id
# -------------------------------------------------------------------------------------
def get_product_from_job_id(job_id: int) -> str:
    """
    Fetch the product name for a given job ID by querying the database.
    """
    query = "SELECT bnp.get_product_from_job_id(:job_id) AS product_name"
    params = {"job_id": job_id}

    try:
        # Assuming `get_table` handles database connection and query execution
        result = get_table(query, params)
        if len(result) == 0:
            return "Unknown Product"
        return result.iloc[0]["product_name"]  # Assuming result is a DataFrame
    except Exception as e:
        print(f"Error fetching product name for job_id {job_id}: {e}")
        return "Unknown Product"


# -------------------------------------------------------------------------------------
# API GET                     /products                              ->products_summary
# -------------------------------------------------------------------------------------
@app.get("/products", response_class=HTMLResponse)
async def products_summary(
    worker_id: Optional[str] = None,
    only_failed: Optional[bool] = False,
    auto_refresh: Optional[bool] = False,
):
    """View summarizing the worker status."""
    query = "SELECT * from bnp.products_view"
    params = {}
    if worker_id:
        query += " WHERE worker_id = :worker_id"
        if only_failed:
            query += " AND status='failed'"
        params = {"worker_id": worker_id}

    elif only_failed:
        query += " WHERE status='failed'"
    df = get_table(query, params)
    df["status"] = df["status"].map(lambda x: format_status(x))

    def get_product_name_with_link(row):
        """
        Create a clickable link for the product name.
        """
        product_name = get_product_name_from_uri(row["source_path"])
        job_id = row["job_id"]  # Assuming the job_id column exists in the view
        return f'<a href="/logs/{job_id}">{product_name}</a>'

    def get_product_name_from_uri(uri: str):
        fields = uri.split("MSIL1C_")
        if len(fields) < 2:
            print("........................................................", uri)
            return uri
        return f"S2A_MSIL1C{fields[1]}".replace(".SAFE", "")

    # Apply the link transformation
    if len(df) > 0:
        df["source_path"] = df.apply(get_product_name_with_link, axis=1)
        df["err_msg"] = df["err_msg"].map(lambda x: "" if x is None else x)

        df = df[["source_path", "acquisition_date","tile_name", "total_execution_time", "status", "err_msg"]]
        html_table = itables.to_html_datatable(
            df, style="table-layout:auto;width:100%;"
        )
        histogram = get_histogram(df)
        if histogram:
            histogram_image = f'<img src="data:image/png;base64,{histogram}" alt="No Overview Image Available" style="max-width: 100%;"/>'

        else:
            histogram_image = "No histogram Available"

        if worker_id:
            html_table += f" for worker {worker_id}"
    worker_presentation = f" for worker {worker_id} " if worker_id else ""

    # Determine auto-refresh state and toggle link
    auto_refresh_toggle = "ON" if not auto_refresh else "OFF"
    new_auto_refresh = "true" if not auto_refresh else "false"



    return HTMLResponse(
        content=f"""
        <html>
            <head>
                {'<meta http-equiv="refresh" content="10">' if auto_refresh else ''}
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                <style>{css}</style>
            </head>
            <body>
                {get_navigation_table()}
                <div class="container">
                    <h1>Products Summary {worker_presentation}</h1>
                    {histogram_image}
                    <div class="mb-3">
                        <a href="/products?worker_id={worker_id or ''}&only_failed={str(only_failed).lower()}&auto_refresh={new_auto_refresh}" 
                           class="btn btn-primary">
                           Auto Refresh {auto_refresh_toggle}
                        </a>
                    </div>
                    <div class="table-responsive">{html_table}</div>
                </div>
            </body>
        </html>
        """
    )


# -------------------------------------------------------------------------------------
# API GET                        /logs{job_id}                       ->get_logs_for_job
# -------------------------------------------------------------------------------------
@app.get("/logs/{job_id}", response_class=HTMLResponse)
async def get_logs_for_job(
    job_id: int, product_id: Optional[str] = None, auto_refresh: Optional[bool] = True
):
    """
    Endpoint to fetch logs for a specific job ID.

    Args:
        job_id (int): The job ID to fetch logs for.
        product_id (str): Optional product ID.
        auto_refresh (bool): Whether auto-refresh is enabled.

    Returns:
        HTMLResponse: A rendered HTML table of logs for the job ID.
    """
    query = """
        SELECT *
        FROM bnp.get_logs_from_job_id(:p_job_id);
    """
    parameters = {"p_job_id": job_id}
    logs_df = get_table(query, parameters)

    dst_path = get_dest_path_from_job_id(job_id=job_id)
    overview_image_path  =   dst_path + "/overview.jpg"
        
    if overview_image_path:
        overview_image = load_image_from_dest_path(overview_image_path)
        overview_image = f'<img src="data:image/png;base64,{overview_image}" alt="No Overview Image Available" style="max-height: 350px;"/>'
    else:
        overview_image = "No Overview available"

    esa_overview_image_path = esaL2A_from_desL2A(dst_path=dst_path)
    if esa_overview_image_path:
        esa_overview_image = load_image_from_dest_path(esa_overview_image_path)
        esa_overview_image = f'<img src="data:image/png;base64,{esa_overview_image}" alt="No Overview Image Available" style="max-width: 100%;"/>'
    else:
        esa_overview_image = "No Overview available"


    job_id_str = f"{job_id}-{get_product_from_job_id(job_id=job_id)}"

    if logs_df.empty:
        return HTMLResponse(
            content=f"""
            <html>
                <head>
                    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                </head>
                <body>
                   {get_navigation_table()}
                    <h1>No logs found</h1> for Job ID {job_id_str}
                </body>
            </html>
            """
        )

    logs_table = itables.to_html_datatable(
        logs_df, style="table-layout:auto;width:100%;"
    )

    auto_refresh_toggle = "ON" if not auto_refresh else "OFF"
    new_auto_refresh = "true" if not auto_refresh else "false"

    return HTMLResponse(
        content=f"""
        <html>
            <head>
                {'<meta http-equiv="refresh" content="10">' if auto_refresh else ''}
                <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
                <style>{css}</style>
            </head>
            <body>
               {get_navigation_table()}
                <div class="container">
                    <h2>Logs for Job ID {job_id_str}</h2>
                    <div class="mb-3">
                        <a href="/logs/{job_id}?product_id={product_id or ''}&auto_refresh={new_auto_refresh}" 
                           class="btn btn-primary">
                           Auto Refresh {auto_refresh_toggle}
                        </a>
                    </div>
                   <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <th style="text-align: center; border: 1px solid black;">Force Overview</th>
                            <th style="text-align: center; border: 1px solid black;">ESA Quickview</th>
                        </tr>
                        <tr>
                            <td style="text-align: center; border: 1px solid black;">
                                {overview_image} 
                            </td>
                            <td style="text-align: center; border: 1px solid black;">
                                 {esa_overview_image} 
                            </td>
                        </tr>
                    </table>
                    <div class="table-responsive">{logs_table}</div>
                </div>
            </body>
        </html>
        """
    )
