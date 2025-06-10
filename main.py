from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Optional, Tuple
from fastapi import FastAPI, UploadFile, Form, HTTPException
import pyodbc
import uuid
import os
import re
import pandas as pd
import shutil
from openpyxl import load_workbook
from openpyxl_image_loader import SheetImageLoader
import boto3
import requests
import json
import logging
import urllib.parse
import mimetypes
from datetime import datetime
import aiohttp
from pathlib import Path
import csv

# These are assumed to be in your project directory
from config import VERSION
from email_utils import send_message_email

# Initialize FastAPI app
app = FastAPI(title="iconluxury.group", version=VERSION)

# --- Pydantic Models ---

# Lightweight job model for initial list
class JobSummary(BaseModel):
    id: int
    inputFile: str
    fileEnd: str | None
    user: str
    rec: int
    img: int

# Full job details model
class ResultItem(BaseModel):
    resultId: int
    entryId: int
    imageUrl: str
    imageDesc: str | None
    imageSource: str | None
    createTime: str | None
    imageUrlThumbnail: str | None
    sortOrder: int
    imageIsFashion: int
    aiCaption: str | None
    aiJson: str | None
    aiLabel: str | None

class RecordItem(BaseModel):
    entryId: int
    fileId: int
    excelRowId: int
    productModel: str | None
    productBrand: str | None
    createTime: str | None
    step1: str | None
    step2: str | None
    step3: str | None
    step4: str | None
    completeTime: str | None
    productColor: str | None
    productCategory: str | None
    excelRowImageRef: str | None

class JobDetails(BaseModel):
    id: int
    inputFile: str
    imageStart: str | None
    fileStart: str | None
    fileEnd: str | None
    resultFile: str | None
    fileLocationUrl: str | None
    logFileUrl: str | None
    user: str
    rec: int
    img: int
    apiUsed: str
    imageEnd: str | None
    results: list[ResultItem]
    records: list[RecordItem]

# Model for domain aggregation
class DomainAggregation(BaseModel):
    domain: str
    totalResults: int
    positiveSortOrderCount: int

# Model for reference data
class ReferenceData(BaseModel):
    data: Dict[str, str]

# Response model for supplier offer summary
class OfferSummary(BaseModel):
    id: int
    fileName: str
    fileLocationUrl: str
    userEmail: Optional[str]
    createTime: Optional[str]
    recordCount: int
    nikOfferCount: int

# Response model for detailed offer data
class OfferDetails(BaseModel):
    id: int
    fileName: str
    fileLocationUrl: str
    userEmail: Optional[str]
    createTime: Optional[str]
    recordCount: int
    nikOfferCount: int
    sampleRecords: List[dict]
    sampleNikOffers: List[dict]


# --- Configuration and Setup ---

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS", "POST"],
    allow_headers=["*"],
)

# MSSQL connection settings
DB_CONFIG = {
    "server": "35.172.243.170",
    "database": "luxurymarket_p4",
    "username": "luxurysitescraper",
    "password": "Ftu5675FDG54hjhiuu$",
    "driver": "{ODBC Driver 17 for SQL Server}",
}

# AWS S3 and Cloudflare R2 configuration
S3_CONFIG = {
    "endpoint": "https://s3.us-east-2.amazonaws.com",
    "region": "us-east-2",
    "access_key": "AKIA2CUNLEV6V627SWI7",
    "secret_key": "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",
    "bucket_name": "iconluxurygroup",
    "r2_endpoint": "https://aa2f6aae69e7fb4bd8e2cd4311c411cb.r2.cloudflarestorage.com",
    "r2_access_key": "8b5a4a988c474205e0172eab5479d6f2",
    "r2_secret_key": "8ff719bbf2946c1b6a81fcf2121e1a41604a0b6f2890f308871b381e98a8d725",
    "r2_account_id": "aa2f6aae69e7fb4bd8e2cd4311c411cb",
    "r2_bucket_name": "iconluxurygroup",
    "r2_custom_domain": "https://iconluxury.group",
}

# Set up logging
logging.basicConfig(level=logging.INFO)
default_logger = logging.getLogger(__name__)


# --- Helper Functions ---

def get_db_connection():
    """Establishes a connection to the MSSQL database."""
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

def get_s3_client(service='s3', logger=None, file_id=None):
    """Creates a Boto3 client for either AWS S3 or Cloudflare R2."""
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger.info(f"Setup logger for get_s3_client, FileID: {file_id}")
    
    try:
        logger.info(f"Creating {service.upper()} client")
        if service == 'r2':
            client = boto3.client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key']
            )
        else:
            client = boto3.client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key']
            )
        logger.info(f"{service.upper()} client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating {service.upper()} client: {e}", exc_info=True)
        raise

def double_encode_plus(filename, logger=None):
    """URL-encodes a filename, specifically handling '+' characters."""
    logger = logger or default_logger
    logger.debug(f"Encoding filename: {filename}")
    first_pass = filename.replace('+', '%2B')
    second_pass = urllib.parse.quote(first_pass)
    logger.debug(f"Double-encoded filename: {second_pass}")
    return second_pass

def upload_to_s3(local_file_path, bucket_name, s3_key, r2_bucket_name=None, logger=None, file_id=None):
    """Uploads a local file to AWS S3 and optionally to Cloudflare R2."""
    logger = logger or default_logger
    if logger == default_logger and file_id:
        logger.info(f"Setup logger for upload_to_s3, FileID: {file_id}")
    
    result_urls = {}
    
    content_type, _ = mimetypes.guess_type(local_file_path)
    if not content_type:
        content_type = 'application/octet-stream'
        logger.warning(f"Could not determine Content-Type for {local_file_path}")
    
    try:
        s3_client = get_s3_client(service='s3', logger=logger, file_id=file_id)
        logger.info(f"Uploading {local_file_path} to S3: {bucket_name}/{s3_key}")
        s3_client.upload_file(
            local_file_path,
            bucket_name,
            s3_key,
            ExtraArgs={'ACL': 'public-read', 'ContentType': content_type}
        )
        double_encoded_key = double_encode_plus(s3_key, logger=logger)
        s3_url = f"https://{bucket_name}.s3.{S3_CONFIG['region']}.amazonaws.com/{double_encoded_key}"
        logger.info(f"Uploaded {local_file_path} to S3: {s3_url} with Content-Type: {content_type}")
        result_urls['s3'] = s3_url
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to S3: {e}")
        raise
    
    if r2_bucket_name:
        try:
            r2_client = get_s3_client(service='r2', logger=logger, file_id=file_id)
            logger.info(f"Uploading {local_file_path} to R2: {r2_bucket_name}/{s3_key}")
            r2_client.upload_file(
                local_file_path,
                r2_bucket_name,
                s3_key,
                ExtraArgs={'ACL': 'public-read', 'ContentType': content_type}
            )
            double_encoded_key = double_encode_plus(s3_key, logger=logger)
            r2_url = f"{S3_CONFIG['r2_custom_domain']}/{double_encoded_key}"
            logger.info(f"Uploaded {local_file_path} to R2: {r2_url} with Content-Type: {content_type}")
            result_urls['r2'] = r2_url
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path} to R2: {e}")
            raise
    
    return result_urls

async def send_file_details_email(
    to_email: str,
    file_id: int,
    filename: str,
    s3_url: str,
    r2_url: str | None,
    record_count: int,
    nikoffer_count: int,
    user_email: str | None
) -> bool:
    """Sends a notification email and triggers a backend job restart."""
    try:
        subject = f"File Upload Notification - File ID: {file_id}"
        restart_job_url = f"https://icon7-8080.iconluxury.today/api/v4/restart-search-all/{file_id}"
        
        message = (
            "File Upload Notification\n"
            f"File ID: {file_id}\n"
            f"File Name: {filename}\n"
            f"S3 URL: {s3_url}\n"
            f"R2 URL: {r2_url or 'Not available'}\n"
            f"Upload Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Records Processed: {record_count}\n"
            f"Nikoffer Records Processed: {nikoffer_count}\n"
            f"Uploaded By: {user_email or 'Unknown'}\n"
            f"Environment: iconluxury.group\n"
            f"Version: {VERSION}\n"
        )
        
        default_logger.info(f"Preparing to send email to {to_email} with subject: {subject}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(restart_job_url, headers={"accept": "application/json"}) as response:
                if response.status == 200:
                    default_logger.info(f"Successfully triggered restart search job for file ID {file_id}")
                else:
                    default_logger.error(f"Failed to trigger restart search job for file ID {file_id}: {response.status}")

        success = await send_message_email(to_emails=to_email, subject=subject, message=message, logger=default_logger)
        
        if success:
            default_logger.info(f"Email sent successfully to {to_email}")
            return True
        else:
            default_logger.error(f"Failed to send email to {to_email}")
            return False
            
    except Exception as e:
        default_logger.error(f"Error sending file details email to {to_email}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")

def validate_column(col: str) -> str:
    """Validates that a string is a valid Excel column name (e.g., 'A', 'AA')."""
    if not col or not re.match(r"^[A-Z]+$", col):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid column name: {col}. Must be uppercase letters only (e.g., 'A', 'B', 'AA')."
        )
    return col

def insert_file_db(filename: str, file_url: str, email: Optional[str], header_index: int, file_type: int ,logger=None) -> int:
    """Inserts a new file record into the utb_ImageScraperFiles table."""
    logger = logger or default_logger
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO utb_ImageScraperFiles (FileName, FileLocationUrl, UserEmail, UserHeaderIndex, FileTypeID, CreateFileStartTime)
            OUTPUT INSERTED.ID
            VALUES (?, ?, ?, ?, ?, GETDATE())
        """
        cursor.execute(query, (filename, file_url, email or 'nik@accessx.com', str(header_index), file_type))
        row = cursor.fetchone()
        if row is None or row[0] is None:
            raise Exception("Insert failed or no identity value returned.")
        file_id = int(row[0])
        conn.commit()
        conn.close()
        logger.info(f"Inserted file record with ID: {file_id}, FileTypeID: {file_type}, header_index: {header_index}")
        return file_id
    except Exception as e:
        logger.error(f"Error in insert_file_db: {e}", exc_info=True)
        raise

def load_payload_db(rows, file_id, column_map, logger=None):
    """Loads extracted data into the utb_ImageScraperRecords table."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with get_db_connection() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"FileID {file_id} does not exist in utb_ImageScraperFiles")

            df = pd.DataFrame(rows)
            rename_dict = {
                'search': 'ProductModel', 'brand': 'ProductBrand', 'color': 'ProductColor',
                'category': 'ProductCategory', 'ExcelRowImageRef': 'ExcelRowImageRef'
            }
            df = df.rename(columns=rename_dict)
            
            if column_map.get('brand') == 'MANUAL':
                df['ProductBrand'] = column_map.get('manualBrand', '')

            df['FileID'] = file_id
            df['ExcelRowID'] = range(1, len(df) + 1)
            
            expected_cols = ['FileID', 'ExcelRowID', 'ProductModel', 'ProductBrand', 'ProductColor', 'ProductCategory', 'ExcelRowImageRef']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
                df[col] = df[col].where(df[col].notna(), None)

            rows_inserted = 0
            for _, row in df.iterrows():
                row_values = [row.get(col, None) for col in expected_cols]
                cursor.execute(f"INSERT INTO utb_ImageScraperRecords ({', '.join(expected_cols)}) VALUES ({', '.join(['?'] * len(expected_cols))})", tuple(row_values))
                rows_inserted += 1

            connection.commit()
            logger.info(f"Committed {rows_inserted} rows into utb_ImageScraperRecords for FileID: {file_id}")
        return df
    except Exception as e:
        logger.error(f"Error loading payload data: {e}", exc_info=True)
        raise

def load_nikoffer_db(rows, file_id, headers=None, logger=None):
    """Loads extracted data into the utb_nikofferloadinitial table."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with get_db_connection() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"FileID {file_id} not found in utb_ImageScraperFiles")

            nik_columns = ['FileID'] + [f'f{i}' for i in range(41)]
            df = pd.DataFrame(rows)
            file_columns = headers if headers else df.columns.tolist()
            mapped_columns = file_columns[:41]

            rows_inserted = 0
            for _, row in df.iterrows():
                row_values = [file_id]
                for col in mapped_columns:
                    value = row.get(col, None)
                    row_values.append(str(value) if value is not None else None)
                # Pad if necessary
                row_values.extend([None] * (len(nik_columns) - len(row_values)))
                
                cursor.execute(f"INSERT INTO utb_nikofferloadinitial ({', '.join(nik_columns)}) VALUES ({', '.join(['?'] * len(nik_columns))})", tuple(row_values))
                rows_inserted += 1

            connection.commit()
            logger.info(f"Committed {rows_inserted} rows into utb_nikofferloadinitial for FileID: {file_id}")
        return df
    except Exception as e:
        logger.error(f"Error loading nikoffer data: {e}", exc_info=True)
        raise

def load_offer_import_db(rows, file_id, logger=None):
    """Loads data from submitted offers into the utb_OfferImport table."""
    logger = logger or default_logger
    try:
        file_id = int(file_id)
        with get_db_connection() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM utb_ImageScraperFiles WHERE ID = ?", (file_id,))
            if cursor.fetchone()[0] == 0:
                raise ValueError(f"FileID {file_id} not found in utb_ImageScraperFiles")

            offer_columns = ['FileID'] + [f'f{i}' for i in range(41)]
            df = pd.DataFrame(rows).astype(str).where(pd.notna(rows), None)
            
            rows_inserted = 0
            for _, row in df.iterrows():
                row_values = [file_id] + list(row.values)[:41]
                row_values.extend([None] * (len(offer_columns) - len(row_values)))
                
                cursor.execute(f"INSERT INTO utb_OfferImport ({', '.join(offer_columns)}) VALUES ({', '.join(['?'] * len(offer_columns))})", tuple(row_values))
                rows_inserted += 1

            connection.commit()
            logger.info(f"Committed {rows_inserted} rows into utb_OfferImport for FileID: {file_id}")
        return df
    except Exception as e:
        logger.error(f"Error loading offer import data: {e}", exc_info=True)
        raise

def infer_column_map(extracted_data: List[Dict]) -> Dict[str, str]:
    """Infers column mappings from headers for generic file uploads."""
    if not extracted_data:
        return {}
    
    columns = list(extracted_data[0].keys())
    patterns = {
        'style': [r'model', r'style', r'sku', r'item'], 'brand': [r'brand', r'manufacturer'],
        'color': [r'color', r'colour'], 'category': [r'category', r'type'],
        'image': [r'image', r'photo', r'picture', r'excelrowimageref']
    }
    column_map = {}
    for field, field_patterns in patterns.items():
        for col in columns:
            if any(re.search(p, col.lower()) for p in field_patterns):
                if not column_map.get(field):
                    column_map[field] = col
                    break
    default_logger.info(f"Inferred column map: {column_map}")
    return column_map

def extract_data_and_images(
    file_path: str, 
    file_id: str, 
    column_map: Dict[str, str], 
    start_row: int, 
    manual_brand: Optional[str] = None
) -> Tuple[List[Dict], Optional[str]]:
    """Extracts data from an Excel file based on specified column letters, starting from a given row."""
    wb = load_workbook(file_path)
    sheet = wb.active
    image_loader = SheetImageLoader(sheet) if column_map.get('image') else None
    
    extracted_images_dir = None
    if column_map.get('image'):
        extracted_images_dir = os.path.join("temp_files", "extracted_images", file_id)
        os.makedirs(extracted_images_dir, exist_ok=True)

    default_logger.info(f"Processing Excel file with data starting at row: {start_row}, max_row: {sheet.max_row}")

    extracted_data = []
    for row_idx in range(start_row, sheet.max_row + 1):
        search_val = sheet[f'{column_map["style"]}{row_idx}'].value if column_map.get('style') else None
        if not search_val: # Skip row if primary search column is empty
            default_logger.info(f"Skipping row {row_idx} due to empty search column.")
            continue

        brand = manual_brand if column_map['brand'] == 'MANUAL' else (sheet[f'{column_map["brand"]}{row_idx}'].value if column_map.get('brand') else None)
        
        data = {
            'search': str(search_val),
            'brand': str(brand) if brand is not None else None,
            'color': str(sheet[f'{column_map["color"]}{row_idx}'].value) if column_map.get('color') else None,
            'category': str(sheet[f'{column_map["category"]}{row_idx}'].value) if column_map.get('category') else None,
            'ExcelRowImageRef': None
        }
        
        if column_map.get('image'):
            image_cell = f'{column_map["image"]}{row_idx}'
            image_ref = sheet[image_cell].value
            if not image_ref and image_loader and image_loader.image_in(image_cell):
                img_path = os.path.join(extracted_images_dir, f"image_{file_id}_{image_cell}.png")
                image = image_loader.get(image_cell)
                if image:
                    image.save(img_path)
                    s3_key = f"images/{file_id}/{os.path.basename(img_path)}"
                    urls = upload_to_s3(img_path, S3_CONFIG['bucket_name'], s3_key, r2_bucket_name=S3_CONFIG['r2_bucket_name'], logger=default_logger, file_id=file_id)
                    image_ref = urls['s3']
            data['ExcelRowImageRef'] = image_ref

        extracted_data.append(data)
    
    default_logger.info(f"Total rows extracted: {len(extracted_data)}")
    return extracted_data, extracted_images_dir

def extract_full_data_and_images(
    file_path: str,
    file_id: str,
    header_row: int
) -> Tuple[List[Dict], List[str], Optional[str]]:
    """Extracts all data and images from an Excel file, using a header row to define columns."""
    wb = load_workbook(file_path)
    sheet = wb.active
    image_loader = SheetImageLoader(sheet)
    extracted_images_dir = os.path.join("temp_files", "extracted_images", file_id)
    os.makedirs(extracted_images_dir, exist_ok=True)
    
    headers = [str(col.value) if col.value else f"Column_{col.column}" for col in sheet[header_row]]
    
    extracted_data = []
    for row_idx in range(header_row + 1, sheet.max_row + 1):
        row_values = [sheet.cell(row=row_idx, column=col_idx + 1).value for col_idx in range(len(headers))]
        if all(val is None for val in row_values):
            continue
        
        data = dict(zip(headers, row_values))
        
        for col_idx, header in enumerate(headers, start=1):
            cell = sheet.cell(row=row_idx, column=col_idx)
            cell_ref = f"{cell.column_letter}{row_idx}"
            if image_loader.image_in(cell_ref):
                img_path = os.path.join(extracted_images_dir, f"image_{file_id}_{cell_ref}.png")
                image = image_loader.get(cell_ref)
                if image:
                    image.save(img_path)
                    s3_key = f"images/{file_id}/{os.path.basename(img_path)}"
                    urls = upload_to_s3(img_path, S3_CONFIG['bucket_name'], s3_key, r2_bucket_name=S3_CONFIG['r2_bucket_name'], logger=default_logger, file_id=file_id)
                    image_col = next((h for h in headers if 'image' in h.lower()), 'ExcelRowImageRef')
                    if image_col not in headers: headers.append(image_col)
                    data[image_col] = urls['s3']
        extracted_data.append(data)
        
    return extracted_data, headers, extracted_images_dir


# --- API Endpoints ---

@app.post("/submitImage")
async def submit_image(
    fileUploadImage: UploadFile,
    header_index: int = Form(...),
    imageColumnImage: Optional[str] = Form(None),
    searchColImage: str = Form(...),
    brandColImage: str = Form(...),
    ColorColImage: Optional[str] = Form(None),
    CategoryColImage: Optional[str] = Form(None),
    sendToEmail: Optional[str] = Form(None),
    manualBrand: Optional[str] = Form(None),
):
    """Processes an Excel file with specific column mappings for image scraping."""
    temp_dir = None
    try:
        file_id = str(uuid.uuid4())
        if header_index < 1:
            raise HTTPException(status_code=400, detail="header_index must be 1 or greater (1-based row number for start of data).")
        
        temp_dir = os.path.join("temp_files", "images", file_id)
        os.makedirs(temp_dir, exist_ok=True)
        uploaded_file_path = os.path.join(temp_dir, fileUploadImage.filename)
        with open(uploaded_file_path, "wb") as buffer:
            shutil.copyfileobj(fileUploadImage.file, buffer)

        s3_key = f"uploads/{file_id}/{fileUploadImage.filename}"
        urls = upload_to_s3(uploaded_file_path, S3_CONFIG['bucket_name'], s3_key, S3_CONFIG['r2_bucket_name'], default_logger, file_id)
        
        column_map = {
            'brand': 'MANUAL' if brandColImage == 'MANUAL' else validate_column(brandColImage), 'style': validate_column(searchColImage),
            'image': validate_column(imageColumnImage) if imageColumnImage else None, 'color': validate_column(ColorColImage) if ColorColImage else None,
            'category': validate_column(CategoryColImage) if CategoryColImage else None, 'manualBrand': manualBrand
        }
        if column_map['brand'] == 'MANUAL' and not manualBrand:
            raise HTTPException(status_code=400, detail="manualBrand is required when brandColImage is 'MANUAL'")

        extracted_data, _ = extract_data_and_images(uploaded_file_path, file_id, column_map, header_index, manualBrand)
        
        file_id_db = insert_file_db(fileUploadImage.filename, urls['s3'], sendToEmail, header_index, 1, default_logger)
        load_payload_db(extracted_data, file_id_db, column_map, default_logger)
        
        await send_file_details_email(
            sendToEmail or "nik@luxurymarket.com", file_id_db, fileUploadImage.filename,
            urls['s3'], urls.get('r2'), len(extracted_data), 0, sendToEmail
        )
        return {"success": True, "s3_url": urls['s3'], "r2_url": urls.get('r2'), "file_id": file_id_db}
    except Exception as e:
        default_logger.error(f"Error in /submitImage: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

@app.post("/submitFullFile")
async def submit_full_file(
    fileUpload: UploadFile,
    header_index: int = Form(...),
    sendToEmail: Optional[str] = Form(None),
):
    """Processes a generic file (XLSX or CSV) by extracting all columns and storing them."""
    temp_dir = None
    try:
        file_id = str(uuid.uuid4())
        if header_index < 1:
            raise HTTPException(status_code=400, detail="header_index must be 1 or greater.")
        
        temp_dir = os.path.join("temp_files", "full_files", file_id)
        os.makedirs(temp_dir, exist_ok=True)
        uploaded_file_path = os.path.join(temp_dir, fileUpload.filename)
        with open(uploaded_file_path, "wb") as buffer:
            shutil.copyfileobj(fileUpload.file, buffer)
        
        s3_key = f"luxurymarket/supplier/offer/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}/{file_id}/{fileUpload.filename}"
        urls = upload_to_s3(uploaded_file_path, S3_CONFIG['bucket_name'], s3_key, S3_CONFIG['r2_bucket_name'], default_logger, file_id)
        
        file_extension = os.path.splitext(fileUpload.filename)[1].lower()
        if file_extension in ['.xlsx', '.xls']:
            extracted_data, headers, _ = extract_full_data_and_images(uploaded_file_path, file_id, header_index)
        elif file_extension == '.csv':
            df = pd.read_csv(uploaded_file_path, header=header_index - 1, encoding='utf-8', low_memory=False).where(pd.notnull(df), None)
            headers = [str(col) if col else f"Column_{i+1}" for i, col in enumerate(df.columns)]
            extracted_data = df.to_dict(orient='records')
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")

        file_id_db = insert_file_db(fileUpload.filename, urls['s3'], sendToEmail, header_index, 2, default_logger)
        
        column_map = infer_column_map(extracted_data)
        load_payload_db(extracted_data, file_id_db, column_map, default_logger)
        load_nikoffer_db(extracted_data, file_id_db, headers, default_logger)
        
        await send_file_details_email(
            sendToEmail or "nik@luxurymarket.com", file_id_db, fileUpload.filename,
            urls['s3'], urls.get('r2'), len(extracted_data), len(extracted_data), sendToEmail
        )
        return {"success": True, "s3_url": urls['s3'], "r2_url": urls.get('r2'), "file_id": file_id_db}
    except Exception as e:
        default_logger.error(f"Error in /submitFullFile: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

@app.post("/submitOffer")
async def submit_offer(
    fileUrl: str = Form(...),
    header_index: int = Form(...),
    sendToEmail: Optional[str] = Form(None),
):
    """Downloads a file from a URL, processes it, and stores the raw data."""
    temp_dir = None
    try:
        file_id = str(uuid.uuid4())
        if header_index < 1:
            raise HTTPException(status_code=400, detail="header_index must be 1 or greater.")
        
        temp_dir = os.path.join("temp_files", "offers", file_id)
        os.makedirs(temp_dir, exist_ok=True)

        response = requests.get(fileUrl)
        response.raise_for_status()
        filename = os.path.basename(urllib.parse.urlparse(fileUrl).path) or f"offer_{file_id}.tmp"
        
        uploaded_file_path = os.path.join(temp_dir, filename)
        with open(uploaded_file_path, "wb") as f:
            f.write(response.content)

        s3_key = f"luxurymarket/supplier/offer/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}/{file_id}/{filename}"
        urls = upload_to_s3(uploaded_file_path, S3_CONFIG['bucket_name'], s3_key, S3_CONFIG['r2_bucket_name'], default_logger, file_id)

        file_extension = os.path.splitext(filename)[1].lower()
        if file_extension in ['.xlsx', '.xls']:
            wb = load_workbook(uploaded_file_path)
            sheet = wb.active
            extracted_data = [
                [cell.value for cell in row]
                for row_idx, row in enumerate(sheet.iter_rows(), 1)
                if row_idx >= header_index and not all(c.value is None for c in row)
            ]
        elif file_extension == '.csv':
            with open(uploaded_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                extracted_data = [row for row_idx, row in enumerate(reader, 1) if row_idx >= header_index and any(row)]
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")

        file_id_db = insert_file_db(filename, urls['s3'], sendToEmail, header_index, 2, default_logger)
        load_offer_import_db(extracted_data, file_id_db, default_logger)

        await send_file_details_email(
            sendToEmail or "nik@luxurymarket.com", file_id_db, filename,
            urls['s3'], urls.get('r2'), len(extracted_data), 0, sendToEmail
        )
        return {"success": True, "s3_url": urls['s3'], "r2_url": urls.get('r2'), "file_id": file_id_db}
    except Exception as e:
        default_logger.error(f"Error in /submitOffer: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

@app.api_route("/api/update-references", methods=["GET", "POST"], response_model=ReferenceData)
async def update_references(updated_data: Optional[ReferenceData] = None):
    """GETs or POSTs a JSON reference file from/to S3."""
    s3_key = "optimal-references.json"
    if updated_data is None:  # GET
        try:
            s3_client = get_s3_client(service='s3')
            response = s3_client.get_object(Bucket=S3_CONFIG['bucket_name'], Key=s3_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return ReferenceData(data=data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
    else:  # POST
        try:
            with open("temp_references.json", "w") as f:
                json.dump(updated_data.data, f)
            upload_to_s3("temp_references.json", S3_CONFIG["bucket_name"], s3_key, S3_CONFIG['r2_bucket_name'], default_logger)
            os.remove("temp_references.json")
            return ReferenceData(data=updated_data.data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error saving data: {str(e)}")

@app.get("/api/scraping-jobs", response_model=list[JobSummary])
async def get_all_jobs(page: int = 1, page_size: int = 10):
    """Retrieves a paginated list of image scraping jobs."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            offset = (page - 1) * page_size
            query = """
                SELECT ID, FileName, CreateFileCompleteTime, UserEmail,
                    (SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = f.ID) as rec_count,
                    (SELECT COUNT(*) FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = f.ID)) as img_count
                FROM utb_ImageScraperFiles f WHERE FileTypeID = 1 ORDER BY ID DESC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            rows = cursor.execute(query, (offset, page_size)).fetchall()
            return [JobSummary(**dict(zip([c[0] for c in cursor.description], row))) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scraping-jobs/{job_id}", response_model=JobDetails)
async def get_job(job_id: int):
    """Retrieves detailed information for a specific scraping job."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM utb_ImageScraperFiles WHERE ID = ?", (job_id,))
            job_row = cursor.fetchone()
            if not job_row: raise HTTPException(status_code=404, detail="Job not found")
            job_data = dict(zip([c[0] for c in cursor.description], job_row))

            cursor.execute("SELECT * FROM utb_ImageScraperRecords WHERE FileID = ?", (job_id,))
            records = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]
            
            cursor.execute("SELECT * FROM utb_ImageScraperResult WHERE EntryID IN (SELECT EntryID FROM utb_ImageScraperRecords WHERE FileID = ?)", (job_id,))
            results = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

            return JobDetails(
                id=job_data['ID'], inputFile=job_data['FileName'], user=job_data['UserEmail'],
                imageStart=job_data.get('ImageStartTime'), fileStart=job_data.get('CreateFileStartTime'),
                fileEnd=job_data.get('CreateFileCompleteTime'), resultFile=job_data.get('FileLocationURLComplete'),
                fileLocationUrl=job_data.get('FileLocationUrl'), logFileUrl=job_data.get('LogFileURL'),
                rec=len(records), img=len(results), apiUsed="google-serp", imageEnd=job_data.get('ImageCompleteTime'),
                results=[ResultItem(**r) for r in results],
                records=[RecordItem(**r) for r in records]
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/whitelist-domains", response_model=List[DomainAggregation])
async def get_whitelist_domains():
    """Aggregates and returns statistics on domains found in scraping results."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT ImageSource, SortOrder FROM utb_ImageScraperResult"
            rows = cursor.execute(query).fetchall()
            domain_data = {}
            for row in rows:
                try:
                    domain = urllib.parse.urlparse(row.ImageSource).hostname.replace("www.", "") if row.ImageSource else "unknown"
                except:
                    domain = "unknown"
                
                if domain not in domain_data:
                    domain_data[domain] = {"totalResults": 0, "positiveSortOrderCount": 0}
                domain_data[domain]["totalResults"] += 1
                if row.SortOrder and row.SortOrder > 0:
                    domain_data[domain]["positiveSortOrderCount"] += 1
            return [DomainAggregation(domain=d, **data) for d, data in domain_data.items()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/luxurymarket/supplier/offers", response_model=List[OfferSummary])
async def list_supplier_offers(page: int = 1, page_size: int = 10):
    """Lists supplier offers with pagination."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            offset = (page - 1) * page_size
            query = """
                SELECT ID, FileName, FileLocationUrl, UserEmail, CreateFileStartTime,
                    (SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = f.ID) as record_count,
                    (SELECT COUNT(*) FROM utb_nikofferloadinitial WHERE FileID = f.ID) as nikoffer_count
                FROM utb_ImageScraperFiles f WHERE FileTypeID = 2 ORDER BY CreateFileStartTime DESC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            rows = cursor.execute(query, (offset, page_size)).fetchall()
            return [OfferSummary(**dict(zip([c[0] for c in cursor.description], row))) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/luxurymarket/supplier/offers/{offer_id}", response_model=OfferDetails)
async def get_supplier_offer(offer_id: int):
    """Gets detailed information about a specific supplier offer."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query_offer = """
                SELECT ID, FileName, FileLocationUrl, UserEmail, CreateFileStartTime,
                    (SELECT COUNT(*) FROM utb_ImageScraperRecords WHERE FileID = f.ID) as record_count,
                    (SELECT COUNT(*) FROM utb_nikofferloadinitial WHERE FileID = f.ID) as nikoffer_count
                FROM utb_ImageScraperFiles f WHERE ID = ? AND FileTypeID = 2
            """
            offer_row = cursor.execute(query_offer, (offer_id,)).fetchone()
            if not offer_row:
                raise HTTPException(status_code=404, detail="Supplier offer not found")
            offer_data = dict(zip([c[0] for c in cursor.description], offer_row))

            cursor.execute("SELECT TOP 5 * FROM utb_ImageScraperRecords WHERE FileID = ? ORDER BY ExcelRowID", (offer_id,))
            sample_records = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

            cursor.execute("SELECT TOP 5 * FROM utb_nikofferloadinitial WHERE FileID = ? ORDER BY FileID", (offer_id,))
            sample_nikoffers = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]
            
            return OfferDetails(**offer_data, sampleRecords=sample_records, sampleNikOffers=sample_nikoffers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)