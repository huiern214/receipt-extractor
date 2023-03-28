'''
receipt_extractor.py - workflow

Download image from Google Drive, archive to Google Cloud Storage, send
to Google Cloud Vision for processing, add results row to Google Sheet.
'''

from __future__ import print_function
import argparse
import base64
import io, os
import webbrowser

from googleapiclient import discovery, http
from httplib2 import Http
from oauth2client import file, client, tools
from google.cloud import storage

BUCKET = 'BUCKET_NAME' # BUCKET NAME
PARENT = 'receipts' # YOUR IMG FILE PREFIX / FOLDER IN BUCKET 
SHEET = 'SHEETS_ID' # SHEETS ID
DEBUG = False

# process credentials for OAuth2 tokens
SCOPES = (
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/cloud-vision',
    'https://www.googleapis.com/auth/spreadsheets',
)

# create storage.json in an existing directory/folder
# dir_name = 'Trial1'
# store = file.Storage(os.path.join(dir_name, 'storage.json'))
store = file.Storage('storage.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    # if client_secret.json is in an existing directory/folder
    # flow = client.flow_from_clientsecrets(os.path.join(dir_name, 'client_secret.json'), SCOPES) 
    creds = tools.run_flow(flow, store)

# create API service endpoints
HTTP = creds.authorize(Http())
DRIVE  = discovery.build('drive',   'v3', http=HTTP)
GCS    = discovery.build('storage', 'v1', http=HTTP)
VISION = discovery.build('vision',  'v1', http=HTTP)
SHEETS = discovery.build('sheets',  'v4', http=HTTP)

def drive_get_img():
    'download files from Drive and return file info & binary if found'
    folder_id = 'FOLDER_ID' # search for file on Google Drive in the given directory
    query = "'%s' in parents and trashed=false" % (folder_id) # list all files in the drive folder
    rsp = DRIVE.files().list(q=query, fields='files(id,name,mimeType,modifiedTime)').execute().get('files', [])
    
    # Return file info if found, else return None
    if rsp:
        return rsp


def gcs_blob_upload(fname, bucket, media, mimetype):
    'upload an object to a Google Cloud Storage bucket'

    # build blob metadata and upload via GCS API
    body = {'name': fname, 'uploadType': 'multipart', 'contentType': mimetype}
    return GCS.objects().insert(bucket=bucket, body=body,
            media_body=http.MediaIoBaseUpload(io.BytesIO(media), mimetype),
            fields='bucket,name').execute()


def vision_detect_text_img(img):
    'send image to Vision API for text annotation'

    # build image metadata and call Vision API to process
    body = {'requests': [{
                'image': {'content': img},
                'features': {'type': 'TEXT_DETECTION'},
    }]}
    rsp = VISION.images().annotate(body=body).execute().get('responses', [{}])[0]

    def containsCaseInsensitive(substring, string):
        if substring.lower() in string.lower():
            return True
        else:
            return False
            
    extracted_text, date, total_price = '', '',''
    # return extracted text from image and parsed data for Sheet (row)
    # Since the format of receipts may vary, the text parsing requires constant bug fixing and updating.
    if 'textAnnotations' in rsp:
        extracted_text = rsp.get('textAnnotations', [{}])[0].get('description', '')
        # print(extracted_text)
        lines = extracted_text.split("\n")
        shop_name = lines[0] #shope name
        shop_address = lines[1] + " " + lines[2] + " " + lines[3] # shop address / location

        for line in lines:
            if containsCaseInsensitive("Date", line):
                date = line.split(" ")[1] # purchase date
            if containsCaseInsensitive("Total", line):
                if containsCaseInsensitive("RM", line):
                    total_price = line.split("RM")[-1] # total price
                else: 
                    total_price = line.split(" ")[-1]
    
    return rsp, extracted_text, shop_name, shop_address, date, total_price


def sheet_append_row(sheet, row):
    'append row to a Google Sheet, return #cells added'

    # call Sheets API to write row to Sheet (via its ID)
    rsp = SHEETS.spreadsheets().values().append(
            spreadsheetId=sheet, range='Sheet1',
            valueInputOption='USER_ENTERED', body={'values': [row]}
    ).execute()
    if rsp:
        return rsp.get('updates').get('updatedCells')


def main(bucket, sheet_id, folder, debug):
    '"main()" drives process from image download through report generation'

    # download img file & info from Drive
    rsp = drive_get_img() # get a list of files from Drive
    if not rsp:
        return
    if debug:
        print(rsp)
        print()

    # process the img files one by one using loop
    for target in rsp: 
        fileId = target['id']
        fname = target['name']
        mtype = target['mimeType']
        data = DRIVE.files().get_media(fileId=fileId).execute() # binary data
        ftime = target['modifiedTime']

        # Create a client object
        ## for local run, https://cloud.google.com/docs/authentication/client-libraries
        # client = storage.Client(project="cloud-workshop-380401")
        ## for remote run in the Cloud Shell Editor,
        client = storage.Client()
        # List all the files (blobs) in the bucket with the specified prefix (folder/directory)
        blobs = client.get_bucket(bucket).list_blobs(prefix=folder)
        files=[a.name for a in blobs]
        # Check if the filename of the image in Google Drive exists in the Google Cloud Storage bucket.
        if (folder + '/' + fname) in files:
            continue

        # upload file to GCS
        gcsname = '%s/%s'% (folder, fname)
        rsp = gcs_blob_upload(gcsname, bucket, data, mtype)
        if not rsp:
            return
        if debug:
            print('Uploaded %r to GCS bucket %r\n' % (rsp['name'], rsp['bucket']))

        # process w/Vision
        rsp, extracted_text, shop_name, shop_address, date, total_price = vision_detect_text_img(base64.b64encode(data).decode('utf-8'))
        if not rsp:
            return
        if debug:
            print('Extracted text from Vision API response:\n %s\n' % (extracted_text))

        # push results to Sheet, get cells-saved count
        row = [date,
                '=HYPERLINK("storage.cloud.google.com/%s/%s", "%s")' % (
                bucket, gcsname, fname), shop_name, shop_address, total_price, ftime
        ]
        rsp = sheet_append_row(sheet_id, row)
        if not rsp:
            return
        if debug:
            print('Added %d cells to Google Sheet\n' % rsp)
    return True


if __name__ == '__main__':
    # args: [-hv] [-b bucket] [-f folder] [-s Sheet ID]
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bucket_id", action="store_true",
            default=BUCKET, help="Google Cloud Storage bucket name")
    parser.add_argument("-f", "--folder", action="store_true",
            default=PARENT, help="Google Cloud Storage image folder")
    parser.add_argument("-s", "--sheet_id", action="store_true",
            default=SHEET, help="Google Sheet Drive file ID (44-char str)")
    parser.add_argument("-v", "--verbose", action="store_true",
            default=DEBUG, help="verbose display output")
    args = parser.parse_args()

    print('Processing files... please wait')
    rsp = main(args.bucket_id, args.sheet_id, args.folder, args.verbose)
    if rsp:
        sheet_url = 'https://docs.google.com/spreadsheets/d/%s/edit' % args.sheet_id
        print('DONE: opening web browser to it, or see %s' % sheet_url)
        webbrowser.open(sheet_url, new=1, autoraise=True)
    else:
        print('ERROR: could not process')