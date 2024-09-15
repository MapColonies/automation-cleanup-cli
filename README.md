# automation-cleanup-cli
This service is used for cleaning our resources from unnecessary data from our tests ( nightly, manual tests.. )The data which will be removed : tiles from S3, DB records such as mapproxy, jobs & tasks and catalog records.

## Configuration

Ensure the following essential values are provided before starting the test:

- `CONF_FILE`: Contains details for connections to DB, S3 and additional routes.
- `LOGS_FILE_PATH`: Path for the file contains all the logs of the cleanup process

## Test Execution
After providing all the essential values, run the file cleanup_script.py. The process involves:
1. For each layer: 
   - Sending request to S3 for deleting it's tiles 
   - Sending request to the DB for deleting the layer from the catalog 
   - Sending request to the DB for deleting the job and its related tasks 
2. Sending request to the DB for deleting all the layers from the mapproxy config
3. Logging the cleanup process into a log file 

