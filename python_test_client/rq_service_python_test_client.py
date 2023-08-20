import grpc
import service_pb2 as pb2
import service_pb2_grpc as pb2_grpc
import hashlib
import logging
import traceback
import time
import os
import sqlite3
import random
import base58

logging.basicConfig(level=logging.INFO)


def compute_sha3_256(file_path):
    hasher = hashlib.sha3_256()
    hasher.update(open(file_path, 'rb').read())
    hash_value = base58.b58encode(hasher.digest()).decode()
    logging.info(f"Computed SHA3-256 hash of file {file_path}: {hash_value}")
    return hash_value

def write_rq_symbol_files_from_db(db_path, output_dir, original_hash):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Generate a fake task_id dynamically
        fake_task_id = ''.join(random.choice('0123456789abcdef') for _ in range(8))
        # Create the directory structure for the symbols
        symbols_path = os.path.join(output_dir, fake_task_id, "symbols")
        os.makedirs(symbols_path, exist_ok=True)
        logging.info(f"Created directory {symbols_path}")
        # Query to retrieve the RQ symbol files
        query = f"SELECT rq_symbol_file_sha3_256_hash, rq_symbol_file_data FROM rq_symbols WHERE original_file_sha3_256_hash = '{original_hash}'"
        logging.info(f"Querying the database with query: {query}")
        cursor.execute(query)
        results = cursor.fetchall()  # Store the results in a variable
        logging.info(f"Number of results found in db: {len(results)}")
        delete_hashes = []  # List to store hashes for deletion
        for row in results:  # Iterate over the stored results
            symbol_hash, symbol_data = row
            delete_hashes.append(symbol_hash)  # Append the hash to the deletion list
            symbol_file_path = os.path.join(symbols_path, symbol_hash)
            with open(symbol_file_path, "wb") as symbol_file:
                symbol_file.write(symbol_data)
        # Delete the corresponding entries from the database
        delete_query = "DELETE FROM rq_symbols WHERE rq_symbol_file_sha3_256_hash IN ({})".format(','.join('?' for _ in delete_hashes))
        cursor.execute(delete_query, delete_hashes)
        conn.commit()  # Commit the transaction
        logging.info(f"Successfully wrote RQ symbol files for fake task_id {fake_task_id} to {symbols_path}")
        return symbols_path
    except Exception as e:
        logging.error(f"Error in write_rq_symbol_files_from_db: {e}\n{traceback.format_exc()}")
        return None
    finally:
        # Close the SQLite connection
        if conn:
            conn.close()

def encode_metadata(file_path):
    try:
        logging.info("Encoding metadata...")
        request = pb2.EncodeMetaDataRequest(
            path=file_path,
            files_number=0,
            block_hash="example_block_hash",
            pastel_id="example_pastel_id"
        )
        response = stub.EncodeMetaData(request)
        logging.info(f"Received encoder parameters: {response.encoder_parameters}, symbols_count: {response.symbols_count}, path: {response.path}")
        return response
    except Exception as e:
        logging.error(f"Error in encode_metadata: {e}\n{traceback.format_exc()}")
        
def encode(file_path):
    try:
        logging.info("Encoding...")
        request = pb2.EncodeRequest(path=file_path)
        response = stub.Encode(request)
        logging.info(f"Received encoder parameters: {response.encoder_parameters}, symbols_count: {response.symbols_count}, path: {response.path}")
        return response
    except Exception as e:
        logging.error(f"Error in encode: {e}\n{traceback.format_exc()}")

def decode(encoder_parameters, path):
    try:
        logging.info("Decoding...")
        request = pb2.DecodeRequest(
            encoder_parameters=encoder_parameters,
            path=path
        )
        response = stub.Decode(request)
        logging.info(f"Decoded file path: {response.path}")
        return response.path
    except Exception as e:
        logging.error(f"Error in decode: {e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    DB_PATH = "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite" # Path to the SQLite database file
    OUTPUT_DIR = "/home/ubuntu/.pastel/rqfiles" # Directory where you want to write the RQ symbol files
    INPUT_FILE_PATH = "/home/ubuntu/rqservice/test_files/input_test_file_small.jpg"
    # INPUT_FILE_PATH = "/home/ubuntu/rqservice/test_files/cp_detector.7z"
    channel = grpc.insecure_channel('localhost:50051')  # Change to your server's address and port
    stub = pb2_grpc.RaptorQStub(channel)
    
    use_test_decode_only = 0
    if not use_test_decode_only:
        logging.info(f"Testing with original file: {INPUT_FILE_PATH}")
        original_hash = compute_sha3_256(INPUT_FILE_PATH)
        metadata_response = encode_metadata(INPUT_FILE_PATH)
        logging.info(f'Encoder parameters: {metadata_response.encoder_parameters}')
        with open('encoder_parameters', 'wb') as f:
            f.write(metadata_response.encoder_parameters)
        if metadata_response:
            logging.info("Now attempting to encode...")            
            encode_response = encode(INPUT_FILE_PATH)
            logging.info(f'Receieved encode response: {encode_response}')
            logging.info("Now sleeping for 1 second to allow the server to finish encoding...")
            time.sleep(1)
            logging.info("Now attempting to write RQ symbol files from the database...")
            symbols_path = write_rq_symbol_files_from_db(DB_PATH, OUTPUT_DIR, original_hash)  # Call the function to write the RQ symbol files to the directory
            logging.info("Now attempting to decode...")
            reconstructed_file_path = decode(metadata_response.encoder_parameters, symbols_path)
            reconstructed_hash = compute_sha3_256(reconstructed_file_path)
            assert original_hash == reconstructed_hash, "Reconstructed file does not match the original file."
            logging.info("Test successful. Original and reconstructed files are identical.")
        else:
            logging.error("Encoding failed.")
    else:
        logging.info("Testing decode only...")
        INPUT_FILE_PATH = "/home/ubuntu/rqservice/test_files/input_test_file_small.jpg"
        task_id = 'fa1cc95f'
        rq_symbol_files_path = f"/home/ubuntu/.pastel/rqfiles/{task_id}/symbols"
        os.remove(DB_PATH)
        logging.info(f"Removed database file {DB_PATH}")
        with open('encoder_parameters', 'rb') as f:
            encoder_parameters = f.read()
        reconstructed_file_path = decode(encoder_parameters, rq_symbol_files_path)
        reconstructed_hash = compute_sha3_256(reconstructed_file_path)
        assert original_hash == reconstructed_hash, "Reconstructed file does not match the original file."
        logging.info("Test successful. Original and reconstructed files are identical.")
