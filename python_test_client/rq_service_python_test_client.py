import grpc
import service_pb2 as pb2
import service_pb2_grpc as pb2_grpc
import hashlib
import logging
import traceback
import time

logging.basicConfig(level=logging.INFO)

def compute_sha3_256(file_path):
    hash_value = hashlib.sha3_256(open(file_path, 'rb').read()).hexdigest()
    logging.info(f"Computed SHA3-256 hash of file {file_path}: {hash_value}")
    return hash_value

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
        logging.info(f"Encoded symbols with count: {response.symbols_count}, path: {response.path}")
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
    INPUT_FILE_PATH = "/home/ubuntu/rqservice/test_files/input_test_file.jpg"

    channel = grpc.insecure_channel('localhost:50051')  # Change to your server's address and port
    stub = pb2_grpc.RaptorQStub(channel)

    logging.info(f"Testing with original file: {INPUT_FILE_PATH}")

    original_hash = compute_sha3_256(INPUT_FILE_PATH)

    metadata_response = encode_metadata(INPUT_FILE_PATH)
    if metadata_response:
        logging.info(f"Encoded file path: {metadata_response.path}")
        logging.info("Now sleeping for 1 second to allow the server to finish encoding...")
        time.sleep(1)
        logging.info("File encoded successfully. Now attempting to decode...")
        reconstructed_file_path = decode(metadata_response.encoder_parameters, INPUT_FILE_PATH) # Use INPUT_FILE_PATH instead of metadata_response.path
        reconstructed_hash = compute_sha3_256(reconstructed_file_path)
        assert original_hash == reconstructed_hash, "Reconstructed file does not match the original file."
        logging.info("Test successful. Original and reconstructed files are identical.")
    else:
        logging.error("Encoding failed.")
