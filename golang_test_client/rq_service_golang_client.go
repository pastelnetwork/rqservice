package main

import (
	"crypto/sha3"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcutil/base58"
	_ "github.com/mattn/go-sqlite3"
	pb "path/to/your/protobuf/package" // Update to your gRPC package
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func computeSha3256(filePath string) string {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	hasher := sha3.New256()
	hasher.Write(data)
	hashValue := base58.Encode(hasher.Sum(nil))
	log.Printf("Computed SHA3-256 hash of file %s: %s", filePath, hashValue)
	return hashValue
}

func writeRqSymbolFilesFromDb(dbPath, outputDir, originalHash string) string {
	// Connect to SQLite database
	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Generate a fake taskID dynamically
	rand.Seed(time.Now().UnixNano())
	fakeTaskID := fmt.Sprintf("%x", rand.Int31())
	symbolsPath := filepath.Join(outputDir, fakeTaskID, "symbols")
	os.MkdirAll(symbolsPath, os.ModePerm)
	log.Printf("Created directory %s", symbolsPath)

	// Query to retrieve the RQ symbol files
	query := fmt.Sprintf("SELECT rq_symbol_file_sha3_256_hash, rq_symbol_file_data FROM rq_symbols WHERE original_file_sha3_256_hash = '%s'", originalHash)
	rows, err := conn.Query(query)
	if err != nil {
		log.Fatalf("Failed to query the database: %v", err)
	}
	defer rows.Close()

	var deleteHashes []string
	var results int
	for rows.Next() {
		var symbolHash string
		var symbolData []byte
		if err := rows.Scan(&symbolHash, &symbolData); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		results++
		deleteHashes = append(deleteHashes, symbolHash)
		symbolFilePath := filepath.Join(symbolsPath, symbolHash)
		ioutil.WriteFile(symbolFilePath, symbolData, os.ModePerm)
	}

	// Delete corresponding entries from the database
	tx, err := conn.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	for i, hash := range deleteHashes {
		if i%10000 == 0 {
			log.Printf("Deleting RQ symbol file %d of %d", i, len(deleteHashes))
		}
		tx.Exec("DELETE FROM rq_symbols WHERE rq_symbol_file_sha3_256_hash = ?", hash)
	}
	tx.Commit()

	log.Printf("Successfully wrote RQ symbol files for fake taskID %s to %s", fakeTaskID, symbolsPath)
	return symbolsPath
}

func encodeMetadata(client pb.RaptorQClient, filePath string) *pb.EncodeMetaDataResponse {
	request := &pb.EncodeMetaDataRequest{
		Path:        filePath,
		FilesNumber: 0,
		BlockHash:   "example_block_hash",
		PastelID:    "example_pastel_id",
	}
	response, err := client.EncodeMetaData(context.Background(), request)
	if err != nil {
		log.Fatalf("Error in encodeMetadata: %v", err)
	}
	log.Printf("Received encoder parameters: %v, symbols_count: %v, path: %v", response.EncoderParameters, response.SymbolsCount, response.Path)
	return response
}

func encode(client pb.RaptorQClient, filePath string) *pb.EncodeResponse {
	request := &pb.EncodeRequest{Path: filePath}
	response, err := client.Encode(context.Background(), request)
	if err != nil {
		log.Fatalf("Error in encode: %v", err)
	}
	log.Printf("Received encoder parameters: %v, symbols_count: %v, path: %v", response.EncoderParameters, response.SymbolsCount, response.Path)
	return response
}

func decode(client pb.RaptorQClient, encoderParameters []byte, path string) string {
	request := &pb.DecodeRequest{
		EncoderParameters: encoderParameters,
		Path:              path,
	}
	response, err := client.Decode(context.Background(), request)
	if err != nil {
		log.Fatalf("Error in decode: %v", err)
	}
	log.Printf("Decoded file path: %v", response.Path)
	return response.Path
}


func main() {
	useTestDecodeOnly := false

	DB_PATH := "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite"
	OUTPUT_DIR := "/home/ubuntu/.pastel/rqfiles"
	INPUT_FILE_PATH := "/home/ubuntu/rqservice/test_files/cp_detector.7z"

	if useTestDecodeOnly {
		os.Remove(DB_PATH)
		log.Printf("Removed database file %s", DB_PATH)
	}

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewRaptorQClient(conn)

	if !useTestDecodeOnly {
		log.Printf("Testing with original file: %s", INPUT_FILE_PATH)
		originalHash := computeSha3256(INPUT_FILE_PATH)
		metadataResponse := encodeMetadata(client, INPUT_FILE_PATH)
		encoderParameters := metadataResponse.GetEncoderParameters()

		ioutil.WriteFile("encoder_parameters", encoderParameters, os.ModePerm)

		if metadataResponse != nil {
			log.Println("Now attempting to encode...")
			encodeResponse := encode(client, INPUT_FILE_PATH)
			log.Println("Now sleeping for 1 second to allow the server to finish encoding...")
			time.Sleep(1 * time.Second)
			log.Println("Now attempting to write RQ symbol files from the database...")
			symbolsPath := writeRqSymbolFilesFromDb(DB_PATH, OUTPUT_DIR, originalHash)
			log.Println("Now attempting to decode...")
			reconstructedFilePath := decode(client, encoderParameters, symbolsPath)
			reconstructedHash := computeSha3256(reconstructedFilePath)

			if originalHash != reconstructedHash {
				log.Fatalf("Reconstructed file does not match the original file.")
			}
			log.Println("Test successful. Original and reconstructed files are identical.")
		} else {
			log.Fatal("Encoding failed.")
		}
	} else {
		log.Println("Testing decode only...")
		INPUT_FILE_PATH = "/home/ubuntu/rqservice/test_files/input_test_file_small.jpg"

		dirs, err := ioutil.ReadDir(OUTPUT_DIR)
		if err != nil {
			log.Fatalf("Failed to read directory: %v", err)
		}

		var latestDir string
		var latestTime time.Time
		for _, dir := range dirs {
			if dir.IsDir() {
				dirTime := dir.ModTime()
				if dirTime.After(latestTime) {
					latestTime = dirTime
					latestDir = dir.Name()
				}
			}
		}
		log.Printf("Using task_id: %s", latestDir)

		rqSymbolFilesPath := filepath.Join(OUTPUT_DIR, latestDir, "symbols")
		encoderParameters, err := ioutil.ReadFile("encoder_parameters")
		if err != nil {
			log.Fatalf("Failed to read encoder parameters: %v", err)
		}

		reconstructedFilePath := decode(client, encoderParameters, rqSymbolFilesPath)
		reconstructedHash := computeSha3256(reconstructedFilePath)
		originalHash := computeSha3256(INPUT_FILE_PATH)

		if originalHash != reconstructedHash {
			log.Fatalf("Reconstructed file does not match the original file.")
		}
		log.Println("Test successful. Original and reconstructed files are identical.")
	}
}