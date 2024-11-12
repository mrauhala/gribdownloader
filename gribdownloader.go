package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config represents the structure of the configuration file
type Config struct {
	IdxURL     string              `json:"idx_url"`
	Parameters map[string][]string `json:"parameters"`
}

// GFSParameter represents a single parameter in the idx file
type GFSParameter struct {
	Number    int
	Offset    int64
	Date      string
	Parameter string
	Level     string
	Type      string
}

// RangeDownload represents a byte range to download
type RangeDownload struct {
	Start int64
	End   int64
}

// downloadFile downloads a file from URL to a local path
func downloadFile(url, localPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	out, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("error saving file: %v", err)
	}

	return nil
}

// parseIDXFile reads and parses a GFS idx file
func parseIDXFile(idxPath string) ([]GFSParameter, error) {
	file, err := os.Open(idxPath)
	if err != nil {
		return nil, fmt.Errorf("error opening idx file: %v", err)
	}
	defer file.Close()

	var parameters []GFSParameter
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")

		if len(parts) < 6 {
			continue
		}

		number, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		param := GFSParameter{
			Number:    number,
			Offset:    offset,
			Date:      strings.TrimPrefix(parts[2], "d="),
			Parameter: parts[3],
			Level:     parts[4],
			Type:      parts[5],
		}

		parameters = append(parameters, param)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading idx file: %v", err)
	}

	return parameters, nil
}

// generateRanges creates download ranges for specified parameters
func generateRanges(parameters []GFSParameter, requestedParams map[string][]string) ([]RangeDownload, error) {
	var ranges []RangeDownload

	for i, param := range parameters {
		// Check if this parameter is requested
		levels, paramRequested := requestedParams[param.Parameter]
		if !paramRequested {
			continue
		}

		// Check if the level is requested for this parameter
		levelRequested := false
		if len(levels) == 0 { // Empty levels means all levels
			levelRequested = true
		} else {
			for _, level := range levels {
				if level == param.Level {
					levelRequested = true
					break
				}
			}
		}

		if !levelRequested {
			continue
		}

		// Calculate the end offset
		var endOffset int64
		if i < len(parameters)-1 {
			endOffset = parameters[i+1].Offset - 1
		} else {
			// For the last parameter, add a buffer (e.g., 1MB) to ensure we get all data
			endOffset = param.Offset + 1024*1024
		}

		ranges = append(ranges, RangeDownload{
			Start: param.Offset,
			End:   endOffset,
		})
	}

	// Merge overlapping or adjacent ranges
	if len(ranges) > 1 {
		merged := []RangeDownload{ranges[0]}
		for i := 1; i < len(ranges); i++ {
			last := &merged[len(merged)-1]
			current := ranges[i]

			// If ranges overlap or are adjacent, merge them
			if current.Start <= last.End+1 {
				if current.End > last.End {
					last.End = current.End
				}
			} else {
				merged = append(merged, current)
			}
		}
		ranges = merged
	}

	return ranges, nil
}

// downloadRange downloads a specific byte range from a URL and writes to the specified position in the output file
func downloadRange(url string, rangeSpec RangeDownload, outputFile string, mutex *sync.Mutex) error {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	// Set range header
	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeSpec.Start, rangeSpec.End)
	req.Header.Set("Range", rangeHeader)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Lock for file operations
	mutex.Lock()
	defer mutex.Unlock()

	// Open file in read-write mode
	out, err := os.OpenFile(outputFile, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening output file: %v", err)
	}
	defer out.Close()

	// Seek to the correct position
	_, err = out.Seek(rangeSpec.Start, 0)
	if err != nil {
		return fmt.Errorf("error seeking in file: %v", err)
	}

	// Copy data to file at the correct position
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("error copying data: %v", err)
	}

	return nil
}

// downloadRanges downloads multiple ranges concurrently into a single file
func downloadRanges(url string, ranges []RangeDownload, outputFile string) error {
	// Calculate total size needed
	var maxEnd int64
	for _, r := range ranges {
		if r.End > maxEnd {
			maxEnd = r.End
		}
	}

	// Create and pre-allocate the output file
	file, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}

	// Pre-allocate the file with the required size
	err = file.Truncate(maxEnd + 1)
	if err != nil {
		file.Close()
		return fmt.Errorf("error pre-allocating file: %v", err)
	}
	file.Close()

	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make(chan error, len(ranges))

	// Start concurrent downloads
	for _, r := range ranges {
		wg.Add(1)
		go func(r RangeDownload) {
			defer wg.Done()
			if err := downloadRange(url, r, outputFile, &mutex); err != nil {
				errors <- fmt.Errorf("error downloading range %d-%d: %v", r.Start, r.End, err)
			}
		}(r)
	}

	wg.Wait()
	close(errors)

	// Collect any errors
	var errorsList []error
	for err := range errors {
		errorsList = append(errorsList, err)
	}

	if len(errorsList) > 0 {
		return fmt.Errorf("encountered %d errors during download: %v", len(errorsList), errorsList)
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: gfs_downloader config.json")
		return
	}

	// Read configuration file
	configFile, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return
	}

	var config Config
	if err := json.Unmarshal(configFile, &config); err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		return
	}

	// Extract filename from URL and create local paths
	idxFileName := filepath.Base(config.IdxURL)
	gribFileName := strings.TrimSuffix(idxFileName, ".idx")
	gribFileURL := strings.TrimSuffix(config.IdxURL, ".idx")

	fmt.Printf("Downloading idx file: %s\n", idxFileName)
	if err := downloadFile(config.IdxURL, idxFileName); err != nil {
		fmt.Printf("Error downloading idx file: %v\n", err)
		return
	}

	// Parse the idx file
	parameters, err := parseIDXFile(idxFileName)
	if err != nil {
		fmt.Printf("Error parsing idx file: %v\n", err)
		return
	}

	// Generate download ranges
	ranges, err := generateRanges(parameters, config.Parameters)
	if err != nil {
		fmt.Printf("Error generating ranges: %v\n", err)
		return
	}

	// Print the ranges
	fmt.Println("Download ranges:")
	var totalSize int64
	for i, r := range ranges {
		size := r.End - r.Start + 1
		totalSize += size
		fmt.Printf("Range %d: %d-%d (%.2f MB)\n", i+1, r.Start, r.End, float64(size)/(1024*1024))
	}
	fmt.Printf("Total download size: %.2f MB\n", float64(totalSize)/(1024*1024))

	// Download the selected ranges
	fmt.Printf("Downloading GRIB data to: %s\n", gribFileName)
	err = downloadRanges(gribFileURL, ranges, gribFileName)
	if err != nil {
		fmt.Printf("Error downloading: %v\n", err)
		return
	}

	fmt.Println("Download completed successfully")
}
