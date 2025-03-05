package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof" // Importing pprof to register debug handlers
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Mutex for writing to the same map
var mu sync.Mutex

// Global debug flag
var debug bool

func init() {
	// Register a flag called "debug" that can be used on the command line.
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	// Parse command-line flags early so that 'debug' is available to the rest of the program.
	flag.Parse()
}

// STRUCT SIZE = 32bits + 32bits + 16bits + 16bits = 12bytes
type DataStatistics struct{
	sum float32
	num_records int32
	max float32
	min float32
}

func(ds *DataStatistics) setSum(value float32) {
	ds.sum = value
}

func(ds *DataStatistics) setNumRecords(value int32) {
	ds.num_records = value
}

func(ds *DataStatistics) setMax(value float32) {
	ds.max = value
}

func(ds *DataStatistics) setMin(value float32) {
	ds.min = value
}

// STRUCT SIZE = 3*12bytes = 36bytes
type DataFields struct{
	vn DataStatistics
	vp DataStatistics
	va DataStatistics
}

func(df *DataFields) SetInitialValues(){
	df.vn.setMax(float32(math.Inf(-1)))
	df.vn.setMin(float32(math.Inf(1)))

	df.vp.setMax(float32(math.Inf(-1)))
	df.vp.setMin(float32(math.Inf(1)))

	df.va.setMax(float32(math.Inf(-1)))
	df.va.setMin(float32(math.Inf(1)))
}

func(ds *DataStatistics) ComputeStatistics(value float32){
	// Adding value to sum
	ds.sum += value
	ds.num_records += 1
	
	if value > ds.max{
		ds.max = value
	}

	if value < ds.min{
		ds.min = value
	}
}

func(ds *DataStatistics) PrintStatistics(field_name string){
	fmt.Printf("%s Data:\n", field_name)
	fmt.Printf("Statistics: \nSum: %.3f | Mean: %.2f | Max: %.2f | Min: %.2f\n\n", ds.sum, ds.sum/float32(ds.num_records), ds.max, ds.min)
}

func PrintDataRow(key string, df *DataFields) string {
	var sb strings.Builder
	sb.Grow(256)

	fmt.Fprintf(&sb, "%s;%.2f;%.2f;%.2f;%.2f;", key, df.vn.sum, df.vn.sum/float32(df.vn.num_records), df.vn.max, df.vn.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f;", df.vp.sum, df.vp.sum/float32(df.vp.num_records), df.vp.max, df.vp.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f\n", df.va.sum, df.va.sum/float32(df.va.num_records), df.va.max, df.va.min)

	return sb.String()
}

func check(e error){
	if e != nil {
		panic(e)
	}
}

func FetchDataCols(text string, delimiter rune) (line_data []string) {
	const VALOR_NOMINAL_COL = 9
	const VALOR_PRESENTE_COL = 10
	const VALOR_AQUISICAO_COL = 11
	const NU_DOCUMENTO_COL = 16

	// Split text into parts
	parts := strings.SplitN(text, string(delimiter), -1)
	
	// Validate that we have enough parts
	if len(parts) < int(NU_DOCUMENTO_COL)+1 {
		return []string{}
	}

	// Extract relevant fields
	vn_data := strings.ReplaceAll(parts[VALOR_NOMINAL_COL], ",", "")
	vp_data := strings.ReplaceAll(parts[VALOR_PRESENTE_COL], ",", "")
	va_data := strings.ReplaceAll(parts[VALOR_AQUISICAO_COL], ",", "")
	nu_doc_data := parts[NU_DOCUMENTO_COL]

	line_data = []string{vn_data, vp_data, va_data, nu_doc_data}

	return line_data
}


func GetCWD()string{
	ex, err := os.Executable()
	check(err)
	exPath := filepath.Dir(ex)
	return exPath
}

func GetFilePathList(folder_path string)[]fs.DirEntry{
	entries, err := os.ReadDir(folder_path)
	check(err)
	return entries
}

func ParseCSVFile(filepath string, map_statistics map[string]*DataFields, wg *sync.WaitGroup){
	// Signal that this goroutine is done
	defer wg.Done()

	// Open File Ptr
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()
	// Counting Lines in the file
	scanner := bufio.NewScanner(filePtr)
	i := 0
	for scanner.Scan(){
		// Skipping column names
		if i == 0{
			i+=1
			continue
		}
		// Only used to limit number of rows computed
		// if i >= 5{
		// 	break;
		// }
		line := scanner.Text()
		delimiter := ';'

		if debug{
			fmt.Printf("FILEPATH: %s\n\n", filepath)
		}
		line_data := FetchDataCols(line, delimiter)
		if len(line_data)<4{
			fmt.Printf("FILEPATH THREW ERROR: %s\n\n", filepath)
			continue
		}

		if debug{
			fmt.Printf("\n\nData: %v\n\n", line_data)
		}

		nu_documento := line_data[3]

		// Lock before modifying the shared structure
		mu.Lock()
		df, exists := map_statistics[nu_documento]
		if !exists{
			df = &DataFields{}
			df.SetInitialValues()
			map_statistics[nu_documento] = df
		}
		mu.Unlock()

		vn, err := strconv.ParseFloat(line_data[0], 32)
        check(err)
        vp, err := strconv.ParseFloat(line_data[1], 32)
        check(err)
        va, err := strconv.ParseFloat(line_data[2], 32)
        check(err)

		// Lock again before modifying inner struct data
		mu.Lock()
		df.vn.ComputeStatistics(float32(vn))
		df.vp.ComputeStatistics(float32(vp))
		df.va.ComputeStatistics(float32(va))
		mu.Unlock()

		i += 1
	}
}

func GenerateOutputFile(map_statistics map[string]*DataFields){
	// Creating dir if not exists
	err := os.MkdirAll("output", 0755)
	check(err)
	// Build new file
	output_filename := "output/calculations.csv"
	// Open the file with the flags: append, create if not exists, and write only.
	// The permission 0644 means the owner can read/write, and others can read.
	filePtr, err := os.OpenFile(output_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	check(err)
	defer filePtr.Close()

	// Use buffered writer
	writer := bufio.NewWriter(filePtr)

	// Write to file
	col_names := "NU_DOCUMENTO;VN_SOMA;VN_MEDIA;VN_MAX;VN_MIN;VP_SOMA;VP_MEDIA;VP_MAX;VP_MIN;VA_SOMA;VA_MEDIA;VA_MAX;VA_MIN\n"
	writer.WriteString(col_names)
	writer.Flush()

	var data_str string
	for key, df := range map_statistics {
		data_str = PrintDataRow(key, df)
		_, err = filePtr.WriteString(data_str)
		check(err)
	}

	writer.Flush()
}

func main() {
	// Start pprof server in a separate goroutine
	go func() {
		log.Println("Starting pprof server on http://localhost:6060/debug/pprof/")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cwd := GetCWD()
	folderPath := cwd + "/files"

	// Check if folder exists before proceeding
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		log.Fatalf("Error: Folder %s does not exist.\n", folderPath)
	}

	filenames := GetFilePathList(folderPath)

	mapStatistics := make(map[string]*DataFields)
	var wg sync.WaitGroup

	for _, filename := range filenames {
		if strings.Contains(filename.Name(), "Zone.Identifier") || !strings.HasSuffix(filename.Name(), ".csv") {
			continue
		}
		wg.Add(1)
		go ParseCSVFile(folderPath+"/"+filename.Name(), mapStatistics, &wg)
	}

	wg.Wait()

	GenerateOutputFile(mapStatistics)

	// Keep the main goroutine running
	select {}
}