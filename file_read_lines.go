package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	_ "net/http/pprof" // Importing pprof to register debug handlers
	"os"
	"path/filepath"
	"requirements/constants"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
	"bytes"
)


// Global debug flag
var debug bool
var filterDocNum string
var filterNomeCedente string
var filterDocCedente string
var filterNomeSacado string
var filterDocSacado string

func init() {
	// Register a flag called "debug" that can be used on the command line.
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	// Filter by Document Number | Example: 113982936
	flag.StringVar(&filterDocNum, "docnum", "", "Filter by a specific document number")
	// Filter by Nome Cedente | Example: PICPAY BANK   BANCO MULTIPLO S A
	flag.StringVar(&filterNomeCedente, "nomecedente", "", "Filter by a specific Cedente name")
	// Filter by Doc Cedente | Example: 09.516.419/0001-75
	flag.StringVar(&filterDocCedente, "doccedente", "", "Filter by a specific Cedente document")
	// Filter by Nome Sacado | Example: JOSE CLEVERTON FRANCELINO NASCIMENTO
	flag.StringVar(&filterNomeSacado, "nomesacado", "", "Filter by a specific Sacado name")
	// Filter by Doc Sacado | Example: 066.407.504-57
	flag.StringVar(&filterDocSacado, "docsacado", "", "Filter by a specific document Sacado document")

	// Parse command-line flags early so that 'debug' is available to the rest of the program.
	flag.Parse()
	filterNomeCedente = strings.ToUpper(filterNomeCedente)
	filterNomeSacado = strings.ToUpper(filterNomeSacado)
}

// STRUCT SIZE = 32bits * 4 = 16bytes
type DataStatistics struct {
	sum         float32
	num_records int32
	max         float32
	min         float32
}

func (ds *DataStatistics) setMax(value float32) {
	ds.max = value
}

func (ds *DataStatistics) setMin(value float32) {
	ds.min = value
}

// STRUCT SIZE = 3*16bytes = 48bytes + strings
type DataFields struct {
	vn           DataStatistics
	vp           DataStatistics
	va           DataStatistics
	nome_cedente string
	doc_cedente  string
	nome_sacado  string
	doc_sacado   string
}

func (df *DataFields) SetInitialValues() {
	df.vn.setMax(float32(math.Inf(-1)))
	df.vn.setMin(float32(math.Inf(1)))

	df.vp.setMax(float32(math.Inf(-1)))
	df.vp.setMin(float32(math.Inf(1)))

	df.va.setMax(float32(math.Inf(-1)))
	df.va.setMin(float32(math.Inf(1)))
}

func (ds *DataStatistics) ComputeStatistics(value float32) {
	// Adding value to sum
	ds.sum += value
	ds.num_records += 1

	if value > ds.max {
		ds.max = value
	}

	if value < ds.min {
		ds.min = value
	}
}

func PrintDataRow(key uint32, df *DataFields) string {
	var sb strings.Builder
	sb.Grow(1024)

	fmt.Fprintf(&sb, "%d;%.2f;%.2f;%.2f;%.2f;", key, df.vn.sum, df.vn.sum/float32(df.vn.num_records), df.vn.max, df.vn.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f;", df.vp.sum, df.vp.sum/float32(df.vp.num_records), df.vp.max, df.vp.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f;", df.va.sum, df.va.sum/float32(df.va.num_records), df.va.max, df.va.min)
	fmt.Fprintf(&sb, "%s;%s;%s;%s\n", df.nome_cedente, df.doc_cedente, df.nome_sacado, df.doc_sacado)

	return sb.String()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func FetchDataCols(line_bytes []byte, delimiter_bytes []byte) (line_data [][]byte) {

	// Split text into parts
	parts := bytes.Split(line_bytes, delimiter_bytes)

	// Validate that we have enough parts
	if len(parts) < int(constants.NU_DOCUMENTO_COL)+1 {
		return [][]byte{}
	}

	// Extracting Mainly Filter Fields
	nome_cedente := parts[constants.NOME_CEDENTE_COL]
	doc_cedente := parts[constants.DOC_CEDENTE_COL]
	nome_sacado := parts[constants.NOME_SACADO_COL]
	doc_sacado := parts[constants.DOC_SACADO_COL]

	// Comma bytes
	comma_bytes := []byte(",")
	empty_space_bytes := []byte("")

	// Extract relevant fields
	vn_data := bytes.Replace(parts[constants.VALOR_NOMINAL_COL], comma_bytes, empty_space_bytes, -1)
	vp_data := bytes.Replace(parts[constants.VALOR_PRESENTE_COL], comma_bytes, empty_space_bytes, -1)
	va_data := bytes.Replace(parts[constants.VALOR_AQUISICAO_COL], comma_bytes, empty_space_bytes, -1)
	nu_doc_data := parts[constants.NU_DOCUMENTO_COL]

	line_data = [][]byte{vn_data, vp_data, va_data, nu_doc_data, nome_cedente, doc_cedente, nome_sacado, doc_sacado}

	return line_data
}

func GetCWD() string {
	ex, err := os.Executable()
	check(err)
	exPath := filepath.Dir(ex)
	return exPath
}

func GetFilePathList(folder_path string) []string {
	entries, err := os.ReadDir(folder_path)
	check(err)
	var filenames []string
	for _, entry := range entries{
		if strings.Contains(entry.Name(), "Zone.Identifier") || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		filenames = append(filenames, entry.Name())
	}
	return filenames
}

func ParseCSVFile(filepath string, results_channel chan<-map[uint32]*DataFields, wg *sync.WaitGroup, bufReaderPool *sync.Pool) {
	if debug{
		fmt.Printf("In ParseCSVFile: filepath: %s\n", filepath)
	}
	// Signal that this goroutine is done
	defer wg.Done()

	// Local map to avoid global mutex locks
	local_map := make(map[uint32]*DataFields)
	defer func() {
        // Send local map to aggregator channel
        results_channel <- local_map
    }()

	// Open File Ptr
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()
	// Counting Lines in the file
	bufReader := GetBufReader(filePtr, bufReaderPool)
	defer PutBufReader(bufReader, bufReaderPool)

	lineNum := 0
	var line []byte
	delimiter := []byte(";")
	eof_flag := false
	for {
		// Read line by line
		line, err = bufReader.ReadBytes('\n')
		if err != nil {
			// If we reached the end of file, print the last line if not empty.
			if err == io.EOF {
				if len(line) > 0 {
					eof_flag = true
				}
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			break
		}
		// Skipping column names
		if lineNum == 0 {
			lineNum++
			continue
		}

		line_data := FetchDataCols(line, delimiter)
		if len(line_data) < 1 {
			fmt.Printf("FILEPATH WITH LINE ERROR:\n %s\n\n", filepath)
			continue
		}

		if debug{
			fmt.Printf("In ParseCSVFile: PART2 | filepath: %s\n linedata: %s\n", filepath, line_data)
		}

		// Extracting Filters
		nu_documento := string(line_data[3])
		nome_cedente := string(line_data[4])
		doc_cedente := string(line_data[5])
		nome_sacado := string(line_data[6])
		doc_sacado := string(line_data[7])

		// Filter map so we can loop through filters
		if filterDocNum != "" && filterDocNum != nu_documento {
			continue
		}
		if filterNomeCedente != "" && filterNomeCedente != nome_cedente {
			continue
		}
		if filterDocCedente != "" && filterDocCedente != doc_cedente {
			continue
		}
		if filterNomeSacado != "" && filterNomeSacado != nome_sacado {
			continue
		}
		if filterDocSacado != "" && filterDocSacado != doc_sacado {
			continue
		}

		// Lock before modifying the shared structure
		nu_documento_uint, err := strconv.ParseUint(nu_documento, 10, 32)
		check(err)
		nu_documento_uint32 := uint32(nu_documento_uint)
		df, exists := local_map[nu_documento_uint32]
		if !exists {
			df = &DataFields{}
			df.SetInitialValues()
			local_map[nu_documento_uint32] = df
			local_map[nu_documento_uint32].nome_cedente = nome_cedente
			local_map[nu_documento_uint32].doc_cedente = doc_cedente
			local_map[nu_documento_uint32].nome_sacado = nome_sacado
			local_map[nu_documento_uint32].doc_sacado = doc_sacado
		}

		vn, err := strconv.ParseFloat(string(line_data[0]), 32)
		check(err)
		vp, err := strconv.ParseFloat(string(line_data[1]), 32)
		check(err)
		va, err := strconv.ParseFloat(string(line_data[2]), 32)
		check(err)

		df.vn.ComputeStatistics(float32(vn))
		df.vp.ComputeStatistics(float32(vp))
		df.va.ComputeStatistics(float32(va))

		lineNum += 1

		if eof_flag {
			break
		}
	}
}

func GenerateOutputFile(map_statistics map[uint32]*DataFields) {
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
	col_names := "NU_DOCUMENTO;VN_SOMA;VN_MEDIA;VN_MAX;VN_MIN;VP_SOMA;VP_MEDIA;VP_MAX;VP_MIN;VA_SOMA;VA_MEDIA;VA_MAX;VA_MIN;NOME_CEDENTE;DOC_CEDENTE;NOME_SACADO;DOC_SACADO\n"
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

// GetBufReader retrieves a *bufio.Reader from the pool and resets it with the provided reader.
func GetBufReader(r io.Reader, bufReaderPool *sync.Pool) *bufio.Reader {
	br := bufReaderPool.Get().(*bufio.Reader)
	br.Reset(r) // Reset attaches the new reader to the buffered reader.
	return br
}

// PutBufReader returns a *bufio.Reader back to the pool after use.
func PutBufReader(br *bufio.Reader, bufReaderPool *sync.Pool) {
	// Optionally, you might want to clear or discard any buffered data.
	bufReaderPool.Put(br)
}

func MergeDataStatistics(global_ds *DataStatistics, local_ds *DataStatistics){
	global_ds.sum += local_ds.sum
	global_ds.num_records += local_ds.num_records

	if local_ds.max > global_ds.max{
		global_ds.max = local_ds.max
	}

	if local_ds.min < global_ds.min{
		global_ds.min = local_ds.min
	}
}

func MergeDataFields(global_df *DataFields, local_df *DataFields){
	MergeDataStatistics(&global_df.vp, &local_df.vp)
	MergeDataStatistics(&global_df.vn, &local_df.vn)
	MergeDataStatistics(&global_df.va, &local_df.va)
}

func main() {
	// Start time tracking
	start := time.Now()
	// Create file to store CPU profile
	cpuProfileFile, err := os.Create("cpu_profile.pprof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer cpuProfileFile.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	// Ensure profiling is stopped when main finishes
	defer pprof.StopCPUProfile()

	cwd := GetCWD()
	folderPath := cwd + "/files"

	// Check if folder exists before proceeding
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		log.Fatalf("Error: Folder %s does not exist.\n", folderPath)
	}

	filenames := GetFilePathList(folderPath)

	var wg sync.WaitGroup

	// Initializing a buffer syncPool
	bufferSize := 200 * 1024 // 200Kb
	bufferPool := sync.Pool{
		New: func() interface{} {
			return bufio.NewReaderSize(nil, bufferSize)
		},
	}

	// Starting a channel
	results_channel := make(chan map[uint32]*DataFields, len(filenames))

	for _, filename := range filenames {
		wg.Add(1)
		go ParseCSVFile(folderPath+"/"+filename, results_channel, &wg, &bufferPool)
	}

	wg.Wait()
	
	// Creating my map data structure
	// Key: ~32 bytes
	// Value pointer: 8 bytes
	// DataFields: 48 bytes
	// Subtotal: 32 + 8 + 48 = 88 bytes per entry
	// Merge all maps in channel into the main global_map
	global_map := make(map[uint32]*DataFields)
	for i := 0; i < len(filenames); i++ {
		local_map := <-results_channel
		// Merge local_map into global_map (with a single goroutine doing this, so no locking required)
		for key, local_df := range local_map {
			if global_df, exists := global_map[key]; exists {
				// merge statistics (e.g., add sums, recompute min/max, update record counts)
				MergeDataFields(global_df, local_df)
			} else {
				global_map[key] = local_df
			}
		}
	}

	// Outputing CSV file
	GenerateOutputFile(global_map)

	// Write Memory Profile at the END of execution
	f, err := os.Create("mem_profile.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// OPTIONAL: Run GC before profiling (useful in some cases)
	// runtime.GC()

	// Write the memory profile after processing
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}

	// Print total execution time
	elapsed := time.Since(start)
	fmt.Printf("Execution Time: %s\n", elapsed)
}