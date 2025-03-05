package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math"
	"runtime/pprof"
	_ "net/http/pprof" // Importing pprof to register debug handlers
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"requirements/constants"
)

// Mutex for writing to the same map
var mu sync.Mutex

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
}

// STRUCT SIZE = 32bits * 4 = 16bytes
type DataStatistics struct{
	sum float32
	num_records int32
	max float32
	min float32
}

func(ds *DataStatistics) setMax(value float32) {
	ds.max = value
}

func(ds *DataStatistics) setMin(value float32) {
	ds.min = value
}

// STRUCT SIZE = 3*16bytes = 48bytes + strings
type DataFields struct{
	vn DataStatistics
	vp DataStatistics
	va DataStatistics
	nome_cedente string
	doc_cedente string
	nome_sacado string
	doc_sacado string
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

func PrintDataRow(key string, df *DataFields) string {
	var sb strings.Builder
	sb.Grow(256)

	fmt.Fprintf(&sb, "%s;%.2f;%.2f;%.2f;%.2f;", key, df.vn.sum, df.vn.sum/float32(df.vn.num_records), df.vn.max, df.vn.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f;", df.vp.sum, df.vp.sum/float32(df.vp.num_records), df.vp.max, df.vp.min)
	fmt.Fprintf(&sb, "%.2f;%.2f;%.2f;%.2f;", df.va.sum, df.va.sum/float32(df.va.num_records), df.va.max, df.va.min)
	fmt.Fprintf(&sb, "%s;%s;%s;%s\n", df.nome_cedente, df.doc_cedente, df.nome_sacado, df.doc_sacado)

	return sb.String()
}

func check(e error){
	if e != nil {
		panic(e)
	}
}

func FetchDataCols(text string, delimiter rune) (line_data []string) {

	// Split text into parts
	parts := strings.SplitN(text, string(delimiter), -1)
	
	// Validate that we have enough parts
	if len(parts) < int(constants.NU_DOCUMENTO_COL)+1 {
		return []string{}
	}

	// Extracting Mainly Filter Fields
	nome_cedente := parts[constants.NOME_CEDENTE_COL]
	doc_cedente := parts[constants.DOC_CEDENTE_COL]
	nome_sacado := parts[constants.NOME_SACADO_COL]
	doc_sacado := parts[constants.DOC_SACADO_COL]

	// Extract relevant fields
	vn_data := strings.ReplaceAll(parts[constants.VALOR_NOMINAL_COL], ",", "")
	vp_data := strings.ReplaceAll(parts[constants.VALOR_PRESENTE_COL], ",", "")
	va_data := strings.ReplaceAll(parts[constants.VALOR_AQUISICAO_COL], ",", "")
	nu_doc_data := parts[constants.NU_DOCUMENTO_COL]

	line_data = []string{vn_data, vp_data, va_data, nu_doc_data, nome_cedente, doc_cedente, nome_sacado, doc_sacado}

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
	// Setting a buffer for 64Kb
	const maxCapacity = 64 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	lineNum := 0
	line := ""
	for scanner.Scan(){
		// Skipping column names
		if lineNum == 0{
			lineNum++
			continue
		}
		// Only used to limit number of rows computed
		line = scanner.Text()
		delimiter := ';'

		if debug{
			fmt.Printf("FILEPATH: %s\n\n", filepath)
		}
		line_data := FetchDataCols(line, delimiter)
		if len(line_data)<1{
			fmt.Printf("FILEPATH WITH LINE ERROR:\n %s\n\n", filepath)
			continue
		}

		if debug{
			fmt.Printf("\n\nData: %v\n\n", line_data)
		}

		// Extracting Filters
		nu_documento := line_data[3]
		nome_cedente := line_data[4]
		doc_cedente := line_data[5]
		nome_sacado := line_data[6]
		doc_sacado := line_data[7]

		// Filter map so we can loop through filters
		if filterDocNum!="" && filterDocNum!=nu_documento{continue}
		if filterNomeCedente!="" && strings.ToUpper(filterNomeCedente)!=nome_cedente{continue}
		if filterDocCedente!="" && filterDocCedente!=doc_cedente{continue}
		if filterNomeSacado!="" && strings.ToUpper(filterNomeSacado)!=nome_sacado{continue}	
		if filterDocSacado!="" && filterDocSacado!=doc_sacado{continue}

		// Lock before modifying the shared structure
		mu.Lock()
		df, exists := map_statistics[nu_documento]
		if !exists{
			df = &DataFields{}
			df.SetInitialValues()
			map_statistics[nu_documento] = df
			map_statistics[nu_documento].nome_cedente = nome_cedente
			map_statistics[nu_documento].doc_cedente = doc_cedente
			map_statistics[nu_documento].nome_sacado = nome_sacado
			map_statistics[nu_documento].doc_sacado = doc_sacado
		}

		vn, err := strconv.ParseFloat(line_data[0], 32)
        check(err)
        vp, err := strconv.ParseFloat(line_data[1], 32)
        check(err)
        va, err := strconv.ParseFloat(line_data[2], 32)
        check(err)

		df.vn.ComputeStatistics(float32(vn))
		df.vp.ComputeStatistics(float32(vp))
		df.va.ComputeStatistics(float32(va))
		mu.Unlock()

		lineNum += 1
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

	// Creating my map data structure
	// Key: ~32 bytes
	// Value pointer: 8 bytes
	// DataFields: 48 bytes
	// Subtotal: 32 + 8 + 48 = 88 bytes per entry
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

	// Write Memory Profile at the END of execution 🔹 🔹
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