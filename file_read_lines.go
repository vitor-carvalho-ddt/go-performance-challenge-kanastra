package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	_ "net/http/pprof" // Importing pprof to register debug handlers
	"os"
	"path/filepath"
	"requirements/constants"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

var uint16bufferPool = sync.Pool{
	New: func() interface{} {
		// Create a slice with length (and capacity) 53. (54 columns, 53 ;)
		buf := make([]uint16, 53)
		return &buf
	},
}

// Create a pool for DataFields objects
var dataFieldsPool = sync.Pool{
    New: func() interface{} {
        df := &DataFields{}
        return df
    },
}

func preAllocateDataFieldsPool(poolsize int) {
    temps := make([]*DataFields, 0, poolsize)
    
    for i := 0; i < poolsize; i++ {
        temps = append(temps, dataFieldsPool.Get().(*DataFields))
    }
    
    for _, df := range temps {
        putDataField(df)
    }
}

func getDataField() *DataFields {
    return dataFieldsPool.Get().(*DataFields)
}

func putDataField(df *DataFields) {
    dataFieldsPool.Put(df)
}

// Mutex for writing to the same map
var mu sync.Mutex

// Global debug flag
var debug bool
var filterDocNum []byte
var filterNomeCedente []byte
var filterDocCedente []byte
var filterNomeSacado []byte
var filterDocSacado []byte

func init() {
	var filterDocNumString string
	var filterNomeCedenteString string
	var filterDocCedenteString string
	var filterNomeSacadoString string
	var filterDocSacadoString string

	// Register a flag called "debug" that can be used on the command line.
	flag.BoolVar(&debug, "debug", false, "Enable debug output")
	// Filter by Document Number | Example: 113982936
	flag.StringVar(&filterDocNumString, "docnum", "", "Filter by a specific document number")
	// Filter by Nome Cedente | Example: PICPAY BANK   BANCO MULTIPLO S A
	flag.StringVar(&filterNomeCedenteString, "nomecedente", "", "Filter by a specific Cedente name")
	// Filter by Doc Cedente | Example: 09.516.419/0001-75
	flag.StringVar(&filterDocCedenteString, "doccedente", "", "Filter by a specific Cedente document")
	// Filter by Nome Sacado | Example: JOSE CLEVERTON FRANCELINO NASCIMENTO
	flag.StringVar(&filterNomeSacadoString, "nomesacado", "", "Filter by a specific Sacado name")
	// Filter by Doc Sacado | Example: 066.407.504-57
	flag.StringVar(&filterDocSacadoString, "docsacado", "", "Filter by a specific document Sacado document")

	flag.Parse()

	filterDocNum = []byte(filterDocNumString)
	filterNomeCedente = []byte(strings.ToUpper(filterNomeCedenteString))
	filterDocCedente = []byte(filterDocCedenteString)
	filterNomeSacado = []byte(strings.ToUpper(filterNomeSacadoString))
	filterDocSacado = []byte(filterDocSacadoString)
}

func byteArrayToInt(byteSlice []byte) (int, error) {
	var result int
	for _, b := range byteSlice {
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid byte: %c", b)
		}
		result = result*10 + int(b-'0')
	}
	return result, nil
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
	vn DataStatistics
	vp DataStatistics
	va DataStatistics
	// nome_cedente []byte
	// doc_cedente  []byte
	// nome_sacado  []byte
	// doc_sacado   []byte
}

func (df *DataFields) SetInitialValues() {
	df.vn.sum = 0
	df.vn.num_records = 0
	df.vn.max = float32(math.Inf(-1))
	df.vn.min = float32(math.Inf(1))

	df.vp.sum = 0
	df.vp.num_records = 0
	df.vp.max = float32(math.Inf(-1))
	df.vp.min = float32(math.Inf(1))

	df.va.sum = 0
	df.va.num_records = 0
	df.va.max = float32(math.Inf(-1))
	df.va.min = float32(math.Inf(1))
}

func (ds *DataStatistics) ComputeStatistics(value float32) {
	ds.sum += value
	ds.num_records += 1

	if value > ds.max {
		ds.max = value
	}

	if value < ds.min {
		ds.min = value
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func FetchDataCols(line_bytes []byte, delimiter_bytes []byte) ([]byte, []byte, []byte, []byte, []byte, []byte, []byte, []byte) {
	// buffers
	var nu_doc_data []byte
	var vn_data []byte
	var vp_data []byte
	var va_data []byte
	var nome_cedente []byte
	var doc_cedente []byte
	var nome_sacado []byte
	var doc_sacado []byte

	num_cols := 54 - 1 // 54 columns minus 1 from EOL
	delimiter_positions := uint16bufferPool.Get().(*[]uint16)
	dp := *delimiter_positions
	dp = dp[:53]

	count := 0
	for index := range line_bytes {
		if line_bytes[index] == byte(';') {
			dp[count] = uint16(index)
			count++
			if count > num_cols {
				break
			}
		}
	}

	nu_doc_data = line_bytes[dp[constants.NU_DOCUMENTO_COL-1]+1 : dp[constants.NU_DOCUMENTO_COL]]
	vn_data = line_bytes[dp[constants.VALOR_NOMINAL_COL-1]+1 : dp[constants.VALOR_NOMINAL_COL]]
	vp_data = line_bytes[dp[constants.VALOR_PRESENTE_COL-1]+1 : dp[constants.VALOR_PRESENTE_COL]]
	va_data = line_bytes[dp[constants.VALOR_AQUISICAO_COL-1]+1 : dp[constants.VALOR_AQUISICAO_COL]]
	nome_cedente = line_bytes[dp[constants.NOME_CEDENTE_COL-1]+1 : dp[constants.NOME_CEDENTE_COL]]
	doc_cedente = line_bytes[dp[constants.DOC_CEDENTE_COL-1]+1 : dp[constants.DOC_CEDENTE_COL]]
	nome_sacado = line_bytes[dp[constants.NOME_SACADO_COL-1]+1 : dp[constants.NOME_SACADO_COL]]
	doc_sacado = line_bytes[dp[constants.DOC_SACADO_COL-1]+1 : dp[constants.DOC_SACADO_COL]]

	uint16bufferPool.Put(delimiter_positions)

	// testing my own function (removes ; inplace)
	RemoveComma(vn_data)
	RemoveComma(vp_data)
	RemoveComma(va_data)

	if len(nu_doc_data) == 0 {
		return []byte{}, []byte{}, []byte{}, []byte{}, []byte{}, []byte{}, []byte{}, []byte{}
	}

	return vn_data, vp_data, va_data, nu_doc_data, nome_cedente, doc_cedente, nome_sacado, doc_sacado
}

func GetCWD() string {
	ex, err := os.Executable()
	check(err)
	exPath := filepath.Dir(ex)
	return exPath
}

func GetFilePathList(folder_path string) []fs.DirEntry {
	entries, err := os.ReadDir(folder_path)
	check(err)
	return entries
}

func RemoveComma(data []byte) {
	data_len := len(data)
	if data_len == 0{
		return
	}

	write_index := 0

	for read_index := range data {
		if data[read_index] != byte(',') {
			data[write_index] = data[read_index]
			write_index++
		}
	}

	for i := write_index; i < data_len; i++{
		data[i] = '0'
	}
}

func ParseFloat32(b []byte) (float32, error) {
    if len(b) == 0 {
        return 0, fmt.Errorf("empty byte slice")
    }

    var result float64
    var isNegative bool
    var decimalPos int = -1
    var i int

    if b[0] == '-' {
        isNegative = true
        i = 1
    } else if b[0] == '+' {
        i = 1
    }

    // Process digits
    for ; i < len(b); i++ {
        if b[i] == '.' {
            if decimalPos >= 0 {
                return 0, fmt.Errorf("multiple decimal points")
            }
            decimalPos = i
            continue
        }

        if b[i] < '0' || b[i] > '9' {
            return 0, fmt.Errorf("invalid character: %c", b[i])
        }

        digit := float64(b[i] - '0')
        result = result*10 + digit
    }

    if decimalPos >= 0 {
        decimalPlaces := len(b) - decimalPos - 1
        result = result / math.Pow10(decimalPlaces)
    }

    if isNegative {
        result = -result
    }

    return float32(result), nil
}

func GenerateKeysBuffer(filepath string, keybuffer_map map[uint32]uint32){
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()

	bufferSize := 4 * 1024 // 4Kb
	bufReader := bufio.NewReaderSize(filePtr, bufferSize)

	lineNum := 0
	var line []byte
	delimiter := []byte(";")
	eof_flag := false
	for {
		// Read line by line
		line, err = bufReader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					eof_flag = true
				}
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			break
		}
		if lineNum == 0 {
			lineNum++
			continue
		}

		if debug {
			fmt.Printf("FILEPATH: %s\n\n", filepath)
		}

		_, _, _, nu_documento, nome_cedente, doc_cedente, nome_sacado, doc_sacado:= FetchDataCols(line, delimiter)

		if !bytes.Equal(filterDocNum, []byte("")) && !bytes.Equal(filterDocNum, nu_documento) {
			continue
		}

		if !bytes.Equal(filterNomeCedente, []byte("")) && !bytes.Equal(filterNomeCedente, nome_cedente) {
			continue
		}
		if !bytes.Equal(filterDocCedente, []byte("")) && !bytes.Equal(filterDocCedente, doc_cedente) {
			continue
		}
		if !bytes.Equal(filterNomeSacado, []byte("")) && !bytes.Contains(nome_sacado, filterNomeSacado) {
			continue
		}
		if !bytes.Equal(filterDocSacado, []byte("")) && !bytes.Equal(filterDocSacado, doc_sacado) {
			continue
		}
		if len(nu_documento) < 1 {
			fmt.Printf("FILEPATH WITH LINE ERROR:\n %s\n\n", filepath)
			continue
		}

		// Lock before modifying the shared structure
		nu_documento_int, err := byteArrayToInt(nu_documento)
		check(err)
		nu_documento_uint32 := uint32(nu_documento_int)
		mu.Lock()
		_, exists := keybuffer_map[nu_documento_uint32]
		if !exists {
			keybuffer_map[nu_documento_uint32] = 0
		}
		keybuffer_map[nu_documento_uint32]++
		mu.Unlock()
		lineNum += 1
		if eof_flag {
			break
		}
	}
}

func ParseCSVFile(filepath string, map_statistics map[uint32]*DataFields, keybuffer_map map[uint32]uint32, max_nu_doc_per_parse int) {
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()

	bufferSize := 4 * 1024 // 4Kb
	bufReader := bufio.NewReaderSize(filePtr, bufferSize)

	lineNum := 0
	var line []byte
	delimiter := []byte(";")
	eof_flag := false
	for {
		// Read line by line
		line, err = bufReader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					eof_flag = true
				}
				break
			}
			fmt.Printf("Error reading line: %v\n", err)
			break
		}
		if lineNum == 0 {
			lineNum++
			continue
		}

		if debug {
			fmt.Printf("FILEPATH: %s\n\n", filepath)
		}

		vn_data, vp_data, va_data, nu_documento, nome_cedente, doc_cedente, nome_sacado, doc_sacado := FetchDataCols(line, delimiter)

		if len(nu_documento) < 1 {
			fmt.Printf("FILEPATH WITH LINE ERROR:\n %s\n\n", filepath)
			continue
		}

		if !bytes.Equal(filterDocNum, []byte("")) && !bytes.Equal(filterDocNum, nu_documento) {
			continue
		}
		if !bytes.Equal(filterNomeCedente, []byte("")) && !bytes.Equal(filterNomeCedente, nome_cedente) {
			continue
		}
		if !bytes.Equal(filterDocCedente, []byte("")) && !bytes.Equal(filterDocCedente, doc_cedente) {
			continue
		}
		if !bytes.Equal(filterNomeSacado, []byte("")) && !bytes.Contains(nome_sacado, filterNomeSacado) {
			continue
		}
		if !bytes.Equal(filterDocSacado, []byte("")) && !bytes.Equal(filterDocSacado, doc_sacado) {
			continue
		}

		// Lock before modifying the shared structure
		nu_documento_int, err := byteArrayToInt(nu_documento)
		check(err)
		nu_documento_uint32 := uint32(nu_documento_int)

		mu.Lock()
		_, exists := keybuffer_map[nu_documento_uint32]
		if !exists{
			mu.Unlock()
			if eof_flag {
				break
			}
			continue
		}

		df, exists := map_statistics[nu_documento_uint32]
		if !exists {
			if len(map_statistics) >= max_nu_doc_per_parse{
				mu.Unlock()
				if eof_flag {
					break
				}
				continue
			}
			df = getDataField()
	    	df.SetInitialValues()
			map_statistics[nu_documento_uint32] = df
		}

		vn, err := ParseFloat32(vn_data)
		check(err)
		vp, err := ParseFloat32(vp_data)
		check(err)
		va, err := ParseFloat32(va_data)
		check(err)

		df.vn.ComputeStatistics(float32(vn))
		df.vp.ComputeStatistics(float32(vp))
		df.va.ComputeStatistics(float32(va))

		keybuffer_map[nu_documento_uint32]--
		if keybuffer_map[nu_documento_uint32] == 0{
			delete(keybuffer_map, nu_documento_uint32)
		}

		mu.Unlock()

		lineNum += 1

		if eof_flag {
			break
		}
	}
}

func WriteDataRow(b []byte, w io.Writer, key uint32, df *DataFields) error {
	// Append key and separator.
	b = strconv.AppendUint(b, uint64(key), 10)
	separator := byte(',')
	b = append(b, separator)

	b = strconv.AppendFloat(b, float64(df.vn.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.sum)/float64(df.vn.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.min), 'f', -1, 32)
	b = append(b, separator)

	b = strconv.AppendFloat(b, float64(df.vp.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.sum)/float64(df.vp.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.min), 'f', -1, 32)
	b = append(b, separator)

	b = strconv.AppendFloat(b, float64(df.va.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.sum)/float64(df.va.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.min), 'f', -1, 32)
	b = append(b, '\n')

	_, err := w.Write(b)
	return err
}

func CreateOutputFile()string{
	err := os.MkdirAll("output", 0755)
	check(err)

	var output_filename bytes.Buffer
	// Build new file
	name_prefix := "output/calculations"
	output_filename.WriteString(name_prefix)
	//Filter to add to output file name
	if !bytes.Equal(filterNomeCedente, []byte("")){
		output_filename.WriteString("_nome_cedente_is_")
		output_filename.WriteString(string(filterNomeCedente[:]))
	}
	if !bytes.Equal(filterDocCedente, []byte("")){
		output_filename.WriteString("_doc_cedente_is_")
		output_filename.WriteString(string(filterDocCedente[:]))
	}
	if !bytes.Equal(filterNomeSacado, []byte("")){
		output_filename.WriteString("_nome_sacado_contains_")
		output_filename.WriteString(string(filterNomeSacado[:]))
	}
	if !bytes.Equal(filterDocSacado, []byte("")){
		output_filename.WriteString("_doc_sacado_is_")
		output_filename.WriteString(string(filterDocSacado[:]))
	}
	
	output_filename.WriteString(".csv")
	
	filePtr, err := os.OpenFile(output_filename.String(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	check(err)
	defer filePtr.Close()

	return output_filename.String()
}

func GenerateOutputFile(map_statistics map[uint32]*DataFields, tag int, output_filename string, iteration int) {
	filePtr, err := os.OpenFile(output_filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	check(err)
	defer filePtr.Close()

	// Use buffered writer
	writer := bufio.NewWriter(filePtr)

	// Write header to file
	if iteration == 1{
		col_names := "NU_DOCUMENTO,VN_SOMA,VN_MEDIA,VN_MAX,VN_MIN,VP_SOMA,VP_MEDIA,VP_MAX,VP_MIN,VA_SOMA,VA_MEDIA,VA_MAX,VA_MIN\n" //,NOME_CEDENTE,DOC_CEDENTE,NOME_SACADO,DOC_SACADO\n"
		writer.WriteString(col_names)
		writer.Flush()
	}

	var buf [256]byte
	b := buf[:0]
	for key, df := range map_statistics {
		err = WriteDataRow(b, writer, key, df)
		check(err)
	}

	writer.Flush()

	runtime.GC()
}

func main() {
	start := time.Now()
	cpuProfileFile, err := os.Create("cpu_profile.pprof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer cpuProfileFile.Close()

	if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	cwd := GetCWD()
	folderPath := cwd + "/files"

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		log.Fatalf("Error: Folder %s does not exist.\n", folderPath)
	}

	entries := GetFilePathList(folderPath)

	// Filter CSV files first
	var filenames []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.Contains(name, "Zone.Identifier") && strings.HasSuffix(name, ".csv") {
		filenames = append(filenames, folderPath+"/"+name)
		}
	}

	// Create a channel to distribute work
	filesChan := make(chan string, len(filenames))
	var wg sync.WaitGroup

	// Determine number of workers
	numWorkers := runtime.NumCPU()
	fmt.Printf("Max Workers: %d\n", numWorkers)

	keybuffer_map := make(map[uint32]uint32, 1000000)
	// First pass generate keybuffer_map
	for i:=0; i<numWorkers; i++{
		wg.Add(1)
		go func() {
			defer wg.Done()

			for filename := range filesChan {
				GenerateKeysBuffer(filename, keybuffer_map)
			}
		}()
	}

	// Feed files into the channel
	for _, filename := range filenames {
		filesChan <- filename
	}
	close(filesChan)
	wg.Wait()

	// Creating my map data structure
	// Key: ~4 bytes
	// Value pointer: 8 bytes
	// DataFields: 48 bytes
	// Subtotal: 4 + 8 + 48 ~ 60 bytes per entry
	// Reopening channel
	output_filename := CreateOutputFile()
	filesChanProc := make(chan string, len(filenames))
	// max_nu_doc_per_parse_float := len(keybuffer_map)/10
	max_nu_doc_per_parse := 100000// int(max_nu_doc_per_parse_float)
	map_statistics := make(map[uint32]*DataFields, max_nu_doc_per_parse)
	preAllocateDataFieldsPool(max_nu_doc_per_parse)
	
	buf_len := len(keybuffer_map)
	c := 1
	for {
		fmt.Printf("Round: %d\n", c)
		if buf_len <= 0{
			break
		}
		for i:=0; i<numWorkers; i++{
			wg.Add(1)
			go func() {
				defer wg.Done()
				for filename := range filesChanProc {
					ParseCSVFile(filename, map_statistics, keybuffer_map, max_nu_doc_per_parse)
				}
			}()
		}
		
		// Feed files into the channel
 	  	for _, filename := range filenames {
 	  	    filesChanProc <- filename
 	  	}
	    	close(filesChanProc)
		wg.Wait()
		GenerateOutputFile(map_statistics, c, output_filename,c)
		// iterate through each key to delete
		for key, df := range map_statistics {
			putDataField(df)
			delete(map_statistics, key)
		}
		buf_len = len(keybuffer_map)
		filesChanProc = make(chan string, len(filenames))
		c++
	}
	runtime.GC()


	// Memory Profiling
	f, err := os.Create("mem_profile.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Execution Time: %s\nDone!", elapsed)
}
