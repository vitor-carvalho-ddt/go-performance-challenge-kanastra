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
	// Local Placeholders
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
	df.vn.setMax(float32(math.Inf(-1)))
	df.vn.setMin(float32(math.Inf(1)))

	df.vp.setMax(float32(math.Inf(-1)))
	df.vp.setMin(float32(math.Inf(1)))

	df.va.setMax(float32(math.Inf(-1)))
	df.va.setMin(float32(math.Inf(1)))
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

func FetchDataCols(line_bytes []byte, delimiter_bytes []byte) ([]byte, []byte, []byte, []byte) {
	// buffer
	var nu_doc_data []byte
	var vn_data []byte
	var vp_data []byte
	var va_data []byte

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

	uint16bufferPool.Put(delimiter_positions)

	// testing my own function (removes ; inplace)
	RemoveComma(vn_data)
	RemoveComma(vp_data)
	RemoveComma(va_data)

	// parts := bytes.Split(line_bytes, delimiter_bytes)

	if len(nu_doc_data) == 0 {
		return []byte{}, []byte{}, []byte{}, []byte{}
	}

	return vn_data, vp_data, va_data, nu_doc_data
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
	for index := range data {
		if data[index] == byte(',') {
			helper := index
			for {
				data[helper] = data[helper+1]
				helper++
				if helper >= data_len-1 {
					data[helper] = byte('0')
					data_len = len(data)
					break
				}
			}
		}
	}
}

func ParseCSVFile(filepath string, map_statistics map[uint32]*DataFields, wg *sync.WaitGroup) {
	// Signal that this goroutine is done
	defer wg.Done()

	// Open File Ptr
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()

	// Counting Lines in the file
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

		if debug {
			fmt.Printf("FILEPATH: %s\n\n", filepath)
		}

		vn_data, vp_data, va_data, nu_documento := FetchDataCols(line, delimiter)

		if len(nu_documento) < 1 {
			fmt.Printf("FILEPATH WITH LINE ERROR:\n %s\n\n", filepath)
			continue
		}

		// Filter map so we can loop through filters
		if !bytes.Equal(filterDocNum, []byte("")) && !bytes.Equal(filterDocNum, nu_documento) {
			continue
		}

		// if !bytes.Equal(filterNomeCedente, []byte("")) && !bytes.Equal(filterNomeCedente, nome_cedente) {
		// 	continue
		// }
		// if !bytes.Equal(filterDocCedente, []byte("")) && !bytes.Equal(filterDocCedente, doc_cedente) {
		// 	continue
		// }
		// if !bytes.Equal(filterNomeSacado, []byte("")) && !bytes.Equal(filterNomeSacado, nome_sacado) {
		// 	continue
		// }
		// if !bytes.Equal(filterDocSacado, []byte("")) && !bytes.Equal(filterDocSacado, doc_sacado) {
		// 	continue
		// }

		// Lock before modifying the shared structure
		nu_documento_int, err := byteArrayToInt(nu_documento)
		check(err)
		nu_documento_uint32 := uint32(nu_documento_int)
		mu.Lock()
		df, exists := map_statistics[nu_documento_uint32]
		if !exists {
			df = &DataFields{}
			df.SetInitialValues()
			map_statistics[nu_documento_uint32] = df
		}

		vn, err := strconv.ParseFloat(string(vn_data), 32)
		check(err)
		vp, err := strconv.ParseFloat(string(vp_data), 32)
		check(err)
		va, err := strconv.ParseFloat(string(va_data), 32)
		check(err)

		df.vn.ComputeStatistics(float32(vn))
		df.vp.ComputeStatistics(float32(vp))
		df.va.ComputeStatistics(float32(va))
		mu.Unlock()

		lineNum += 1

		if eof_flag {
			break
		}
	}
}

func WriteDataRowNew(b []byte, w io.Writer, key uint32, df *DataFields) error {
	// Append key and separator.
	b = strconv.AppendUint(b, uint64(key), 10)
	separator := byte(',')
	b = append(b, separator)

	// Append "vn" values.
	b = strconv.AppendFloat(b, float64(df.vn.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.sum)/float64(df.vn.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vn.min), 'f', -1, 32)
	b = append(b, separator)

	// Append "vp" values.
	b = strconv.AppendFloat(b, float64(df.vp.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.sum)/float64(df.vp.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.vp.min), 'f', -1, 32)
	b = append(b, separator)

	// Append "va" values.
	b = strconv.AppendFloat(b, float64(df.va.sum), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.sum)/float64(df.va.num_records), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.max), 'f', -1, 32)
	b = append(b, separator)
	b = strconv.AppendFloat(b, float64(df.va.min), 'f', -1, 32)
	b = append(b, '\n')

	// Write the complete row at once.
	_, err := w.Write(b)
	return err
}

func GenerateOutputFile(map_statistics map[uint32]*DataFields) {
	err := os.MkdirAll("output", 0755)
	check(err)
	// Build new file
	output_filename := "output/calculations.csv"
	filePtr, err := os.OpenFile(output_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	check(err)
	defer filePtr.Close()

	// Use buffered writer
	writer := bufio.NewWriter(filePtr)

	// Write to file
	col_names := "NU_DOCUMENTO,VN_SOMA,VN_MEDIA,VN_MAX,VN_MIN,VP_SOMA,VP_MEDIA,VP_MAX,VP_MIN,VA_SOMA,VA_MEDIA,VA_MAX,VA_MIN\n" //,NOME_CEDENTE,DOC_CEDENTE,NOME_SACADO,DOC_SACADO\n"
	writer.WriteString(col_names)
	writer.Flush()

	var buf [256]byte
	b := buf[:0]
	for key, df := range map_statistics {
		err = WriteDataRowNew(b, writer, key, df)
		check(err)
	}

	writer.Flush()

	runtime.GC()
}

func main() {
	// Start time tracking
	start := time.Now()

	// Start CPU profiling
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

	filenames := GetFilePathList(folderPath)

	// Creating my map data structure
	// Key: ~4 bytes
	// Value pointer: 8 bytes
	// DataFields: 48 bytes
	// Subtotal: 4 + 8 + 48 ~ 60 bytes per entry
	mapStatistics := make(map[uint32]*DataFields)
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
