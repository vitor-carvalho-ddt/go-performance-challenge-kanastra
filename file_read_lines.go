package main

import(
	"fmt"
	"os"
	"math"
	"path/filepath"
	"bufio"
	"strings"
	"strconv"
	"golang.org/x/exp/constraints"
)

type Number interface {
    constraints.Integer | constraints.Float
}

func max[T Number](a, b T) T {
	if a > b{
		return a
	}
	return b
}

func min[T Number](a, b T) T {
	if a < b{
		return a
	}
	return b
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
	ds.setSum(ds.sum + value)
	ds.setNumRecords(ds.num_records + 1)
	ds.setMax(max(ds.max, value))
	ds.setMin(min(ds.min, value))
}

func(ds *DataStatistics) PrintStatistics(field_name string){
	fmt.Printf("%s Data:\n", field_name)
	fmt.Printf("Statistics: \nSum: %.3f | Mean: %.2f | Max: %.2f | Min: %.2f\n\n", ds.sum, ds.sum/float32(ds.num_records), ds.max, ds.min)
}

func check(e error){
	if e != nil {
		panic(e)
	}
}

func IndexOfNth(text string, delimiter rune, nth int8) int {
	s := 0;
	last_pos := -1;
	var i int8
    for i = 0; i < nth; i++ {
        s = strings.IndexRune(text[last_pos+1:], delimiter);
        if s == -1{break};
		if i == 0{
			last_pos+=s+1
		}else{
			s+=1
			last_pos+=s
		}
    }
	// fmt.Printf("nth: %d | last_pos: %d\n", nth, last_pos)
    return last_pos;
}


func FetchDataCols(text string, delimiter rune) []string{
	// Constants definition
	const VALOR_NOMINAL_COL int8 = 9
	const VALOR_PRESENTE_COL int8 = 12
	const VALOR_AQUISICAO_COL int8 = 11
	const NU_DOCUMENTO_COL int8 = 16

	data_string_start_pos := IndexOfNth(text, delimiter, VALOR_NOMINAL_COL)

	text = text[data_string_start_pos+1:]
	vn_delimiter_pos := strings.IndexRune(text, delimiter)
	vn_data := text[:vn_delimiter_pos]

	text = text[vn_delimiter_pos+1:]
	vp_delimiter_pos := strings.IndexRune(text, delimiter)
	vp_data := text[:vp_delimiter_pos]

	text = text[vp_delimiter_pos+1:]
	va_delimiter_pos := strings.IndexRune(text, delimiter)
	va_data := text[:va_delimiter_pos]

	nu_doc_start_pos := IndexOfNth(text, delimiter, (NU_DOCUMENTO_COL-VALOR_AQUISICAO_COL))
	text = text[nu_doc_start_pos+1:]
	nu_doc_delimiter_pos := strings.IndexRune(text, delimiter)
	nu_doc_data := text[:nu_doc_delimiter_pos]

	line_data := []string{vn_data, vp_data, va_data, nu_doc_data}

	return line_data
}

func main(){
	// Fetching CWD
	ex, err := os.Executable()
	check(err)
	exPath := filepath.Dir(ex)
	fmt.Println(exPath)

	// Constructing File Path
	filename := "58148845000109_Estoque_PICPAY FGTS FIDC_001.csv"
	filedir := "/files/"
	filepath := exPath + filedir + filename
	fmt.Println(filepath)

	// Opening File Pointer
	filePtr, err := os.Open(filepath)
	check(err)
	defer filePtr.Close()

	// Counting Lines in the file
	scanner := bufio.NewScanner(filePtr)
	i := 0
	// Initializing my map data structure
	map_statistics := make(map[string]*DataFields)
	for scanner.Scan(){
		// Skipping column names
		if i == 0{
			i+=1
			continue
		}
		if i >= 5{
			break;
		}
		line := scanner.Text()
		delimiter := ';'

		line_data := FetchDataCols(line, delimiter)
		fmt.Printf("\n\nData: %v\n\n", line_data)
		nu_documento := line_data[3]

		_, ok := map_statistics[nu_documento]
		if !ok{
			map_statistics[nu_documento] = &DataFields{}
			map_statistics[nu_documento].SetInitialValues()
		}

		vn, err := strconv.ParseFloat(line_data[0], 32)
        check(err)
        vp, err := strconv.ParseFloat(line_data[1], 32)
        check(err)
        va, err := strconv.ParseFloat(line_data[2], 32)
        check(err)

		map_statistics[nu_documento].vn.ComputeStatistics(float32(vn))
		map_statistics[nu_documento].vp.ComputeStatistics(float32(vp))
		map_statistics[nu_documento].va.ComputeStatistics(float32(va))

		map_statistics[nu_documento].vn.PrintStatistics("VN")
		map_statistics[nu_documento].vp.PrintStatistics("VP")
		map_statistics[nu_documento].va.PrintStatistics("VA")

		i += 1
	}

	if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}
