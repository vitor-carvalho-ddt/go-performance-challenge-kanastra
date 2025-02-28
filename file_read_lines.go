
package main

import(
	"fmt"
	"os"
	"path/filepath"
	"bufio"
)

func check(e error){
	if e != nil {
		panic(e)
	}
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
	for scanner.Scan(){
		if i >= 5{
			break;
		}
		line := scanner.Text()
		fmt.Println("Line: ", line)
		i += 1
	}

	if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}