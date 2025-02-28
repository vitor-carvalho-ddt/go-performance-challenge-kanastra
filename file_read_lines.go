
package main

import(
	"fmt"
	"os"
	"path/filepath"
	"bufio"
	"strings"
)

func check(e error){
	if e != nil {
		panic(e)
	}
}

func IndexOfNth(text string, delimiter rune, nth int) int {
	s := 0;
	last_pos := -1;
    for i := 0; i < nth; i++ {
        s = strings.IndexRune(text[last_pos+1:], delimiter);
        if s == -1{break};
		if i == 0{
			last_pos+=s+1
		}else{
			s+=1
			last_pos+=s
		}
    }
	fmt.Printf("nth: %d | last_pos: %d\n", nth, last_pos)
    return last_pos;
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
		// fmt.Println("Line: ", line)
		delimiter := ';'
		nth := 16

		pos := IndexOfNth(line, delimiter, nth)
		pos_next := IndexOfNth(line, delimiter, nth+1)
		data := ""
		if pos_next == -1{
			fmt.Printf("\nLast occurence of delimiter!\n")
			data = line[pos+1:]
		}else{
			data = line[pos+1:pos_next]
		}

		fmt.Printf("\nFinding nth %c in line: pos == %d\n", delimiter, pos)
		fmt.Printf("Data:%s\n", data)

		i += 1
	}

	if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}