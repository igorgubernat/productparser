package main

import (
    "fmt"
    "os"
    "bufio"
    "sync"
    "flag"
    "database/sql"
    _ "github.com/lib/pq"
    "strings"
    "time"
)

var wg, parseWg sync.WaitGroup
var c int

func main () {

    t0 := time.Now()

    // Database connection
    db, err := sql.Open("postgres", "user=shop password=amira dbname=shop sslmode=disable")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer db.Close()

    // Parse command line arguments
    splitNumber := flag.Int("f", 8, "Number of chunks to split a file into")
    numGoroutines := flag.Int("db", 16, "Number of concurrent database writes")
    rowsPerQuery := flag.Int("q", 1024, "Number of rows per query")
    channelLength := flag.Int("cl", 512, "Buffered channel length")
    flag.Parse()
    fileName := flag.Args()[0]

    pc := make(chan string, *channelLength)

    // Launch goroutines that write lines to database
    for i := 1; i <= *numGoroutines; i++ {
        wg.Add(1)
        go addRow(db, pc, *rowsPerQuery, i)
    }

    // Open file for reading
    fileInfo, err := os.Stat(fileName)
    if err != nil {
        fmt.Println(err)
        return
    }
    fileSize := fileInfo.Size()
    file, err := os.Open(fileName)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()

    // Split file reading into splitNumber chunks
    step := fileSize / int64(*splitNumber)
    var start, end int64
    for end < fileSize {
        end = splitAt(file, start + step, fileSize)
        if end > fileSize {
            end = fileSize
        }
        parseWg.Add(1)
        go launchReader(fileName, start, end, pc) // Each chunk executes in a goroutine
        start = end + 1
    }

    parseWg.Wait() // Waiting for all file chunks to finish parsing to close the channel
    close(pc)
    wg.Wait() // Waiting for all database inserts to finish

    t1 := time.Now()
    fmt.Printf("\nTime elapsed: %v\n", t1.Sub(t0))
}

// Returns the byte position of the nearest newline after the argument n
// so that no line is split in the middle
func splitAt (file *os.File, n int64, fileSize int64) int64 {
    if n >= fileSize - 100 {
        return fileSize
    }
    b := make([]byte, 100)
    counter := n
    for {
        _, err := file.ReadAt(b, counter)
        if err != nil {
            fmt.Println("error in split ", err)
            os.Exit(1)
        }
        for i, val := range b {
            if val == '\n' {
                return counter + int64(i)
            }
        }
        counter += 100
        if counter >= fileSize - 100 {
            return fileSize
        }
    }
}

// Parses file chunk from position "from" to position "to"
func launchReader (fileName string, from int64, to int64, pc chan string) {

    defer parseWg.Done()
    file, err := os.Open(fileName)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    defer file.Close()

    file.Seek(from, 0) // Seek to the start of required chunk
    readBytes := from // Current position in file
    var s string
    scanner := bufio.NewScanner(file)

    for scanner.Scan() {
        s = scanner.Text()
        readBytes += int64(len(s)) + 1
        if strings.HasPrefix(s, "PRODUCT_ID") { // Omit header line
            continue
        }
        s = strings.Replace(s, "'", "''", -1) // Escaping single quotes for postgres
        fields := strings.Split(s, "\t")
        // Fields 0 and 4 are numbers, so they should not be surrounded by single quotes
        s = fmt.Sprintf("(%s, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s')", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8])
        c++
        if c % 10000 == 0 { // Progress indicator
            fmt.Print("*")
        }
        pc <- s
        if readBytes - 1 >= to { // Stop when we reach the end of required chunk
            return
        }
    }
}

// Writes Product object to the database
func addRow (db *sql.DB, pc chan string, rowsPerQuery int, i int) {
    var err error
    var lastInsertedId int
    var counter int
    var s []string

    defer wg.Done()

    for p := range pc {

        s = append(s, p)
        counter++
        if counter == rowsPerQuery {
            err = db.QueryRow("INSERT INTO ParsedProducts (PRODUCT_ID, PRODUCT_NAME, BRAND_NAME, PRODUCT_SIZE, SOURCE_ID, SOURCE_PRODUCT_ID, PRODUCT_URI, PRODUCT_DESCRIPTION, PRODUCT_IMAGE_URI) VALUES " + strings.Join(s, ", ") + " RETURNING 1").Scan(&lastInsertedId)
            if err != nil {
                fmt.Printf("QueryRow: %s\n", err)
            }
            counter = 0
            s = nil
        }
    }
    if counter > 0 {
        err = db.QueryRow("INSERT INTO ParsedProducts (PRODUCT_ID, PRODUCT_NAME, BRAND_NAME, PRODUCT_SIZE, SOURCE_ID, SOURCE_PRODUCT_ID, PRODUCT_URI, PRODUCT_DESCRIPTION, PRODUCT_IMAGE_URI) VALUES " + strings.Join(s, ", ") + " RETURNING 1").Scan(&lastInsertedId)
        if err != nil {
            fmt.Printf("QueryRow: %s\n", err)
        }
    }
}