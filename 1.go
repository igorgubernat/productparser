package main

import (
    "fmt"
    "database/sql"
    _ "github.com/lib/pq"
    "bufio"
    "os"
    "strings"
    "sync"
    "time"
    "flag"
)

var wg sync.WaitGroup

func main () {
    t0 := time.Now()
    db, err := sql.Open("postgres", "user=shop password=amira dbname=shop sslmode=disable")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer db.Close()

    var c int // line counter

    numGoroutines := flag.Int("db", 16, "Number of concurrent database writes")
    rowsPerQuery := flag.Int("q", 1024, "Number of rows per query")
    channelLength := flag.Int("cl", 512, "Buffered channel length")
    flag.Parse()

    scanner := bufio.NewScanner(os.Stdin)
    pc := make(chan string, *channelLength)

    for i := 1; i <= *numGoroutines; i++ {
        wg.Add(1)
        go addRow(db, pc, *rowsPerQuery)
    }

    for scanner.Scan() {
        c++
        s := scanner.Text()
        s = strings.Replace(s, "'", "''", -1)
        fields := strings.Split(s, "\t")
        s = fmt.Sprintf("(%s, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s')", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8])
        if c == 1 {
            continue
        }
        if c % 1000 == 0 {
            fmt.Print("*")
        }
        pc <- s
    }

    close(pc)
    wg.Wait()
    t1 := time.Now()
    fmt.Printf("\nTime elapsed: %v\n", t1.Sub(t0))
}

func addRow (db *sql.DB, pc chan string, rowsPerQuery int) {
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