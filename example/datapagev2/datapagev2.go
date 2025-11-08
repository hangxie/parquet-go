package main

import (
	"fmt"
	"log"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

// User represents a simple struct to demonstrate data page V2 writing
type User struct {
	ID       int64  `parquet:"name=id, type=INT64"`
	Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age      int32  `parquet:"name=age, type=INT32"`
	Email    string `parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8"`
	Active   bool   `parquet:"name=active, type=BOOLEAN"`
	Salary   *int64 `parquet:"name=salary, type=INT64"`
	Location string `parquet:"name=location, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	filename := "/tmp/datapagev2_example.parquet"

	// Write parquet file using data page V2
	if err := writeDataPageV2(filename); err != nil {
		log.Fatalf("Failed to write parquet file: %v", err)
	}

	// Read and verify the parquet file
	if err := readParquetFile(filename); err != nil {
		log.Fatalf("Failed to read parquet file: %v", err)
	}
}

func writeDataPageV2(filename string) error {
	// Create local file writer
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to create file writer: %w", err)
	}

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(User), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set data page version to 2 (default is 1)
	pw.DataPageVersion = 2

	fmt.Println("Writing parquet file with Data Page V2...")

	// Generate and write sample data
	users := []User{
		{ID: 1, Name: "Alice Johnson", Age: 28, Email: "alice@example.com", Active: true, Salary: common.ToPtr(int64(75000)), Location: "New York"},
		{ID: 2, Name: "Bob Smith", Age: 35, Email: "bob@example.com", Active: true, Salary: common.ToPtr(int64(95000)), Location: "San Francisco"},
		{ID: 3, Name: "Charlie Brown", Age: 42, Email: "charlie@example.com", Active: false, Salary: nil, Location: "Chicago"},
		{ID: 4, Name: "Diana Prince", Age: 31, Email: "diana@example.com", Active: true, Salary: common.ToPtr(int64(105000)), Location: "Seattle"},
		{ID: 5, Name: "Eve Wilson", Age: 29, Email: "eve@example.com", Active: true, Salary: common.ToPtr(int64(82000)), Location: "Boston"},
		{ID: 6, Name: "Frank Miller", Age: 38, Email: "frank@example.com", Active: false, Salary: common.ToPtr(int64(67000)), Location: "Austin"},
		{ID: 7, Name: "Grace Lee", Age: 26, Email: "grace@example.com", Active: true, Salary: common.ToPtr(int64(71000)), Location: "Denver"},
		{ID: 8, Name: "Henry Davis", Age: 45, Email: "henry@example.com", Active: true, Salary: common.ToPtr(int64(110000)), Location: "Portland"},
		{ID: 9, Name: "Iris Taylor", Age: 33, Email: "iris@example.com", Active: false, Salary: nil, Location: "Miami"},
		{ID: 10, Name: "Jack Anderson", Age: 40, Email: "jack@example.com", Active: true, Salary: common.ToPtr(int64(98000)), Location: "Atlanta"},
	}

	for _, user := range users {
		if err := pw.Write(user); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("failed to write user: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop writer: %w", err)
	}

	fmt.Printf("Successfully wrote %d users to %s\n", len(users), filename)
	return nil
}

func readParquetFile(filename string) error {
	// Open local file reader
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		return fmt.Errorf("failed to create file reader: %w", err)
	}

	// Create parquet reader
	pr, err := reader.NewParquetReader(fr, new(User), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer func() {
		if stopErr := pr.ReadStopWithError(); stopErr != nil {
			log.Printf("Warning: failed to stop reader: %v", stopErr)
		}
	}()

	fmt.Printf("\nReading parquet file %s...\n", filename)
	fmt.Printf("Total rows: %d\n\n", pr.GetNumRows())

	// Read all users
	users := make([]User, pr.GetNumRows())
	if err := pr.Read(&users); err != nil {
		return fmt.Errorf("failed to read users: %w", err)
	}

	// Display the data
	fmt.Println("ID\tName\t\t\tAge\tEmail\t\t\t\tActive\tSalary\t\tLocation")
	fmt.Println("--------------------------------------------------------------------------------------------")
	for _, user := range users {
		salary := "N/A"
		if user.Salary != nil {
			salary = fmt.Sprintf("$%d", *user.Salary)
		}
		fmt.Printf("%d\t%-20s\t%d\t%-25s\t%v\t%-12s\t%s\n",
			user.ID, user.Name, user.Age, user.Email, user.Active, salary, user.Location)
	}

	return nil
}
