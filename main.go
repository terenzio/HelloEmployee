package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type Employee struct {
	ID         int
	Name       string
	Department string
	Meta       map[string]interface{}
}

type BatchEmployee struct {
	BatchName  string
	Department string
	MetaBatch  []map[string]interface{}
}

func main() {
	dsn := "root:password@tcp(mariadb:3306)/company"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// Create employees table
	createEmployeesTable := `
	CREATE TABLE IF NOT EXISTS employees (
		employee_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		employee_name VARCHAR(100),
		employee_department VARCHAR(100),
		employee_meta LONGTEXT
	);`
	if _, err := db.Exec(createEmployeesTable); err != nil {
		log.Fatalf("Failed to create employees table: %v", err)
	}

	// Insert 10 Alice records with different metadata
	for i := 1; i <= 10; i++ {
		meta := map[string]interface{}{
			"project": fmt.Sprintf("Project-%d", i),
			"level":   "Senior",
			"batch":   i,
		}
		metaJSON, _ := json.Marshal(meta)

		_, err := db.Exec(
			`INSERT INTO employees (employee_name, employee_department, employee_meta) VALUES (?, ?, ?)`,
			"Alice", "Engineering", metaJSON,
		)
		if err != nil {
			log.Fatalf("Insert into employees failed: %v", err)
		}
	}
	fmt.Println("Inserted 10 employee records for Alice.")

	// Read and print all employee records
	rows, err := db.Query("SELECT employee_id, employee_name, employee_department, employee_meta FROM employees ORDER BY employee_id ASC")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var allEmployees []Employee

	fmt.Println("\nEmployee Records:")
	for rows.Next() {
		var e Employee
		var metaStr string
		if err := rows.Scan(&e.ID, &e.Name, &e.Department, &metaStr); err != nil {
			log.Fatal(err)
		}
		json.Unmarshal([]byte(metaStr), &e.Meta)
		allEmployees = append(allEmployees, e)

		fmt.Printf("ID: %d, Name: %s, Department: %s, Meta: %+v\n", e.ID, e.Name, e.Department, e.Meta)
	}

	// Create and insert into batched table
	batchAndInsertEmployees(db, allEmployees, 3)

	// Read and print all batched records
	rows2, err := db.Query("SELECT employee_name, employee_department, employee_meta FROM employee_batched ORDER BY employee_name ASC")
	if err != nil {
		log.Fatalf("Query for batched table failed: %v", err)
	}
	defer rows2.Close()

	fmt.Println("\nBatched Records:")
	for rows2.Next() {
		var name, dept, metaStr string
		if err := rows2.Scan(&name, &dept, &metaStr); err != nil {
			log.Fatal(err)
		}
		var metaArray []map[string]interface{}
		json.Unmarshal([]byte(metaStr), &metaArray)

		fmt.Printf("Batch: %s, Department: %s\n", name, dept)
		for i, meta := range metaArray {
			fmt.Printf("  Record %d: %+v\n", i+1, meta)
		}
	}
}

// Creates the table `employee_batched` if it doesn't exist
func createBatchedTable(db *sql.DB) {
	createTable := `
	CREATE TABLE IF NOT EXISTS employee_batched (
		employee_name VARCHAR(100) PRIMARY KEY,
		employee_department VARCHAR(100),
		employee_meta LONGTEXT
	);`
	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("Failed to create employee_batched table: %v", err)
	}
}

// batchAndInsertEmployees groups employees into fixed-size batches
// and inserts each group as one row in the `employee_batched` table
func batchAndInsertEmployees(db *sql.DB, employees []Employee, batchSize int) {
	createBatchedTable(db)

	// Loop through employees in steps of batchSize to process each batch
	for i := 0; i < len(employees); i += batchSize {
		// Determine the slice range for this batch
		end := i + batchSize
		if end > len(employees) {
			end = len(employees)
		}
		batch := employees[i:end]

		// Collect the employee_meta fields into a slice
		metaBatch := []map[string]interface{}{}
		for _, e := range batch {
			metaBatch = append(metaBatch, e.Meta)
		}

		// Use batch number in the employee_name as a unique key
		batchName := fmt.Sprintf("alice_batch_%d", (i/batchSize)+1)

		// Marshal the batch metadata slice into JSON
		metaJSON, err := json.Marshal(metaBatch)
		if err != nil {
			log.Fatalf("Failed to marshal metaBatch: %v", err)
		}

		// Insert the batch record into the employee_batched table
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Failed to begin transaction: %v", err)
		}
		defer tx.Rollback()

		_, err = tx.Exec(
			`INSERT INTO employee_batched (employee_name, employee_department, employee_meta) VALUES (?, ?, ?)`,
			batchName, "Engineering", metaJSON,
		)
		if err != nil {
			log.Fatalf("Insert into employee_batched failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			log.Fatalf("Failed to commit transaction: %v", err)
		}
		fmt.Printf("Inserted batch: %s\n", batchName)
	}
}
