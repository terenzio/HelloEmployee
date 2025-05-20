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
}

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

func batchAndInsertEmployees(db *sql.DB, employees []Employee, batchSize int) {
	createBatchedTable(db)

	for i := 0; i < len(employees); i += batchSize {
		end := i + batchSize
		if end > len(employees) {
			end = len(employees)
		}
		batch := employees[i:end]
		metaBatch := []map[string]interface{}{}
		for _, e := range batch {
			metaBatch = append(metaBatch, e.Meta)
		}

		batchName := fmt.Sprintf("alice_batch_%d", (i/batchSize)+1)
		metaJSON, _ := json.Marshal(metaBatch)

		_, err := db.Exec(
			`INSERT INTO employee_batched (employee_name, employee_department, employee_meta) VALUES (?, ?, ?)`,
			batchName, "Engineering", metaJSON,
		)
		if err != nil {
			log.Fatalf("Insert into employee_batched failed: %v", err)
		}
		fmt.Printf("Inserted batch: %s\n", batchName)
	}
}
