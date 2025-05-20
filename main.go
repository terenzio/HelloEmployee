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

func main() {
	dsn := "root:password@tcp(mariadb:3306)/company"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// Create table if not exists
	createTable := `
	CREATE TABLE IF NOT EXISTS employees (
		employee_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		employee_name VARCHAR(100),
		employee_department VARCHAR(100),
		employee_meta LONGTEXT
	);`
	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10 employee records for Alice with different metadata
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
			log.Fatalf("Insert failed: %v", err)
		}
	}
	fmt.Println("Inserted 10 employee records for Alice.")

	// Read and print all employee records
	rows, err := db.Query("SELECT employee_id, employee_name, employee_department, employee_meta FROM employees")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	fmt.Println("\nEmployee Records:")
	for rows.Next() {
		var e Employee
		var metaStr string
		if err := rows.Scan(&e.ID, &e.Name, &e.Department, &metaStr); err != nil {
			log.Fatal(err)
		}
		json.Unmarshal([]byte(metaStr), &e.Meta)
		fmt.Printf("ID: %d, Name: %s, Department: %s, Meta: %+v\n", e.ID, e.Name, e.Department, e.Meta)
	}
}
