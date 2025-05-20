# Grouping Employees in MariaDB (Golang App with Batching)

This project is a simple but extensible Go application that:

- Connects to a MariaDB database using Docker Compose
- Inserts 10 records into an `employees` table
- Groups those records into batches of 3
- Stores each batch into a second table `employee_batched`
- Prints both the raw records and the batched ones to the console

---

## 🛠 Requirements

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

---

## 🚀 How to Run

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourname/hello-docker.git
   cd hello-docker

   ```

2. Build and run the container:

   ```bash
   docker-compose down -v   # Stops and removes volumes
   docker-compose build
   docker-compose run batch

   docker-compose down     # Clean up
   ```

## 🧱 In-depth analysis

3. Database Screenshots
   ![alt text](employee_table.png)

   ![alt text](employee_batched.png)

4. Group into Batches of 3

   The 10 employee records are split into batches of 3 like so:

   • Batch 1 → Records 1–3

   • Batch 2 → Records 4–6

   • Batch 3 → Records 7–9

   • Batch 4 → Record 10 (last one)

   Each batch is inserted into employee_batched with a name like alice_batch_1.

   ```json
   [
     { "project": "Project-1", "level": "Senior", "batch": 1 },
     { "project": "Project-2", "level": "Senior", "batch": 2 },
     { "project": "Project-3", "level": "Senior", "batch": 3 }
   ]
   ```
