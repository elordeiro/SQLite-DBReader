# SQLite Implementation

This project is a custom implementation of an SQLite database reader and query engine, written in Go.

## Features

-   **Supported SQL Commands:**

    -   `.dbinfo` - Displays information about the database.
    -   `.tables` - Lists all tables in the database.
    -   **SELECT** - Retrieves data from the database.
    -   **FROM** - Specifies the table to select data from.
    -   **WHERE** - Filters records based on specified conditions.
    -   **COUNT** - Returns the number of rows that match a specified condition.

-   **Case-Insensitive SELECT Statements:**  
    The SELECT statement is case-insensitive, allowing for flexible queries.

-   **Automatic Index Utilization:**  
    The program automatically detects and uses indexes on columns, optimizing search queries for faster results.

## Prerequisites

-   **Go Language:**  
    Ensure that you have Go installed on your system. You can download it from the official [Go website](https://golang.org/).

## How to Compile

To compile the project, navigate to the root directory of the project and run the following command:

```bash
$ go build -o sqlite app/*.go
```

This will generate an executable named `sqlite`.

## How to Run

To execute the program, use the following syntax:

```bash
$ ./sqlite "dbpath" "sql"
```

-   **dbpath:** The path to the SQLite database file.
-   **sql:** The SQL query to be executed.

## Example Usage

Here's an example of how to run the program with a sample database and query:

```bash
$ ./sqlite "sample.db" "SELECT id, name FROM apples WHERE color = 'red';"
```

This command will retrieve the `id` and `name` fields from the `apples` table where the `color` is `'red'`.

## Performance Tips

-   **Indexing:**  
    Searches are made faster if there is an index on the column specified in the WHERE clause. The program will automatically check for an index on the column and use it if available.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

---
