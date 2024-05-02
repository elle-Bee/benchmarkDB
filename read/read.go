package read

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Read() {
	tables := []string{"table1", "table2", "table3", "table4"} // Representing MongoDB collections or MySQL tables
	year := "2018"
	field := "Year"
	var mongoClient *mongo.Client
	var mysqlDB *sql.DB
	var err error // Declare err outside of the mongoClient initialization

	// Initialize MongoDB client
	mongoClient, err = initMongoClient() // Assign to err without :=
	if err != nil {
		fmt.Println("Error initializing MongoDB client:", err)
		return
	}
	defer mongoClient.Disconnect(context.Background())

	// Initialize MySQL client
	mysqlDB, err = initMySQLClient() // Add this line to initialize mysqlDB
	if err != nil {
		fmt.Println("Error initializing MySQL client:", err)
		return
	}
	defer mysqlDB.Close()

	// Single Threaded
	fmt.Println("************Performing single-threaded reads***************")
	for _, table := range tables {
		start := time.Now()
		err := singleThreadedRead(mongoClient, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in MongoDB: %v\n", table, err)
		} else {
			fmt.Println("Time taken for single-threaded MongoDB read in", table+":", time.Since(start))
		}

		start = time.Now()
		err = singleThreadedRead(mysqlDB, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in MySQL: %v\n", table, err)
		} else {
			fmt.Println("    Time taken for single-threaded MySQL read in", table+":", time.Since(start))
		}
	}
	fmt.Println("***********************************************************")

	// Multi Threaded
	fmt.Println("************Performing multi-threaded reads***************")
	for _, table := range tables {
		start := time.Now()
		err := multiThreadedRead(mongoClient, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in multi-threaded MongoDB: %v\n", table, err)
		} else {
			fmt.Println("    Time taken for multi-threaded MongoDB read in", table+":", time.Since(start))
		}

		start = time.Now()
		err = multiThreadedRead(mysqlDB, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in multi-threaded MySQL: %v\n", table, err)
		} else {
			fmt.Println("    Time taken for multi-threaded MySQL read in", table+":", time.Since(start))
		}
	}
	fmt.Println("**********************************************************")

	if err != nil { // Check if there's any error occurred during the reads
		fmt.Println("Program completed with errors")
		os.Exit(1) // Exit with non-zero exit code to indicate failure
	}

	fmt.Println("Program completed successfully")
	os.Exit(0)
}

func singleThreadedRead(client interface{}, table, field, year string) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		filter := generateMongoDBFilter(field, year)
		cursor, err := mongoClient.Database("mydb").Collection(table).Find(context.Background(), filter)
		if err != nil {
			return err
		}
		defer cursor.Close(context.Background())
		return nil
	case *sql.DB:
		mysqlDB := c
		query := generateMySQLQuery(table, field, year)
		rows, err := mysqlDB.Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func multiThreadedRead(client interface{}, table, field, year string) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := singleThreadedRead(mongoClient, table, field, year)
			if err != nil {
				fmt.Printf("Error reading %s in MongoDB: %v\n", table, err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := singleThreadedRead(mysqlDB, table, field, year)
			if err != nil {
				fmt.Printf("Error reading %s in MySQL: %v\n", table, err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func initMongoClient() (*mongo.Client, error) {
	// Initialize MongoDB client
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func initMySQLClient() (*sql.DB, error) {
	// Initialize MySQL client
	db, err := sql.Open("mysql", "root:manage@tcp(localhost:3306)/mydb")
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func generateMongoDBFilter(field, year string) interface{} {
	filter := bson.M{
		field: year,
	}
	return filter
}

func generateMySQLQuery(table, field, year string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = '%s'", table, field, year)
}
