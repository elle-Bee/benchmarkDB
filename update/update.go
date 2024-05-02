package update

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func Update() {
	tables := []string{"table1"} // Representing MySQL tables
	var record string
	var prevVal string
	var newVal string
	var mongoClient *mongo.Client
	var mysqlDB *sql.DB

	fmt.Print("Enter the record you want to update: ")
	fmt.Scanf("%v", &record)

	fmt.Print("What was the previous value: ")
	fmt.Scanln("%v", &prevVal)

	fmt.Print("Enter the new value: ")
	fmt.Scanln("%v", &newVal)

	// Single Threaded
	for _, table := range tables {
		start := time.Now()
		err := singleThreadedUpdate(mongoClient, table, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s: %v\n", table, err)
		} else {
			fmt.Printf("Time taken for single-threaded MongoDB update in %s: %v\n", table, time.Since(start))
		}
	}

	for _, table := range tables {
		start := time.Now()
		err := singleThreadedUpdate(mysqlDB, table, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s: %v\n", table, err)
		} else {
			fmt.Printf("Time taken for single-threaded MySQL update in %s: %v\n", table, time.Since(start))
		}
	}

	// Multi Threaded
	for _, table := range tables {
		start := time.Now()
		err := multiThreadedUpdate(mongoClient, table, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s: %v\n", table, err)
		} else {
			fmt.Printf("Time taken for multi-threaded MongoDB update in %s: %v\n", table, time.Since(start))
		}
	}

	for _, table := range tables {
		start := time.Now()
		err := multiThreadedUpdate(mysqlDB, table, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s: %v\n", table, err)
		} else {
			fmt.Printf("Time taken for multi-threaded MySQL update in %s: %v\n", table, time.Since(start))
		}
	}
}

func generateMongoDBQuery(database, collection, record, prevVal, newVal string) interface{} {
	// Generate MongoDB update query
	updateQuery := bson.M{
		"$set": bson.M{
			record: newVal,
		},
		record: prevVal,
	}
	return updateQuery
}

func generateMySQLQuery(table, record, prevVal, newVal string) string {
	// Generate MySQL update query
	return fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s = '%s'", table, record, newVal, record, prevVal)
}

func singleThreadedUpdate(client interface{}, table, record, prevVal, newVal string) error {
	// Perform single-threaded updates
	fmt.Println("Performing single-threaded updates...")

	// Check the type of client
	switch c := client.(type) {
	case *mongo.Client: // MongoDB client
		mongoClient := c
		// Update in MongoDB
		mongoQuery := generateMongoDBQuery(table, "", record, prevVal, newVal)
		_, err := mongoClient.Database("mydb").Collection(table).UpdateOne(context.TODO(), bson.M{record: prevVal}, mongoQuery)
		if err != nil {
			return err
		}
	case *sql.DB: // MySQL database connection
		mysqlDB := c
		// Update in MySQL
		mysqlQuery := generateMySQLQuery(table, record, prevVal, newVal)
		_, err := mysqlDB.Exec(mysqlQuery)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported client type")
	}

	return nil
}

func multiThreadedUpdate(client interface{}, table, record, prevVal, newVal string) error {
	// Perform multi-threaded updates
	fmt.Println("Performing multi-threaded updates...")

	// Check the type of client
	switch c := client.(type) {
	case *mongo.Client: // MongoDB client
		mongoClient := c
		// Update in MongoDB
		go func() {
			err := singleThreadedUpdate(mongoClient, table, record, prevVal, newVal)
			if err != nil {
				fmt.Println("Error updating MongoDB:", err)
			}
		}()
	case *sql.DB: // MySQL database connection
		mysqlDB := c
		// Update in MySQL
		go func() {
			err := singleThreadedUpdate(mysqlDB, table, record, prevVal, newVal)
			if err != nil {
				fmt.Println("Error updating MySQL:", err)
			}
		}()
	default:
		return errors.New("unsupported client type")
	}

	return nil
}
