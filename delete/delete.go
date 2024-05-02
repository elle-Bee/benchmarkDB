package delete

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"benchmarkDB/create"
)

func Delete() {
	// Dummy data to be inserted
	data := []create.Record{
		{Name: "Rajesh Kumar", School: "Delhi Public School", JobDescription: "Software Engineer", Department: "Engineering", Earnings: 50000, Year: 2023},
		{Name: "Priya Patel", School: "Kendriya Vidyalaya", JobDescription: "Data Analyst", Department: "Analytics", Earnings: 45000, Year: 2023},
		{Name: "Amit Singh", School: "St. Xavier's School", JobDescription: "Marketing Manager", Department: "Marketing", Earnings: 60000, Year: 2023},
		{Name: "Sunita Gupta", School: "Holy Cross School", JobDescription: "HR Executive", Department: "Human Resources", Earnings: 48000, Year: 2023},
	}

	var mongoClient *mongo.Client
	var mysqlDB *sql.DB
	var err error

	// Initialize MongoDB client
	mongoClient, err = initMongoClient()
	if err != nil {
		fmt.Println("Error initializing MongoDB client:", err)
		return
	}
	defer mongoClient.Disconnect(context.Background())

	// Initialize MySQL client
	mysqlDB, err = initMySQLClient()
	if err != nil {
		fmt.Println("Error initializing MySQL client:", err)
		return
	}
	defer mysqlDB.Close()

	// Reinsert records before deletion for single-threaded operations
	fmt.Println("Creating records for single-threaded delete")
	err = create.SingleThreadedInsert(mongoClient, data)
	if err != nil {
		fmt.Println("Error reinserting data into MongoDB:", err)
		return
	}

	err = create.SingleThreadedInsert(mysqlDB, data)
	if err != nil {
		fmt.Println("Error reinserting data into MySQL:", err)
		return
	}

	// Single Threaded Delete
	fmt.Println("************Performing single-threaded deletes***************")
	err = singleThreadedDelete(mongoClient, data)
	if err != nil {
		fmt.Println("Error deleting data from MongoDB:", err)
	} else {
		fmt.Println("Single-threaded MongoDB delete successful")
	}

	err = singleThreadedDelete(mysqlDB, data)
	if err != nil {
		fmt.Println("Error deleting data from MySQL:", err)
	} else {
		fmt.Println("Single-threaded MySQL delete successful")
	}
	fmt.Println("*************************************************************")

	// Reinsert records before deletion for multi-threaded operations
	fmt.Println("Recreating records for multi-threaded delete")
	err = create.SingleThreadedInsert(mongoClient, data)
	if err != nil {
		fmt.Println("Error reinserting data into MongoDB:", err)
		return
	}

	err = create.SingleThreadedInsert(mysqlDB, data)
	if err != nil {
		fmt.Println("Error reinserting data into MySQL:", err)
		return
	}

	// Multi Threaded Delete
	fmt.Println("************Performing multi-threaded deletes***************")
	err = multiThreadedDelete(mongoClient, data)
	if err != nil {
		fmt.Println("Error deleting data from multi-threaded MongoDB:", err)
	} else {
		fmt.Println("Multi-threaded MongoDB delete successful")
	}

	err = multiThreadedDelete(mysqlDB, data)
	if err != nil {
		fmt.Println("Error deleting data from multi-threaded MySQL:", err)
	} else {
		fmt.Println("Multi-threaded MySQL delete successful")
	}
	fmt.Println("************************************************************")

	if err != nil {
		fmt.Println("Program completed with errors")
		os.Exit(1)
	}

	fmt.Println("Program completed successfully")
	os.Exit(0)
}

func initMongoClient() (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func initMySQLClient() (*sql.DB, error) {
	db, err := sql.Open("mysql", "root:manage@tcp(localhost:3306)/mydb")
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func singleThreadedDelete(client interface{}, data []create.Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		collection := mongoClient.Database("mydb").Collection("collectionName")
		for _, record := range data {
			filter := bson.M{"Name": record.Name, "Year": record.Year} // Assuming Name and Year as unique identifiers
			_, err := collection.DeleteOne(context.Background(), filter)
			if err != nil {
				return err
			}
		}
		return nil
	case *sql.DB:
		mysqlDB := c
		query := "DELETE FROM tableName WHERE Name = ? AND Year = ?"
		stmt, err := mysqlDB.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, record := range data {
			_, err := stmt.Exec(record.Name, record.Year)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func multiThreadedDelete(client interface{}, data []create.Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := singleThreadedDelete(mongoClient, data)
			if err != nil {
				fmt.Println("Error deleting data from MongoDB:", err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := singleThreadedDelete(mysqlDB, data)
			if err != nil {
				fmt.Println("Error deleting data from MySQL:", err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}
