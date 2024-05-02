package create

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Record struct {
	Name           string
	School         string
	JobDescription string
	Department     string
	Earnings       float64
	Year           int
}

func Create() {
	// Dummy data to be inserted
	data := []Record{
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

	// Single Threaded
	fmt.Println("************Performing single-threaded inserts***************")
	start := time.Now()
	err = SingleThreadedInsert(mongoClient, data)
	if err != nil {
		fmt.Println("Error inserting data into MongoDB:", err)
	} else {
		fmt.Println("Time taken for single-threaded MongoDB insert:", time.Since(start))
	}

	start = time.Now()
	err = SingleThreadedInsert(mysqlDB, data)
	if err != nil {
		fmt.Println("Error inserting data into MySQL:", err)
	} else {
		fmt.Println("    Time taken for single-threaded MySQL insert:", time.Since(start))
	}
	fmt.Println("*************************************************************")

	// Multi Threaded
	fmt.Println("************Performing multi-threaded inserts***************")
	start = time.Now()
	err = MultiThreadedInsert(mongoClient, data)
	if err != nil {
		fmt.Println("Error inserting data into multi-threaded MongoDB:", err)
	} else {
		fmt.Println("    Time taken for multi-threaded MongoDB insert:", time.Since(start))
	}

	start = time.Now()
	err = MultiThreadedInsert(mysqlDB, data)
	if err != nil {
		fmt.Println("Error inserting data into multi-threaded MySQL:", err)
	} else {
		fmt.Println("    Time taken for multi-threaded MySQL insert:", time.Since(start))
	}
	fmt.Println("*************************************************************")

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

func SingleThreadedInsert(client interface{}, data []Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		collection := mongoClient.Database("mydb").Collection("collectionName")
		for _, record := range data {
			_, err := collection.InsertOne(context.Background(), record)
			if err != nil {
				return err
			}
		}
		return nil
	case *sql.DB:
		mysqlDB := c
		query := "INSERT INTO tableName (Name, School, JobDescription, Department, Earnings, Year) VALUES (?, ?, ?, ?, ?, ?)"
		stmt, err := mysqlDB.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, record := range data {
			_, err := stmt.Exec(record.Name, record.School, record.JobDescription, record.Department, record.Earnings, record.Year)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func MultiThreadedInsert(client interface{}, data []Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := SingleThreadedInsert(mongoClient, data)
			if err != nil {
				fmt.Println("Error inserting data into MongoDB:", err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := SingleThreadedInsert(mysqlDB, data)
			if err != nil {
				fmt.Println("Error inserting data into MySQL:", err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}
