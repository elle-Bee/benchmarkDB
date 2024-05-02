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
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type Record struct {
	Name       string
	School     string
	Job        string
	Department string
	Earnings   float64
	Year       int
}

func Create() {
	// Dummy data to be inserted
	data1 := []Record{
		{Name: "Rajesh Kumar", School: "Delhi Public School", Job: "Software Engineer", Department: "Engineering", Earnings: 50000, Year: 2023},
		{Name: "Priya Patel", School: "Kendriya Vidyalaya", Job: "Data Analyst", Department: "Analytics", Earnings: 45000, Year: 2023},
		{Name: "Amit Singh", School: "St. Xavier's School", Job: "Marketing Manager", Department: "Marketing", Earnings: 60000, Year: 2023},
		{Name: "Sunita Gupta", School: "Holy Cross School", Job: "HR Executive", Department: "Human Resources", Earnings: 48000, Year: 2023},
	}

	data2 := []Record{
		{Name: "Vikas Sharma", School: "DPS School", Job: "Software Developer", Department: "IT", Earnings: 55000, Year: 2023},
		{Name: "Anita Verma", School: "KV School", Job: "Data Scientist", Department: "Analytics", Earnings: 48000, Year: 2023},
		{Name: "Sachin Gupta", School: "St. Xavier's School", Job: "Sales Manager", Department: "Sales", Earnings: 62000, Year: 2023},
		{Name: "Meena Singh", School: "Holy Family School", Job: "HR Manager", Department: "Human Resources", Earnings: 52000, Year: 2023},
		{Name: "Rajesh Verma", School: "DPS School", Job: "Software Engineer", Department: "IT", Earnings: 55000, Year: 2023},
		{Name: "Anjali Tiwari", School: "KV School", Job: "Data Analyst", Department: "Analytics", Earnings: 49000, Year: 2023},
		{Name: "Rahul Singh", School: "St. Xavier's School", Job: "Marketing Manager", Department: "Marketing", Earnings: 61000, Year: 2023},
		{Name: "Sunita Sharma", School: "Holy Family School", Job: "HR Executive", Department: "Human Resources", Earnings: 53000, Year: 2023},
	}

	tables := []string{"table1", "table2", "table3", "table4"} // Representing MongoDB collections or MySQL tables

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

	// Collect time taken for inserts
	var singleThreadedMongoDBTime, singleThreadedMySQLTime, multiThreadedMongoDBTime, multiThreadedMySQLTime time.Duration

	// Single Threaded
	fmt.Println("************Performing single-threaded inserts***************")
	start := time.Now()
	err = SingleThreadedInsert(mongoClient, tables, data1)
	if err != nil {
		fmt.Println("Error inserting data into MongoDB:", err)
	} else {
		singleThreadedMongoDBTime = time.Since(start)
		fmt.Println("Time taken for single-threaded MongoDB insert:", singleThreadedMongoDBTime)
	}

	start = time.Now()
	err = SingleThreadedInsert(mysqlDB, tables, data1)
	if err != nil {
		fmt.Println("Error inserting data into MySQL:", err)
	} else {
		singleThreadedMySQLTime = time.Since(start)
		fmt.Println("Time taken for single-threaded MySQL insert:", singleThreadedMySQLTime)
	}
	fmt.Println("*************************************************************")

	// Multi Threaded
	fmt.Println("************Performing multi-threaded inserts***************")
	start = time.Now()
	err = MultiThreadedInsert(mongoClient, tables, data2)
	if err != nil {
		fmt.Println("Error inserting data into multi-threaded MongoDB:", err)
	} else {
		multiThreadedMongoDBTime = time.Since(start)
		fmt.Println("Time taken for multi-threaded MongoDB insert:", multiThreadedMongoDBTime)
	}

	start = time.Now()
	err = MultiThreadedInsert(mysqlDB, tables, data2)
	if err != nil {
		fmt.Println("Error inserting data into multi-threaded MySQL:", err)
	} else {
		multiThreadedMySQLTime = time.Since(start)
		fmt.Println("Time taken for multi-threaded MySQL insert:", multiThreadedMySQLTime)
	}
	fmt.Println("*************************************************************")

	// Plotting the graph
	plotGraph(singleThreadedMongoDBTime, singleThreadedMySQLTime, multiThreadedMongoDBTime, multiThreadedMySQLTime)

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

func SingleThreadedInsert(client interface{}, tables []string, data []Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		for _, table := range tables {
			collection := mongoClient.Database("mydb").Collection(table)
			for _, record := range data {
				_, err := collection.InsertOne(context.Background(), record)
				if err != nil {
					return err
				}
			}
		}
		return nil
	case *sql.DB:
		mysqlDB := c
		for _, table := range tables {
			query := "INSERT INTO " + table + " (Name, School, Job, Department, Earnings, Year) VALUES (?, ?, ?, ?, ?, ?)"
			stmt, err := mysqlDB.Prepare(query)
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, record := range data {
				_, err := stmt.Exec(record.Name, record.School, record.Job, record.Department, record.Earnings, record.Year)
				if err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func MultiThreadedInsert(client interface{}, tables []string, data []Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := SingleThreadedInsert(mongoClient, tables, data)
			if err != nil {
				fmt.Println("Error inserting data into MongoDB:", err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := SingleThreadedInsert(mysqlDB, tables, data)
			if err != nil {
				fmt.Println("Error inserting data into MySQL:", err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func plotGraph(singleThreadedMongoDBTime, singleThreadedMySQLTime, multiThreadedMongoDBTime, multiThreadedMySQLTime time.Duration) {
	p := plot.New()

	// Create data points for the plot
	data := plotter.Values{singleThreadedMongoDBTime.Seconds(), singleThreadedMySQLTime.Seconds(), multiThreadedMongoDBTime.Seconds(), multiThreadedMySQLTime.Seconds()}
	labels := []string{"Single-Threaded MongoDB", "Single-Threaded MySQL", "Multi-Threaded MongoDB", "Multi-Threaded MySQL"}

	// Create a horizontal bar chart
	bars, err := plotter.NewBarChart(data, vg.Points(50))
	if err != nil {
		fmt.Println("Error creating bar chart:", err)
		return
	}

	// Add the bars to the plot
	p.Add(bars)

	// Set the labels and title
	p.Y.Label.Text = "Time (seconds)"
	p.X.Label.Text = "Insert Operations"
	p.Title.Text = "Time taken for insert operations"

	// Set labels for X axis
	p.NominalX(labels...)

	// Save the plot to a file
	if err := p.Save(10*vg.Inch, 6*vg.Inch, "plot_create.png"); err != nil {
		fmt.Println("Error saving plot:", err)
	}
}
