package delete

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

	"benchmarkDB/create"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func Delete() {
	// Dummy data to be inserted
	data1 := []create.Record{
		{Name: "Rajesh Kumar", School: "Delhi Public School", Job: "Software Engineer", Department: "Engineering", Earnings: 50000, Year: 2023},
		{Name: "Priya Patel", School: "Kendriya Vidyalaya", Job: "Data Analyst", Department: "Analytics", Earnings: 45000, Year: 2023},
		{Name: "Amit Singh", School: "St. Xavier's School", Job: "Marketing Manager", Department: "Marketing", Earnings: 60000, Year: 2023},
		{Name: "Sunita Gupta", School: "Holy Cross School", Job: "HR Executive", Department: "Human Resources", Earnings: 48000, Year: 2023},
	}

	data2 := []create.Record{
		{Name: "Vikas Sharma", School: "DPS School", Job: "Software Developer", Department: "IT", Earnings: 55000, Year: 2023},
		{Name: "Anita Verma", School: "KV School", Job: "Data Scientist", Department: "Analytics", Earnings: 48000, Year: 2023},
		{Name: "Sachin Gupta", School: "St. Xavier's School", Job: "Sales Manager", Department: "Sales", Earnings: 62000, Year: 2023},
		{Name: "Meena Singh", School: "Holy Family School", Job: "HR Manager", Department: "Human Resources", Earnings: 52000, Year: 2023},
		{Name: "Rajesh Verma", School: "DPS School", Job: "Software Engineer", Department: "IT", Earnings: 55000, Year: 2023},
		{Name: "Anjali Tiwari", School: "KV School", Job: "Data Analyst", Department: "Analytics", Earnings: 49000, Year: 2023},
		{Name: "Rahul Singh", School: "St. Xavier's School", Job: "Marketing Manager", Department: "Marketing", Earnings: 61000, Year: 2023},
		{Name: "Sunita Sharma", School: "Holy Family School", Job: "HR Executive", Department: "Human Resources", Earnings: 53000, Year: 2023},
		{Name: "Nitin Kumar", School: "DPS School", Job: "Software Developer", Department: "IT", Earnings: 56000, Year: 2023},
		{Name: "Priyanka Singh", School: "KV School", Job: "Data Scientist", Department: "Analytics", Earnings: 47000, Year: 2023},
		{Name: "Amit Sharma", School: "St. Xavier's School", Job: "Sales Manager", Department: "Sales", Earnings: 63000, Year: 2023},
		{Name: "Pooja Verma", School: "Holy Family School", Job: "HR Manager", Department: "Human Resources", Earnings: 54000, Year: 2023},
		{Name: "Ajay Singh", School: "DPS School", Job: "Software Developer", Department: "IT", Earnings: 57000, Year: 2023},
		{Name: "Deepika Patel", School: "KV School", Job: "Data Analyst", Department: "Analytics", Earnings: 46000, Year: 2023},
		{Name: "Vikram Singh", School: "St. Xavier's School", Job: "Marketing Manager", Department: "Marketing", Earnings: 60000, Year: 2023},
		{Name: "Monica Sharma", School: "Holy Family School", Job: "HR Executive", Department: "Human Resources", Earnings: 51000, Year: 2023},
		{Name: "Sunil Kumar", School: "DPS School", Job: "Software Developer", Department: "IT", Earnings: 58000, Year: 2023},
		{Name: "Neetu Singh", School: "KV School", Job: "Data Scientist", Department: "Analytics", Earnings: 49000, Year: 2023},
		{Name: "Rohit Verma", School: "St. Xavier's School", Job: "Sales Manager", Department: "Sales", Earnings: 64000, Year: 2023},
		{Name: "Sapna Sharma", School: "Holy Family School", Job: "HR Manager", Department: "Human Resources", Earnings: 55000, Year: 2023},
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

	// Collect time taken for each operation
	var mongoTimes, mysqlTimes []float64

	// Reinsert records before deletion for single-threaded operations
	fmt.Println("Creating records for single-threaded delete")
	start := time.Now()
	err = create.SingleThreadedInsert(mongoClient, tables, data1)
	if err != nil {
		fmt.Println("Error reinserting data into MongoDB:", err)
		return
	}
	mongoTimes = append(mongoTimes, time.Since(start).Seconds())

	start = time.Now()
	err = create.SingleThreadedInsert(mysqlDB, tables, data1)
	if err != nil {
		fmt.Println("Error reinserting data into MySQL:", err)
		return
	}
	mysqlTimes = append(mysqlTimes, time.Since(start).Seconds())

	// Single Threaded Delete
	fmt.Println("************Performing single-threaded deletes***************")
	for _, table := range tables {
		start = time.Now()
		err = singleThreadedDelete(mongoClient, table, data1)
		if err != nil {
			fmt.Printf("Error deleting data from MongoDB collection %s: %v\n", table, err)
		} else {
			fmt.Printf("Single-threaded delete successful from MongoDB collection %s\n", table)
			mongoTimes = append(mongoTimes, time.Since(start).Seconds())
		}

		start = time.Now()
		err = singleThreadedDelete(mysqlDB, table, data1)
		if err != nil {
			fmt.Printf("Error deleting data from MySQL table %s: %v\n", table, err)
		} else {
			fmt.Printf("Single-threaded delete successful from MySQL table %s\n", table)
			mysqlTimes = append(mysqlTimes, time.Since(start).Seconds())
		}
	}
	fmt.Println("*************************************************************")

	// Reinsert records before deletion for multi-threaded operations
	fmt.Println("Recreating records for multi-threaded delete")
	start = time.Now()
	err = create.SingleThreadedInsert(mongoClient, tables, data2)
	if err != nil {
		fmt.Println("Error reinserting data into MongoDB:", err)
		return
	}
	mongoTimes = append(mongoTimes, time.Since(start).Seconds())

	start = time.Now()
	err = create.SingleThreadedInsert(mysqlDB, tables, data2)
	if err != nil {
		fmt.Println("Error reinserting data into MySQL:", err)
		return
	}
	mysqlTimes = append(mysqlTimes, time.Since(start).Seconds())

	// Multi Threaded Delete
	fmt.Println("************Performing multi-threaded deletes***************")
	for _, table := range tables {
		start = time.Now()
		err = multiThreadedDelete(mongoClient, table, data2)
		if err != nil {
			fmt.Printf("Error deleting data from multi-threaded MongoDB collection %s: %v\n", table, err)
		} else {
			fmt.Printf("Multi-threaded delete successful from MongoDB collection %s\n", table)
			mongoTimes = append(mongoTimes, time.Since(start).Seconds())
		}

		start = time.Now()
		err = multiThreadedDelete(mysqlDB, table, data2)
		if err != nil {
			fmt.Printf("Error deleting data from multi-threaded MySQL table %s: %v\n", table, err)
		} else {
			fmt.Printf("Multi-threaded delete successful from MySQL table %s\n", table)
			mysqlTimes = append(mysqlTimes, time.Since(start).Seconds())
		}
	}
	fmt.Println("************************************************************")

	// Plotting the graph
	plotGraph(mongoTimes, mysqlTimes, tables)

	if err != nil {
		fmt.Println("Program completed with errors")
		os.Exit(1)
	}

	fmt.Println("Program completed successfully")
	os.Exit(0)
}

// Plot a graph comparing the time taken for MongoDB and MySQL operations
func plotGraph(mongoTimes, mysqlTimes []float64, tables []string) {
	// Create a new plot
	p := plot.New()

	// Create new bar chart
	barWidth := vg.Points(50)
	barsMongo, err := plotter.NewBarChart(plotter.Values(mongoTimes), barWidth)
	if err != nil {
		fmt.Println("Error creating bar chart for MongoDB:", err)
		os.Exit(1)
	}
	barsMongo.LineStyle.Width = vg.Length(0)
	barsMongo.Color = plotutil.Color(0)
	barsMySQL, err := plotter.NewBarChart(plotter.Values(mysqlTimes), barWidth)
	if err != nil {
		fmt.Println("Error creating bar chart for MySQL:", err)
		os.Exit(1)
	}
	barsMySQL.LineStyle.Width = vg.Length(0)
	barsMySQL.Color = plotutil.Color(1)

	// Add bars to the plot
	p.Add(barsMongo, barsMySQL)

	// Set labels and title
	p.Title.Text = "Operation Timings"
	p.X.Label.Text = "Time (ms)"
	p.Y.Label.Text = "Frequency"
	if err := p.Save(6*vg.Inch, 4*vg.Inch, "timings.png"); err != nil {
		fmt.Println("Error saving plot:", err)
	}

	// Save the plot to a PNG file
	err = p.Save(6*vg.Inch, 4*vg.Inch, "./plots/plot_delete.png")
	if err != nil {
		fmt.Println("Error saving plot:", err)
		os.Exit(1)
	}

	fmt.Println("Plot saved as plot.png")
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
	db, err := sql.Open("mysql", ":SQL_PASStcp(localhost:3306)/MYSQL_DATABASE")
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func singleThreadedDelete(client interface{}, table string, data []create.Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		collection := mongoClient.Database("MONGODB_DATABASE").Collection(table)
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
		query := "DELETE FROM " + table + " WHERE Name = ? AND Year = ?"
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

func multiThreadedDelete(client interface{}, table string, data []create.Record) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := singleThreadedDelete(mongoClient, table, data)
			if err != nil {
				fmt.Printf("Error deleting data from MongoDB collection %s: %v\n", table, err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := singleThreadedDelete(mysqlDB, table, data)
			if err != nil {
				fmt.Printf("Error deleting data from MySQL table %s: %v\n", table, err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}
