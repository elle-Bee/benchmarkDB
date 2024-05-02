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
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
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
	singleThreadedMongoTimes := make([]float64, len(tables))
	singleThreadedMySQLTimes := make([]float64, len(tables))

	for i, table := range tables {
		start := time.Now()
		err := singleThreadedRead(mongoClient, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in MongoDB: %v\n", table, err)
		} else {
			singleThreadedMongoTimes[i] = time.Since(start).Seconds()
			fmt.Println("Time taken for single-threaded MongoDB read in", table+":", time.Since(start))
		}

		start = time.Now()
		err = singleThreadedRead(mysqlDB, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in MySQL: %v\n", table, err)
		} else {
			singleThreadedMySQLTimes[i] = time.Since(start).Seconds()
			fmt.Println("    Time taken for single-threaded MySQL read in", table+":", time.Since(start))
		}
	}
	fmt.Println("***********************************************************")

	// Multi Threaded
	fmt.Println("************Performing multi-threaded reads***************")
	multiThreadedMongoTimes := make([]float64, len(tables))
	multiThreadedMySQLTimes := make([]float64, len(tables))

	for i, table := range tables {
		start := time.Now()
		err := multiThreadedRead(mongoClient, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in multi-threaded MongoDB: %v\n", table, err)
		} else {
			multiThreadedMongoTimes[i] = time.Since(start).Seconds()
			fmt.Println("    Time taken for multi-threaded MongoDB read in", table+":", time.Since(start))
		}

		start = time.Now()
		err = multiThreadedRead(mysqlDB, table, field, year)
		if err != nil {
			fmt.Printf("Error reading %s in multi-threaded MySQL: %v\n", table, err)
		} else {
			multiThreadedMySQLTimes[i] = time.Since(start).Seconds()
			fmt.Println("    Time taken for multi-threaded MySQL read in", table+":", time.Since(start))
		}
	}
	fmt.Println("**********************************************************")

	// Plotting
	err = plotTimeBarChart("Time taken for Single-Threaded Reads", tables, singleThreadedMongoTimes, singleThreadedMySQLTimes)
	if err != nil {
		fmt.Println("Error plotting single-threaded reads:", err)
	}
	err = plotTimeBarChart("Time taken for Multi-Threaded Reads", tables, multiThreadedMongoTimes, multiThreadedMySQLTimes)
	if err != nil {
		fmt.Println("Error plotting multi-threaded reads:", err)
	}

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
		cursor, err := mongoClient.Database("MONGODB_DATABASE").Collection(table).Find(context.Background(), filter)
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
	db, err := sql.Open("mysql", "MYSQL_USERNAME:SQL_PASStcp(localhost:3306)/MYSQL_DATABASE")
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

func plotTimeBarChart(title string, labels []string, mongoTimes, mysqlTimes []float64) error {
	p := plot.New()

	p.Title.Text = title
	p.Y.Label.Text = "Time (s)"

	bars1, err := plotter.NewBarChart(plotter.Values(mongoTimes), vg.Points(50))
	if err != nil {
		return err
	}
	bars1.LineStyle.Width = vg.Length(0)
	bars1.Color = plotter.DefaultLineStyle.Color
	bars1.Offset = -vg.Points(25)
	p.Add(bars1)

	bars2, err := plotter.NewBarChart(plotter.Values(mysqlTimes), vg.Points(50))
	if err != nil {
		return err
	}
	bars2.LineStyle.Width = vg.Length(0)
	bars2.Color = plotter.DefaultLineStyle.Color
	bars2.Offset = vg.Points(25)
	p.Add(bars2)

	p.Legend.Add("MongoDB", bars1)
	p.Legend.Add("MySQL", bars2)
	p.NominalX(labels...)

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "./PLOTS/plot_read.png"); err != nil {
		return err
	}

	return nil
}
