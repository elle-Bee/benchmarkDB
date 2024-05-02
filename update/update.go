package update

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

func Update() {
	tables := []string{"table1", "table2", "table3", "table4"} // Representing MySQL tables
	record := "Chang Lee"
	prevVal := "2019"
	newVal := "9999"
	field := "Year"
	var mongoClient *mongo.Client
	var mysqlDB *sql.DB
	var err error // Declare err outside of the mongoClient initialization

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
	fmt.Println("************Performing single-threaded updates***************")
	var mongoTimes []float64
	var mysqlTimes []float64

	for _, table := range tables {
		start := time.Now()
		err := singleThreadedUpdate(mongoClient, table, field, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s in MongoDB: %v\n", table, err)
		} else {
			mongoTimes = append(mongoTimes, time.Since(start).Seconds())
			fmt.Println("Time taken for single-threaded MongoDB update in", table+":", time.Since(start))
		}

		start = time.Now()
		err = singleThreadedUpdate(mysqlDB, table, field, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s in MySQL: %v\n", table, err)
		} else {
			mysqlTimes = append(mysqlTimes, time.Since(start).Seconds())
			fmt.Println("    Time taken for single-threaded MySQL update in", table+":", time.Since(start))
		}
	}
	fmt.Println("*************************************************************")

	// Multi Threaded
	fmt.Println("************Performing multi-threaded updates***************")
	for _, table := range tables {
		start := time.Now()
		err := multiThreadedUpdate(mongoClient, table, field, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s in multi-threaded MongoDB: %v\n", table, err)
		} else {
			fmt.Println("    Time taken for multi-threaded MongoDB update in", table+":", time.Since(start))
		}

		start = time.Now()
		err = multiThreadedUpdate(mysqlDB, table, field, record, prevVal, newVal)
		if err != nil {
			fmt.Printf("Error updating %s in multi-threaded MySQL: %v\n", table, err)
		} else {
			fmt.Println("    Time taken for multi-threaded MySQL update in", table+":", time.Since(start))
		}
	}
	fmt.Println("*************************************************************")

	// Plotting
	err = plotTimeBarChart("Time taken for Updates", tables, mongoTimes, mysqlTimes)
	if err != nil {
		fmt.Println("Error plotting update times:", err)
	}

	if err != nil { // Check if there's any error occurred during the updates
		fmt.Println("Program completed with errors")
		os.Exit(1) // Exit with non-zero exit code to indicate failure
	}

	fmt.Println("Program completed successfully")
	os.Exit(0)
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

func singleThreadedUpdate(client interface{}, table, field, record, prevVal, newVal string) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		updateQuery := generateMongoDBQuery(table, "", field, record, prevVal, newVal)
		_, err := mongoClient.Database("mydb").Collection(table).UpdateOne(context.TODO(), bson.M{"Name": record}, updateQuery)
		return err
	case *sql.DB:
		mysqlDB := c
		updateQuery := generateMySQLQuery(table, field, record, prevVal, newVal)
		_, err := mysqlDB.Exec(updateQuery)
		return err
	default:
		return errors.New("unsupported client type")
	}
}

func multiThreadedUpdate(client interface{}, table, field, record, prevVal, newVal string) error {
	switch c := client.(type) {
	case *mongo.Client:
		mongoClient := c
		go func() {
			err := singleThreadedUpdate(mongoClient, table, field, record, prevVal, newVal)
			if err != nil {
				fmt.Printf("Error updating %s in MongoDB: %v\n", table, err)
			}
		}()
		return nil
	case *sql.DB:
		mysqlDB := c
		go func() {
			err := singleThreadedUpdate(mysqlDB, table, field, record, prevVal, newVal)
			if err != nil {
				fmt.Printf("Error updating %s in MySQL: %v\n", table, err)
			}
		}()
		return nil
	default:
		return errors.New("unsupported client type")
	}
}

func generateMongoDBQuery(collection, database, field, record, prevVal, newVal string) interface{} {
	updateQuery := bson.M{
		"$set": bson.M{
			field: newVal,
		},
	}
	return updateQuery
}

func generateMySQLQuery(table, field, record, prevVal, newVal string) string {
	return fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE Name = '%s' AND %s = '%s'", table, field, newVal, record, field, prevVal)
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

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "plot_update.png"); err != nil {
		return err
	}

	return nil
}
