package main

import (
	ui "benchmarkDB/ui"
	"context"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	fmt.Println("************************************************")

	// Connection to MySql

	sqldb, err := sql.Open("mysql", "MYMYSQL_USERNAME:SQL_PASS@tcp(localhost:3306)/MYSQL_DATABASE")
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}

	err = sqldb.Ping()
	if err != nil {
		fmt.Println("Error pinging database:", err)
		return
	}

	fmt.Println("Connected to MySQL database!")

	// connection to MongoDB
	const uri = "mongodb://localhost:27017/MONGODB_DATABASE"
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("Pinged the server. Successfully connected to MongoDB!")

	fmt.Println("************************************************")

	p := tea.NewProgram(ui.InitialModel())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error occured: %v", err)
		os.Exit(1)
	}

	defer sqldb.Close()
}
