package database

import (
	"database/sql"
	"log"

	"github.com/biexiang/dbsync/config"

	_ "github.com/go-sql-driver/mysql"
)

var DB = make(map[string]*sql.DB)

//InitDB 初始化连接
func InitDB() {
	GetConn(config.W.Source)
	GetConn(config.W.Destination)
}

//GetConn 获取连接
func GetConn(conn config.Conn) *sql.DB {
	if _, ok := DB[conn.Host]; ok {
		return DB[conn.Host]
	}
	//root:root@tcp(127.0.0.1:3306)/test
	dsn := conn.User + ":" + conn.Pass + "@tcp(" + conn.Host + ":" + conn.Port + ")/" + conn.Database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	DB[conn.Host] = db
	return db
}
