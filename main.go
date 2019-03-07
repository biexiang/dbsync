package main

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/biexiang/dbsync/config"
	"github.com/biexiang/dbsync/database"
)

func main() {

	var err error
	var rows [][]string
	var affectID int64
	var lastID int64
	var fistFlag bool

	//读取配置文件到struct
	config.InitConfig()

	//连接数据库 同步表结构
	dstDB := database.GetConn(config.W.Destination)
	srcDB := database.GetConn(config.W.Source)

	//同步数据
	for _, table := range config.W.Table {

		if table.Rebuild {
			err = truncateTable(dstDB, table)
			if err != nil {
				goto EXCEPTION
			}
		}

		fistFlag = true
		lastID, err = fetchDstLatestID(dstDB, table)
		if err != nil {
			goto EXCEPTION
		}

		for fistFlag || len(rows) > 0 {
			rows, lastID, err = fetchSrcRow(srcDB, table, lastID, table.Batch)
			if err != nil {
				goto EXCEPTION
			}

			fistFlag = false

			//TODO 如果数据插入异常怎么办 主键重复
			for _, row := range rows {
				affectID, err = insertDstRow(dstDB, table, row)
				if err != nil {
					goto EXCEPTION
				}
				if affectID == 0 {
					err = errors.New("affected rows is zero")
					goto EXCEPTION
				}
			}
		}
		log.Println("Done with Table " + table.Name)
	}
	return
EXCEPTION:
	log.Println("Aparently Oops -> ", err)
}

func insertDstRow(db *sql.DB, table config.TableInfo, row []string) (affect int64, err error) {
	var s = "insert into " + table.Name + " values ('" + strings.Join(row, "','") + "')"
	var ret sql.Result
	ret, err = db.Exec(s)
	if err != nil {
		return 0, err
	}
	rowCount, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

func fetchSrcRow(db *sql.DB, table config.TableInfo, id int64, size int64) (ret [][]string, offset int64, err error) {
	var rows *sql.Rows
	strID := "id>" + strconv.FormatInt(id, 10)
	var sl = []string{strID}
	sl = append(sl, table.Where...)
	clause := strings.Join(sl[:], " and ")
	var sql = "select * from " + table.Name + " where " + clause + " order by id asc limit " + strconv.FormatInt(size, 10)

	log.Println(sql)

	rows, err = db.Query(sql)
	if err != nil {
		return nil, 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	lsize := len(columns)
	pts := make([]interface{}, lsize)
	container := make([]interface{}, lsize)
	for i := range pts {
		pts[i] = &container[i]
	}

	for rows.Next() {
		err = rows.Scan(pts...)
		if err != nil {
			return nil, 0, err
		}
		sl := toString(container)
		ret = append(ret, sl)
		offset, err = strconv.ParseInt(sl[0], 10, 0)
		if err != nil {
			return nil, 0, err
		}
	}
	rows.Close()
	if offset == 0 {
		offset = id
	}
	log.Println("Fetched ", id, " - ", size)
	return ret, offset, nil
}

func toString(columns []interface{}) []string {
	var strCln []string
	for _, column := range columns {
		strCln = append(strCln, string(column.([]uint8)))
	}
	return strCln
}

func fetchDstLatestID(db *sql.DB, table config.TableInfo) (id int64, err error) {
	var rows *sql.Rows
	var sql = "select id from " + table.Name + " order by id desc limit 1"
	rows, err = db.Query(sql)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			log.Println(1)
			return 0, err
		}
	}
	rows.Close()
	return id, nil
}

func truncateTable(db *sql.DB, table config.TableInfo) (err error) {
	var sql = "truncate table " + table.Name
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}
