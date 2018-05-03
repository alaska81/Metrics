package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	hashgenerator "MetricsNew/hashgenerator"

	_ "github.com/lib/pq"
)

var db *sql.DB
var Requests dbRequests

type Transaction struct {
	Tx *sql.Tx
}

type BdRW interface {
	Select(rows *sql.Rows) error
	Insert(rows *sql.Rows, t *Transaction) error
}

type dbRequests struct {
	requestsIn  map[string]*sql.Stmt
	requestsOut map[string]*sql.Stmt
}

type ClientInfo struct {
	Hash         string
	Phone        string
	Name         string
	Birthday     time.Time
	CreationTime time.Time
}

type ClientOrdersAddress struct {
	ImportID      string
	City          string
	Street        string
	House         int64
	Building      string
	Floor         int64
	Apartment     int64
	Entrance      int64
	DoorphoneCode string

	Phone string
}

type ClientOrders struct {
	ClientHash string
	Address_id int64
}

func main() {
	var err error

	db, err = sql.Open("postgres", "postgres://batman1:3E3k7B8d@localhost:5433/clientinfo?sslmode=disable")
	if err != nil {
		panic(fmt.Errorf("Postgresql not found!: %v", err))
	}

	if err = db.Ping(); err != nil {
		panic(fmt.Errorf("Postgresql not reply!: %v", err))
	}

	if err = Requests.initrequestsIn(); err != nil {
		panic(fmt.Errorf("Postgresql initRequests error: %v", err))
	}
	if err = Requests.initrequestsOut(); err != nil {
		panic(fmt.Errorf("Postgresql initRequests error: %v", err))
	}

	for requestName := range Requests.requestsIn {
		rows, err := Requests.Query(requestName)
		if err != nil {
			panic(fmt.Errorf("Query error: %v", err))
		}

		var rw BdRW
		switch requestName {
		case "Select.ClientInfo2":
			rw = &ClientInfo{}
		case "Select.ClientOrdersAddress2":
			rw = &ClientOrdersAddress{}
		default:
			panic("Не определен запрос!")
		}

		var t Transaction
		if err := t.Begin(); err != nil {
			panic(fmt.Errorf("t.Begin: %v", err))
		}
		defer t.RollBack()

		k := 0
		for rows.Next() {
			if err := rw.Select(rows); err != nil {
				panic(fmt.Errorf("rw.Select: %v", err))
			}
			if err := rw.Insert(rows, &t); err != nil {
				panic(fmt.Errorf("rw.Insert: %v", err))
			}

			k++
			fmt.Printf("\r%s\r", "                                                                                ")
			fmt.Printf("%v", k)

			if k%1000 == 0 {
				if err := t.Commit(); err != nil {
					panic(fmt.Errorf("t.Commit: %v", err))
				}
				if err := t.Begin(); err != nil {
					panic(fmt.Errorf("t.Begin: %v", err))
				}
			}
		}

		if err := t.Commit(); err != nil {
			panic(fmt.Errorf("t.Commit: %v", err))
		}
		defer rows.Close()

	}

	fmt.Println("finish...")

}

func (dbr *dbRequests) initrequestsIn() error {

	dbr.requestsIn = make(map[string]*sql.Stmt)
	var err error
	fmt.Println("Begin init requests")

	///
	dbr.requestsIn["Select.ClientInfo2"], err = db.Prepare(`
		SELECT "Hash", "Phone", "Name", "Birthday", "CreationTime"
		FROM "ClientInfo2"
	`)
	if err != nil {
		return fmt.Errorf("Select.ClientInfo2: %v", err)
	}

	///
	dbr.requestsIn["Select.ClientOrdersAddress2"], err = db.Prepare(`
		SELECT "ID", "City", "Street", "House", "Building" , "Floor", "Apartment", "Entrance", "DoorphoneCode", "Phone"
		FROM "ClientOrdersAddress2"
		INNER JOIN "ClientOrders2" ON "Address_id" = "ID"
		INNER JOIN "ClientInfo2" ON "Hash" = "ClientHash"
		GROUP BY "ID", "City", "Street", "House", "Building" , "Floor", "Apartment", "Entrance", "DoorphoneCode", "Phone"
	`)
	if err != nil {
		return fmt.Errorf("Select.ClientOrdersAddress2: %v", err)
	}

	fmt.Println("End init requests")

	return nil
}

func (dbr *dbRequests) initrequestsOut() error {

	dbr.requestsOut = make(map[string]*sql.Stmt)
	var err error
	fmt.Println("Begin init requests")

	///
	dbr.requestsOut["Select.ClientInfo"], err = db.Prepare(`
		SELECT "Hash", "ID" FROM "ClientInfo" WHERE "Phone" = $1
	`)
	if err != nil {
		return fmt.Errorf("Select.ClientInfo: %v", err)
	}

	dbr.requestsOut["Insert.ClientInfo"], err = db.Prepare(`
		INSERT INTO "ClientInfo" ("Hash", "Phone", "Name", "Password", "Mail", "Bonus", "BonusWord", "Active", "BlackList", "CauseBlackList", "Birthday", "CreationTime") 
		VALUES ($1, $2, $3, '' , '', 0, '', false, false, '', $4, $5)
	`)
	if err != nil {
		return fmt.Errorf("Insert.ClientInfo: %v", err)
	}

	///
	dbr.requestsOut["Select.ClientOrdersAddress"], err = db.Prepare(`
		SELECT "ID" FROM "ClientOrdersAddress" WHERE import_id = $1
	`)
	if err != nil {
		return fmt.Errorf("Select.ClientOrdersAddress: %v", err)
	}

	dbr.requestsOut["Insert.ClientOrdersAddress"], err = db.Prepare(`
		INSERT INTO "ClientOrdersAddress" ("City", "Street", "House", "Building", "Floor", "Apartment", "Entrance", "DoorphoneCode", "Comment", import_id) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '', $9)
	`)
	if err != nil {
		return fmt.Errorf("Insert.ClientOrdersAddress: %v", err)
	}

	///
	dbr.requestsOut["Select.ClientOrders"], err = db.Prepare(`
		SELECT "ID" FROM "ClientOrders" WHERE "Address_id" = $1
	`)
	if err != nil {
		return fmt.Errorf("Select.ClientOrders: %v", err)
	}

	dbr.requestsOut["Insert.ClientOrders"], err = db.Prepare(`
		INSERT INTO "ClientOrders" ("ClientHash", "Order_id", "Address_id", "Client_id") 
		VALUES ($1, 0, $2, $3)
	`)
	if err != nil {
		return fmt.Errorf("Insert.ClientOrders: %v", err)
	}

	fmt.Println("End init requests")

	return nil
}

func (dbr *dbRequests) Query(requestName string) (*sql.Rows, error) {
	fmt.Println(requestName)
	_, ok := dbr.requestsIn[requestName]
	if !ok {
		return nil, errors.New("Missmatch request!")
	}

	rows, err := dbr.requestsIn[requestName].Query()
	if err != nil {
		return nil, fmt.Errorf(requestName+" Query %v", err)
	}

	return rows, nil
}

func (v *ClientInfo) Select(rows *sql.Rows) error {
	return rows.Scan(&v.Hash, &v.Phone, &v.Name, &v.Birthday, &v.CreationTime)
}

func (v *ClientOrdersAddress) Select(rows *sql.Rows) error {
	return rows.Scan(&v.ImportID, &v.City, &v.Street, &v.House, &v.Building, &v.Floor, &v.Apartment, &v.Entrance, &v.DoorphoneCode, &v.Phone)
}

func (v *ClientInfo) Insert(rows *sql.Rows, T *Transaction) error {
	var err error
	var Hash interface{}
	var ClientID interface{}

	if err := T.Tx.Stmt(Requests.requestsOut["Select.ClientInfo"]).QueryRow(v.Phone).Scan(&Hash, &ClientID); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.ClientInfo: %v", err)
	}

	if Hash == nil {
		v.Hash, err = hashgenerator.GenerateHash28(v.Name, "ClientInfo")
		if err != nil {
			return fmt.Errorf("GenerateHash28 %v", err)
		}
		fmt.Println(v.Hash)

		_, err := T.Tx.Stmt(Requests.requestsOut["Insert.ClientInfo"]).Exec(&v.Hash, &v.Phone, &v.Name, &v.Birthday, &v.CreationTime)
		if err != nil {
			return fmt.Errorf("Insert.ClientInfo: %v", err)
		}

	}

	return nil
}

func (v *ClientOrdersAddress) Insert(rows *sql.Rows, T *Transaction) error {
	//var err error
	var AddressID interface{}

	if err := T.Tx.Stmt(Requests.requestsOut["Select.ClientOrdersAddress"]).QueryRow(v.ImportID).Scan(&AddressID); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.ClientOrdersAddress: %v", err)
	}

	if AddressID == nil {
		lastID, err := T.Tx.Stmt(Requests.requestsOut["Insert.ClientOrdersAddress"]).Exec(&v.City, &v.Street, &v.House, &v.Building, &v.Floor, &v.Apartment, &v.Entrance, &v.DoorphoneCode, &v.ImportID)
		if err != nil {
			return fmt.Errorf("Insert.ClientOrdersAddress: %v", err)
		}

		AddressID, _ = lastID.LastInsertId()
	}

	//fmt.Println("AddressID ", AddressID)

	var ID interface{}

	if err := T.Tx.Stmt(Requests.requestsOut["Select.ClientOrders"]).QueryRow(AddressID).Scan(&ID); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.ClientOrders: %v", err)
	}

	if ID == nil {
		var Hash interface{}
		var ClientID interface{}
		if err := T.Tx.Stmt(Requests.requestsOut["Select.ClientInfo"]).QueryRow(v.Phone).Scan(&Hash, &ClientID); err != nil && err.Error() != "sql: no rows in result set" {
			return fmt.Errorf("Select.ClientInfo.Hash: %v", err)
		}

		fmt.Println("Hash ", Hash)
		_, err := T.Tx.Stmt(Requests.requestsOut["Insert.ClientOrders"]).Exec(Hash, AddressID, ClientID)
		if err != nil {
			return fmt.Errorf("Insert.ClientOrders: %v", err)
		}

	}

	return nil
}

//Открывает транзакцию
func (T *Transaction) Begin() error {
	var err error
	if T.Tx == nil {
		T.Tx, err = db.Begin()
	}
	if err == nil {
		fmt.Println("Открыл транзакцию")
	}
	return err
}

//Откатывает транзакцию
func (T *Transaction) RollBack() {
	if T.Tx != nil {
		T.Tx.Rollback()
		fmt.Println("Откатил транзакцию")
	}
}

//Закрывает транзакцию
func (T *Transaction) Commit() error {
	err := T.Tx.Commit()
	if err == nil {
		T.Tx = nil
		fmt.Println("Закрыл транзакцию")
	}

	return err
}
