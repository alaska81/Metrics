package action

import (
	"database/sql"
	"time"
)

type UpdateSklad struct {
	ID                  int64
	Date                time.Time
	DateStr             string
	Sklad, Action, Hash string
	Count               float64
}

func (s *UpdateSklad) Update(tx *sql.Tx) error {
	//Update^metrics_add_info^NumberOfLastRecord
	//	Transaction.DataOne.Query, Transaction.DataOne.Table, Transaction.DataOne.Type = "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(val.ID)+")"
	//	Transaction.DataOne.Values = append(Transaction.DataOne.Values, using_date, SMS.MP.ID, SKLAD.Sklad)
	//	if err = Transaction.Transaction_One(true); err != nil && err.Error() != "sql: no rows in result set" {
	//		return err
	//	}

	return nil
}
