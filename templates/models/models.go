package models

type QueryMessage struct {
	Table string
	Query string

	TypeParameter string
	Values        []interface{}
	Limit         int64
	Offset        int64
}
