package tls

import (
	"MetricsTest/templates/models"
	"time"
)

type Usr struct {
	Solo  interface{}
	Array []interface{}

	Obj interface{}

	Q models.QueryMessage
}

var Ur Usr

type User struct {
	UserHash     string
	UID          string
	PhoneNumber  string
	INN          string
	Password     string
	VPNNumber    string
	VPNPassword  string
	SurName      string
	FirstName    string
	SecondName   string
	RoleHash     string
	RoleName     string
	OrgHash      string
	OrgName      string
	SkladName    []string
	HourRate     float64
	CountRate    float64
	Lang         string
	CreationTime time.Time
	DelDate      time.Time
	Active       bool
	OneCid       string

	CreationTimeStr string
}

type UserSklad struct {
	UserHash  string
	SkladName string
}
