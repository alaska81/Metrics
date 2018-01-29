package routing

import (
	db "MetricsTest/postgresql"
	//"MetricsTest/structures"
	//	ConfigServis "MetricsTest/config"
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"

	UserTls "MetricsTest/templates/user/tls"
)

func InitHandle(R *gin.Engine) {

	//var DB db.Common

	//if err := DB.Select_Common("Select", "metrics", ""); err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	R.GET("/", func(c *gin.Context) {
		c.HTML(200, "index", gin.H{})
	})

	//Type_And_Mod := R.Group("/Type_And_Mod")
	Common := R.Group("/Common")
	Common.GET("/Config", configHandle)
	Common.POST("/Select", selectHandle)
	Common.POST("/Action", actionHandle)

}

func Check(c *gin.Context, err error) error {
	if err != nil {
		fmt.Println("\nCHECK::->", err.Error(), "\n")
		//db.Loging.Println(err.Error())
		c.JSON(200, gin.H{
			"Error": err.Error(),
		})
		return err
	}
	return nil
}

func configHandle(c *gin.Context) {
	Gin := gin.H{}
	c.Request.ParseForm()
	params := c.Request.Form
	for i, v := range params {
		switch i {
		case "Tables[]":
			for _, f := range v {
				fmt.Println(f)
				switch f {
				//	var DB db.Common
				//	if err := DB.Select_Common("Select", f, ""); err != nil {
				//		fmt.Println(err.Error())
				//		return
				//	}

				//U := tls.Nomenc{}
				//if err := U.SelectAll(f, "Read", 999999, 0); Check(c, err) != nil {
				//	return
				//}
				//Gin[f] = U.Array
				case "Users":
					U := UserTls.Usr{}
					if err := U.SelectAll("UserGlobal", "Select", 999999, 0); Check(c, err) != nil {
						return
					}
					Gin[f] = U.Array

				default:
					return
				}
			}
		}
	}
	Gin["Error"] = nil
	c.JSON(200, Gin)
}

func selectHandle(c *gin.Context) {
	Gin := gin.H{}
	c.Request.ParseForm()
	params := c.Request.Form
	fmt.Println("\n params1:", params)
	for i, v := range params {
		switch i {
		case "Tables[]":
			for _, f := range v {
				var DB db.Common
				if err := json.Unmarshal([]byte(f), &DB.JS_Select); Check(c, err) != nil {
					fmt.Println("ERROR:", err.Error())
					return
				}

				if DB.JS_Select.Table == "CancellationOrder" {

					U := UserTls.Common{}
					//Select.CancellationOrder.CanceledOrders

					if err := U.Select("192.168.0.132:666", "Select", DB.JS_Select.Table, DB.JS_Select.TypeParameter, DB.JS_Select.Values...); Check(c, err) != nil {
						fmt.Sprintln(2222)
						return
					}
					Gin[DB.JS_Select.Table] = U.Array
				} else {

					if err := DB.Select_Common("Select", DB.JS_Select.Table, DB.JS_Select.TypeParameter, DB.JS_Select.Values...); Check(c, err) != nil {
						return
					}
					key := DB.JS_Select.Table
					if DB.JS_Select.TypeParameter != "" {
						key += "." + DB.JS_Select.TypeParameter
					}
					if DB.JS_Select.TypeParameter == "ParametersByInterval" {
						if DB.JS_Select.ParameterQuery != -1 {
							key += "." + fmt.Sprint(DB.JS_Select.ParameterQuery)
						}
					}
					Gin[key] = DB.Data
				}
			}
		}
	}

	Gin["Error"] = nil
	c.JSON(200, Gin)
}

func actionHandle(c *gin.Context) {
	Gin := gin.H{}
	c.Request.ParseForm()
	params := c.Request.Form
	fmt.Println("params2:", params)
	for i, v := range params {
		switch i {
		case "Tables[]":
			for _, f := range v {
				var DB db.Common
				if err := json.Unmarshal([]byte(f), &DB.JS_Select); Check(c, err) != nil {
					return
				}
				if err := DB.Action(DB.JS_Select.Query, DB.JS_Select.Table, DB.JS_Select.TypeParameter, DB.JS_Select.Values...); Check(c, err) != nil {
					return
				}
				Gin[DB.JS_Select.Table] = "Действие выполено успешно"
			}
		}
	}
	Gin["Error"] = nil
	c.JSON(200, Gin)
}
