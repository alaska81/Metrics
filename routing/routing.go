package routing

import (
	"MetricsTest/config"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/contrib/sessions"
	"github.com/gin-gonic/gin"
)

var r *gin.Engine
var word sessions.CookieStore

func Gin_Start() {
	log.SetFlags(log.LstdFlags)
	fmt.Println("Инициализация gin")
	r = gin.Default()
	word = sessions.NewCookieStore([]byte(config.Config.Word))
	word.Options(sessions.Options{
		Domain: config.Config.Using_Domain,
	})
	r.Use(sessions.Sessions("mysession", word))
	r.LoadHTMLGlob("./../templates/*.tmpl")
	r.Static("/assets", "./../assets")
	fmt.Println("Инициализация маршрутов")
	InitHandle(r)
	fmt.Println("Инициализация успешна")
	fmt.Println("Запуск интерфейса метрики", time.Now())
	r.Run(config.Config.Port_Gin)
}
