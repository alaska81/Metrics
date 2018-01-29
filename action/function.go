package action

import (
	"errors"
	"fmt"
	"math"
	"time"

	fn "MetricsTest/function"
	"MetricsTest/postgresql"
)

//Проверка - делать ли ещё 1 итерацию по шагам.
func NextStep(SMS *postgresql.SMS) bool {
	if SMS.MSD.End_date.Before(SMS.MSD.StartDate) {
		switch SMS.MP.Min_Step_ID {
		case 1: //минуты
			return (SMS.MSD.StartDate.YearDay() != SMS.MSD.End_date.YearDay() ||
				SMS.MSD.StartDate.Hour() != SMS.MSD.End_date.Hour() ||
				math.Abs(float64(SMS.MSD.StartDate.Minute()-SMS.MSD.End_date.Minute())) > 5)
		case 2: //часы
			return (SMS.MSD.StartDate.YearDay() != SMS.MSD.End_date.YearDay() ||
				SMS.MSD.StartDate.Hour() != SMS.MSD.End_date.Hour()) //&& math.Abs(float64(SMS.MSD.StartDate.Hour()-SMS.MSD.End_date.Hour())) >= 1
		case 3: //дни
			return math.Abs(float64((SMS.MSD.StartDate.YearDay() - SMS.MSD.End_date.YearDay()))) > 1
		case 4: //месяцы
			return (SMS.MSD.StartDate.Year() != SMS.MSD.End_date.Year() || SMS.MSD.StartDate.Month() != SMS.MSD.End_date.Month())
		case 5: //годы
			return (SMS.MSD.StartDate.Year() != SMS.MSD.End_date.Year())
		default:
			panic(errors.New("(NextStep) Шаг не поддерживается:" + fmt.Sprint(SMS.MP.Min_Step_ID)))
			return false
		}
	} else {
		return false
	}
}

//Следующая дата для текущего шага
func SetTimeStep(SMS *postgresql.SMS, RETURNING bool) (string, []interface{}, error) {
	var using_date string
	var Values []interface{}
	var Sign time.Duration = 1 //Знак
	switch SMS.MP.Min_Step_ID {
	case 1:
		if SMS.MSD.StartDate.YearDay() != SMS.MSD.End_date.YearDay() ||
			SMS.MSD.StartDate.Hour() != SMS.MSD.End_date.Hour() ||
			math.Abs(float64(SMS.MSD.StartDate.Minute()-SMS.MSD.End_date.Minute())) >= 5 {
			if RETURNING {
				using_date = fn.FormatDate(SMS.MSD.End_date.Add(time.Minute * 5 * Sign))
				Values = append(Values, fn.FormatDate(SMS.MSD.End_date), using_date) //Добавляем 5 минут
			} else {
				SMS.MSD.End_date = SMS.MSD.End_date.Add(time.Minute * 5 * Sign)
			}
		} else {
			return "", Values, errors.New("Достигнута максимальная дата:" + fmt.Sprint(SMS.MP.Min_Step_ID))
		}
	case 2:
		if SMS.MSD.StartDate.YearDay() != SMS.MSD.End_date.YearDay() ||
			math.Abs(float64(SMS.MSD.StartDate.Hour()-SMS.MSD.End_date.Hour())) >= 1 {
			if RETURNING {
				using_date = fn.FormatDate(SMS.MSD.End_date.Add(time.Hour * Sign))
				Values = append(Values, fn.FormatDate(SMS.MSD.End_date), using_date) //Добавляем час
			} else {
				SMS.MSD.End_date = SMS.MSD.End_date.Add(time.Hour * Sign)
			}
		} else {
			return "", Values, errors.New("Достигнута максимальная дата:" + fmt.Sprint(SMS.MP.Min_Step_ID))
		}
	case 3, 4, 5:
		if math.Abs(float64(SMS.MSD.StartDate.YearDay()-SMS.MSD.End_date.YearDay())) >= 1 {
			if RETURNING {
				using_date = fn.FormatDate(SMS.MSD.End_date.AddDate(0, 0, 1*int(Sign)))
				Values = append(Values, using_date) //Добавляем день
			} else {
				SMS.MSD.End_date = SMS.MSD.End_date.AddDate(0, 0, 1*int(Sign))
			}
		} else {
			return "", Values, errors.New("Достигнута максимальная дата:" + fmt.Sprint(SMS.MP.Min_Step_ID))
		}

	default:
		return "", Values, errors.New("Шаг не поддерживается:" + fmt.Sprint(SMS.MP.Min_Step_ID))
	}
	return using_date, Values, nil
}
