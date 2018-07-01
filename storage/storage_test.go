package storage

import (
	"testing"
)

var data = "— Coca-Cola опубликовала заявление об утечке данных, произошедшей в одном из филиалов в сентябре 2017 года. Расследование установило, что один из сотрудников незаконно скопировал данные 8 000 коллег на личный жесткий диск перед уходом из компании: https://goo.gl/yBoFBq"

func setByte(data string) []byte {
	var key []byte
	key = []byte(data)
	return key
}

func copyByte(data string) []byte {
	var key = make([]byte, len(data))
	copy(key, data)
	return key
}

func BenchmarkSet(b *testing.B) {
	for n := 0; n < b.N; n++ {
		setByte(data)
	}

}

func BenchmarkCopy(b *testing.B) {
	for n := 0; n < b.N; n++ {
		copyByte(data)
	}
}

//var s = New("db_test")

//
//func BenchmarkStorage_Set(b *testing.B) {
//	for n := 0; n < b.N; n++ {
//		key := strconv.Itoa(n)
//		s.Set(key, []byte(data))
//	}
//}
//
//func BenchmarkStorage_Get(b *testing.B) {
//	for n := 0; n < b.N; n++ {
//		key := strconv.Itoa(n)
//		s.Get(key)
//	}
//}
//
//var db, err = buntdb.Open("data_test.db")
//
//func BenchmarkDB_Set(b *testing.B) {
//	for n := 0; n < b.N; n++ {
//		key := strconv.Itoa(n)
//		db.Update(func(tx *buntdb.Tx) error {
//			tx.Set(key, data, nil)
//			return nil
//		})
//	}
//}
//
//func BenchmarkDB_Get(b *testing.B) {
//	for n := 0; n < b.N; n++ {
//		key := strconv.Itoa(n)
//		db.View(func(tx *buntdb.Tx) error {
//			tx.Get(key)
//			return nil
//		})
//	}
//}
