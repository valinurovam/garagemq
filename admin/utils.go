package admin

import (
	"encoding/json"
	"net/http"
)

func JSONResponse(w http.ResponseWriter, data interface{}, code int) (int, error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)

	body, err := json.Marshal(data)
	if err != nil {
		body = []byte(
			`{"error": "Marshal error"}`)
	}

	return w.Write(body)
}
