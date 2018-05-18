package auth

import (
	"bytes"
	"errors"
	"golang.org/x/crypto/bcrypt"
)

const SaslPlain = "PLAIN"

type SaslData struct {
	Identity string
	Username string
	Password string
}

func ParsePlain(response []byte) (SaslData, error) {
	parts := bytes.Split(response, []byte{0})
	if len(parts) != 3 {
		return SaslData{}, errors.New("Unable to parse PLAIN SALS response")
	}

	saslData := SaslData{}
	saslData.Identity = string(parts[0])
	saslData.Username = string(parts[1])
	saslData.Password = string(parts[2])

	return saslData, nil
}

func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(hash), err
}

func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

