package auth

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

// SaslPlain method
const SaslPlain = "PLAIN"

// SaslData represents standard SASL properties
type SaslData struct {
	Identity string
	Username string
	Password string
}

// ParsePlain check and parse SASL-raw data and return SaslData structure
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

// HashPassword hash raw password and return hash for check
func HashPassword(password string, isMd5 bool) (string, error) {
	if isMd5 {
		h := md5.New()
		h.Write([]byte(password))
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(hash), err
}

// CheckPasswordHash check given password and hash
func CheckPasswordHash(password, hash string, isMd5 bool) bool {
	if isMd5 {
		h := md5.New()
		h.Write([]byte(password))

		return hash == hex.EncodeToString(h.Sum(nil))
	}
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}
