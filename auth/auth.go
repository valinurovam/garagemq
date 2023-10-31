package auth

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

const (
	// SaslPlain method
	SaslPlain  = "PLAIN"
	authMD5    = "md5"
	authPlain  = "plain"
	authBcrypt = "bcrypt"
)

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
		return SaslData{}, errors.New("unable to parse PLAIN SALS response")
	}

	saslData := SaslData{}
	saslData.Identity = string(parts[0])
	saslData.Username = string(parts[1])
	saslData.Password = string(parts[2])

	return saslData, nil
}

// HashPassword hash raw password and return hash for check
func HashPassword(password string, authType string) (string, error) {
	switch authType {
	case authMD5:
		h := md5.New()
		// digest.Write never return any error, so skip error check
		h.Write([]byte(password))
		return hex.EncodeToString(h.Sum(nil)), nil
	case authBcrypt:
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		return string(hash), err
	case authPlain:
		return password, nil
	default:
		return "", fmt.Errorf("unknown auth type %s", authType)
	}
}

// CheckPasswordHash check given password and hash
func CheckPasswordHash(password, hash string, authType string) bool {
	switch authType {
	case authMD5:
		h := md5.New()
		// digest.Write never return any error, so skip error check
		h.Write([]byte(password))
		return hash == hex.EncodeToString(h.Sum(nil))
	case authBcrypt:
		err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
		return err == nil
	case authPlain:
		return password == hash
	default:
		return false
	}
}
