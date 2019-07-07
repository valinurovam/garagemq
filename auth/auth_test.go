package auth

import "testing"

func TestParsePlain_Success(t *testing.T) {
	data := []byte{'t', 'e', 's', 't', 'i', 0, 't', 'e', 's', 't', 'u', 0, 't', 'e', 's', 't', 'p'}
	sasl, err := ParsePlain(data)
	if err != nil {
		t.Fatal(err)
	}

	if sasl.Identity != "testi" {
		t.Fatalf("identity expected %s, actual %s", "testi", sasl.Identity)
	}

	if sasl.Password != "testp" {
		t.Fatalf("password expected %s, actual %s", "testi", sasl.Password)
	}

	if sasl.Username != "testu" {
		t.Fatalf("username expected %s, actual %s", "testi", sasl.Username)
	}
}

func TestParsePlain_Failed_WrongFormat(t *testing.T) {
	data := []byte{'t', 'e', 's', 't', 'i', 0, 't', 'e', 's', 't', 'u', 't', 'e', 's', 't', 'p'}
	_, err := ParsePlain(data)
	if err == nil {
		t.Fatal("Expected parse error, actual nil")
	}
}


func TestHashPassword_Failed(t *testing.T) {
	password := t.Name()
	_, err := HashPassword(password, t.Name())
	if err == nil {
		t.Fatal("Expected error about auth type")
	}
}


func TestCheckPasswordHash_Bcrypt(t *testing.T) {
	password := t.Name()
	hash, err := HashPassword(password, authBcrypt)
	if err != nil {
		t.Fatal(err)
	}

	if !CheckPasswordHash(password, hash, authBcrypt) {
		t.Fatal("Expected true on check password")
	}

	if CheckPasswordHash("tEsTpAsSwOrD", hash, authBcrypt) {
		t.Fatal("Expected false on check password")
	}
}

func TestCheckPasswordHash_MD5(t *testing.T) {
	password := t.Name()
	hash, err := HashPassword(password, authMD5)
	if err != nil {
		t.Fatal(err)
	}

	if !CheckPasswordHash(password, hash, authMD5) {
		t.Fatal("Expected true on check password")
	}

	if CheckPasswordHash("tEsTpAsSwOrD", hash, authMD5) {
		t.Fatal("Expected false on check password")
	}
}

func TestCheckPasswordHash_Plain(t *testing.T) {
	password := t.Name()
	hash, err := HashPassword(password, authPlain)
	if err != nil {
		t.Fatal(err)
	}

	if !CheckPasswordHash(password, hash, authPlain) {
		t.Fatal("Expected true on check password")
	}

	if CheckPasswordHash("tEsTpAsSwOrD", hash, authMD5) {
		t.Fatal("Expected false on check password")
	}
}

func TestCheckFailed(t *testing.T) {
	password := t.Name()
	hash, err := HashPassword(password, authPlain)
	if err != nil {
		t.Fatal(err)
	}

	if CheckPasswordHash(password, hash, "wrong type") {
		t.Fatal("Expected false on check password with wrong type")
	}

}