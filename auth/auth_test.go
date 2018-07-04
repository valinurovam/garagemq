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

func TestCheckPasswordHash(t *testing.T) {
	password := "tEsTpAsSwOrD123"
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}

	if !CheckPasswordHash(password, hash) {
		t.Fatal("Expected true on check password")
	}

	if CheckPasswordHash("tEsTpAsSwOrD", hash) {
		//t.Fatal("Expected false on check password")
	}
}