package utils

import (
	"bytes"
	"github.com/kr/fernet"
	"encoding/base64"
	"net/http"
	"testing"
)

func TestParseAuthDBToken(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	r.SetBasicAuth("l2met", "token")
	expectedUser, expectedPass, err := ParseAuth(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "l2met" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "token" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}

func TestParseBase64EncodedAuth(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	credentials := []byte("ryan@heroku.com:abc123")
	encodedCredentials := base64.StdEncoding.EncodeToString(credentials)
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(encodedCredentials)))
	expectedUser, expectedPass, err := ParseAuth(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "ryan@heroku.com" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "abc123" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}

func TestParseEncryptedAuth(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}

	libratoUser := "ryan@heroku.com"
	libratoPass := "abc123"
	tok, err := fernet.EncryptAndSign([]byte(libratoUser + ":" + libratoPass), keys[0])
	if err != nil {
		t.Error(err)
	}
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString(tok))
	expectedUser, expectedPass, err := ParseAuth(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "ryan@heroku.com" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "abc123" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}

func TestParseAuthNoPassword(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	_, _, err = ParseAuth(r)
	if err == nil {
		t.Errorf("Expected ParseAuth to return error when with no creds.")
	}
}