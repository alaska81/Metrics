package hashgenerator

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"time"
)

func GenerateRandomString(s int) (string, error) {
	b, err := GenerateRandomBytes(s)
	return base64.URLEncoding.EncodeToString(b), err
}

func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func GenerateHash28(Value, solt string) (string, error) {

	str, err := GenerateRandomString(16)

	if err != nil {
		return "", err
	}

	//bytes28 = sha512.Sum512_224([]byte(time.Now().String() + str + Value + solt))
	hash := sha256.New()
	hash.Write([]byte(time.Now().String() + str + Value + solt))

	return hex.EncodeToString(hash.Sum(nil)), nil

}
