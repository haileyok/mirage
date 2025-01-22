package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/crypto"
	"github.com/mr-tron/base58"
)

func extractMultikey(key string) (*string, error) {
	if !strings.HasPrefix(key, DID_KEY_PREFIX) {
		return nil, fmt.Errorf("key is not a did:key")
	}

	fmtd := strings.TrimPrefix(key, DID_KEY_PREFIX)
	return &fmtd, nil
}

func extractPrefixedBytes(multikey string) ([]byte, error) {
	if !strings.HasPrefix(multikey, BASE58_MULTIBASE_PREFIX) {
		println(multikey)
		return nil, fmt.Errorf("multikey is not prefixed correctly")
	}

	encoded := strings.TrimPrefix(multikey, BASE58_MULTIBASE_PREFIX)

	decoded, err := base58.Decode(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode multikey: %w", err)
	}

	return decoded, nil
}

func hasPrefix(bs, prefix []byte) bool {
	if len(bs) < len(prefix) {
		return false
	}

	return bytes.Equal(bs[:len(prefix)], prefix)
}

type parsedMultikey struct {
	jwtAlg   string
	keyBytes []byte
}

func parseMultikey(key string) (*parsedMultikey, error) {
	multikey, err := extractMultikey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to extract multikey: %w", err)
	}

	decoded, err := extractPrefixedBytes(*multikey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract prefixed bytes: %w", err)
	}

	if hasPrefix(decoded, P256_DID_PREFIX) {
		k, err := crypto.ParsePublicBytesP256(decoded[2:])
		if err != nil {
			return nil, fmt.Errorf("failed to parse P256 key: %w", err)
		}

		return &parsedMultikey{
			jwtAlg:   P256_JWT_ALG,
			keyBytes: k.Bytes(),
		}, nil
	} else if hasPrefix(decoded, SECP256K1_DID_PREFIX) {
		k, err := crypto.ParsePublicBytesK256(decoded[2:])
		if err != nil {
			println(string(decoded))
			return nil, fmt.Errorf("failed to parse SECP256K1 key: %w", err)
		}

		return &parsedMultikey{
			jwtAlg:   SECP256K1_JWT_ALG,
			keyBytes: k.Bytes(),
		}, nil
	} else {
		return nil, fmt.Errorf("unsupported key type")
	}
}

type keyAndContext struct {
	Context            string
	Type               string
	PublicKeyMultibase string
}

func formatKeyAndContext(key string) (*keyAndContext, error) {
	parsed, err := parseMultikey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multikey: %w", err)
	}

	var context string
	if parsed.jwtAlg == P256_JWT_ALG {
		context = "https://w3id.org/security/suites/ecdsa-2019/v1"
	} else if parsed.jwtAlg == SECP256K1_JWT_ALG {
		context = "https://w3id.org/security/suites/secp256k1-2019/v1"
	} else {
		return nil, fmt.Errorf("unsupported jwt alg")
	}

	return &keyAndContext{
		Context:            context,
		Type:               "Multikey",
		PublicKeyMultibase: strings.TrimPrefix(key, DID_KEY_PREFIX),
	}, nil
}
