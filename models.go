package mirage

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type DidHandle struct {
	Id        uint      `gorm:"primaryKey"`
	Did       string    `gorm:"uniqueIndex;index:idx_did_handle_did_created_at"`
	Handle    string    `gorm:"index;index:idx_did_handle_handle_created_at"`
	UpdatedAt time.Time `gorm:"index;index:idx_did_handle_did_created_at,sort:desc;index:idx_did_handle_handle_created_at,sort:desc"`
}

type PlcEntry struct {
	ID        uint             `json:"-" gorm:"primaryKey"`
	Did       string           `json:"did" gorm:"index;index:idx_plc_entry_did_cid;index:idx_plc_entry_did_created_at"`
	Operation PlcOperationType `json:"operation" gorm:"type:jsonb"`
	Cid       string           `json:"cid" gorm:"uniqueIndex;index:idx_plc_entry_did_cid"`
	Nullified bool             `json:"nullified"`
	CreatedAt string           `json:"createdAt" gorm:"index;index:idx_plc_entry_did_created_at"`
}

type PlcOperation struct {
	Sig                 string                `json:"sig"`
	Prev                *string               `json:"prev"`
	Type                string                `json:"type"`
	Services            map[string]PlcService `json:"services"`
	AlsoKnownAs         []string              `json:"alsoKnownAs"`
	RotationKeys        []string              `json:"rotationKeys"`
	VerificationMethods map[string]string     `json:"verificationMethods"`
}

type PlcTombstone struct {
	Sig  string `json:"sig"`
	Prev string `json:"prev"`
	Type string `json:"type"`
}

type PlcService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

type LegacyPlcOperation struct {
	Sig         string `json:"sig"`
	Prev        string `json:"prev"`
	Type        string `json:"type"`
	Handle      string `json:"handle"`
	Service     string `json:"service"`
	SigningKey  string `json:"signingKey"`
	RecoveryKey string `json:"recoveryKey"`
}

type PlcOperationType struct {
	PlcOperation       *PlcOperation
	PlcTombstone       *PlcTombstone
	LegacyPlcOperation *LegacyPlcOperation
}

func (o *PlcOperationType) UnmarshalJSON(data []byte) error {
	type Base struct {
		PlcOperation       *PlcOperation
		PlcTombstone       *PlcTombstone
		LegacyPlcOperation *LegacyPlcOperation
		Type               string `json:"type"`
	}

	var base Base
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	switch base.Type {
	case "plc_operation":
		var op PlcOperation
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.PlcOperation = &op
	case "plc_tombstone":
		var op PlcTombstone
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.PlcTombstone = &op
	case "create":
		var op LegacyPlcOperation
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.LegacyPlcOperation = &op
	default:
		if base.PlcOperation != nil || base.PlcTombstone != nil || base.LegacyPlcOperation != nil {
			o.PlcOperation = base.PlcOperation
			o.PlcTombstone = base.PlcTombstone
			o.LegacyPlcOperation = base.LegacyPlcOperation
		} else {
			return fmt.Errorf("invalid operation type %s", base.Type)
		}
	}

	return nil
}

func (o *PlcOperationType) Value() (interface{}, error) {
	return json.Marshal(o)
}

func (o *PlcOperationType) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("could not scan PlcOperationType")
	}
	return json.Unmarshal(bytes, o)
}

type DocVerificationMethod struct {
	Id                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase"`
}

type DocService struct {
	Id              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

type ResolveDidResponse struct {
	Context            []string                `json:"@context"`
	Id                 string                  `json:"id"`
	AlsoKnownAs        []string                `json:"alsoKnownAs"`
	VerificationMethod []DocVerificationMethod `json:"verificationMethod"`
	Service            []DocService            `json:"service"`
}

type DataResponse struct {
	Did                 string                `json:"did"`
	VerificationMethods map[string]string     `json:"verificationMethods"`
	RotationKeys        []string              `json:"rotationKeys"`
	AlsoKnownAs         []string              `json:"alsoKnownAs"`
	Services            map[string]PlcService `json:"services"`
}
