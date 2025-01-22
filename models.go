package main

import (
	"encoding/json"
	"errors"
)

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
	Prev                string                `json:"prev"`
	Type                string                `json:"type"`
	Services            map[string]PlcService `json:"services"`
	AlsoKnownAs         []string              `json:"alsoKnownAs"`
	RotationKeys        []string              `json:"rotationKeys"`
	VerificationMethods map[string]string     `json:"verificationMethods"`
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
	LegacyPlcOperation *LegacyPlcOperation
}

func (o *PlcOperationType) UnmarshalJSON(data []byte) error {
	var op PlcOperation
	err := json.Unmarshal(data, &op)
	if err == nil && op.AlsoKnownAs != nil {
		o.PlcOperation = &op
		return nil
	}
	var lop LegacyPlcOperation
	err = json.Unmarshal(data, &lop)
	if err == nil {
		o.LegacyPlcOperation = &lop
		return nil
	}
	return errors.New("could not unmarshal PlcOperationTypes")
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
