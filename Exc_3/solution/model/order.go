package model

import "time"

type Order struct {
	DrinkID   uint64    `json:"drink_id"`
	CreatedAt time.Time `json:"created_at"`
	Amount    uint64    `json:"amount"`
}
