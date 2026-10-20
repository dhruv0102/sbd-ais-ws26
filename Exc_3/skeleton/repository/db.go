package repository

import (
	"ordersystem/model"
	"time"
)

type DatabaseHandler struct {
	// Available drinks
	drinks []model.Drink

	// Order history
	orders []model.Order
}

func NewDatabaseHandler() *DatabaseHandler {
	// Initialize the menu with example data
	drinks := []model.Drink{
		{
			ID:          1,
			Name:        "Coffee",
			Price:       2.50,
			Description: "Freshly brewed coffee",
		},
		{
			ID:          2,
			Name:        "Tea",
			Price:       2.00,
			Description: "Hot black tea",
		},
		{
			ID:          3,
			Name:        "Water",
			Price:       1.50,
			Description: "Still mineral water",
		},
	}

	// Initialize some example orders
	orders := []model.Order{
		{
			DrinkID:   1,
			CreatedAt: time.Now(),
			Amount:    2,
		},
		{
			DrinkID:   2,
			CreatedAt: time.Now(),
			Amount:    1,
		},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	return db.orders
}

func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	totalledOrders := make(map[uint64]uint64)

	for _, order := range db.orders {
		totalledOrders[order.DrinkID] += order.Amount
	}

	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	db.orders = append(db.orders, *order)
}
