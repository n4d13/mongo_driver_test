package repositories

type Store struct {
	Id        string
	StoreId   string `bson:"store_id"`
	Name      string
	HugeValue string
}
