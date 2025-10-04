package sales

type ProductTransaction struct {
	CustomerName string
	ProductName  string
	Quantity     int
	Price        float64
}

type SalesDataSource struct {
	numberRecords int
}

func NewSalesDataSource(numberRecords int) *SalesDataSource {
	return &SalesDataSource{numberRecords: numberRecords}
}

func NewSalesDataSourceDefault() *SalesDataSource {
	return &SalesDataSource{numberRecords: 10}
}

func (s *SalesDataSource) Fetch() []ProductTransaction {
	return generateProductTransactions(s.numberRecords)
}

func generateProductTransactions(count int) []ProductTransaction {
	transactions := make([]ProductTransaction, count)
	for i := 0; i < count; i++ {
		transactions[i] = ProductTransaction{
			CustomerName: "Customer",
			ProductName:  "Product",
			Quantity:     1,
			Price:        10.0,
		}
	}
	return transactions
}
