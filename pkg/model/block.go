package model

type Transaction struct {
	Hash  string `josn:"hash"`
	From  string `josn:"from"`
	To    string `josn:"to"`
	Value string `josn:"value"`
}

type Block struct {
	Number       string        `json:"number"`
	Hash         string        `json:"hash"`
	ParentHash   string        `json:"parentHash"`
	Timestamp    string        `json:"timestamp"`
	Transactions []Transaction `json:"transactions"`
}
