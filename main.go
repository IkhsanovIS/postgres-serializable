package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "tx-demo"
)

type Transfer struct {
	from, to, amount int
}

var ctx context.Context
var counter atomic.Uint32

func main() {
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatal("config parse error: ", config)
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(context.Background(), "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		return err
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatal("config parse error: ", config)
	}

	createTableUsers := `DROP TABLE IF EXISTS users;
	
		CREATE TABLE users
			(
				id integer,
				balance integer,
				PRIMARY KEY (id)
			);`

	_, err = pool.Exec(context.Background(), createTableUsers)
	if err != nil {
		log.Fatal("error: ", err)
	}

	insertUser := `INSERT INTO users (id, balance)
					VALUES ($1, $2)`
	for id := 1; id <= 1000; id++ {
		_, err = pool.Exec(context.Background(), insertUser, id, 100)
		if err != nil {
			log.Fatal("err: ", err)
		}
	}

	txs := make([]Transfer, 10000)
	for i := 0; i < 10000; i++ {
		fromId := rand.Intn(1000)

		txs[i] = Transfer{
			from:   fromId,
			to:     genRandIdExpect(fromId),
			amount: 10,
		}
	}

	txC := make(chan Transfer, 0)
	go func() {
		for _, tx := range txs {
			txC <- tx
		}
	}()

	updateBalanceUserSum := `update users
						set balance=balance+$2
						where id=$1`

	updateBalanceUserSub := `update users
						set balance=balance-$2
						where id=$1`

	for i := 0; i < 4; i++ {

		go func() {
			for {
				tx := <-txC

				transaction, err := pool.Begin(context.Background())
				if err != nil {
					log.Fatal("err: ", err)
				}
				transaction.Exec(context.Background(), updateBalanceUserSub, tx.from, tx.amount)
				transaction.Exec(context.Background(), updateBalanceUserSum, tx.to, tx.amount)

				err = transaction.Commit(context.Background())
				if err != nil {
					log.Println("error: ", err)
					txC <- tx
				}
				counter.Add(1)

			}
		}()
	}
}

func genRandIdExpect(n int) int {
	rndNum := rand.Intn(1000)
	if rndNum == n {
		return genRandIdExpect(n)
	}
	return rndNum
}
