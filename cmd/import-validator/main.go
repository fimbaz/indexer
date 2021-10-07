package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"

	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand/agreement"
	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/rpcs"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/algorand/indexer/fetcher"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres"
	"github.com/algorand/indexer/util"
)

type blockHandler struct {
	f func(*rpcs.EncodedBlockCert) error
}

func (h blockHandler) HandleBlock(block *rpcs.EncodedBlockCert) {
	err := h.f(block)
	if err != nil {
		fmt.Printf("error handling block %d err: %v\n", block.Block.Round(), err)
		os.Exit(1)
	}
}

func getGenesisBlock(client *algod.Client) (bookkeeping.Block, error) {
	data, err := client.BlockRaw(0).Do(context.Background())
	if err != nil {
		return bookkeeping.Block{}, fmt.Errorf("getGenesisBlock() client err: %w", err)
	}

	var block rpcs.EncodedBlockCert
	err = protocol.Decode(data, &block)
	if err != nil {
		return bookkeeping.Block{}, fmt.Errorf("getGenesisBlock() decode err: %w", err)
	}

	return block.Block, nil
}

func getGenesis(client *algod.Client) (bookkeeping.Genesis, error) {
	data, err := client.GetGenesis().Do(context.Background())
	if err != nil {
		return bookkeeping.Genesis{}, fmt.Errorf("getGenesis() client err: %w", err)
	}

	var res bookkeeping.Genesis
	err = protocol.DecodeJSON([]byte(data), &res)
	if err != nil {
		return bookkeeping.Genesis{}, fmt.Errorf("getGenesis() decode err: %w", err)
	}

	return res, nil
}

func openIndexerDb(postgresConnStr string, genesis *bookkeeping.Genesis, genesisBlock *bookkeeping.Block, logger *logrus.Logger) (*postgres.IndexerDb, error) {
	db, availableCh, err :=
		postgres.OpenPostgres(postgresConnStr, idb.IndexerDbOptions{}, logger)
	if err != nil {
		return nil, fmt.Errorf("openIndexerDb() err: %w", err)
	}
	<-availableCh

	_, err = db.GetNextRoundToAccount()
	if err != idb.ErrorNotInitialized {
		if err != nil {
			return nil, fmt.Errorf("openIndexerDb() err: %w", err)
		}
	} else {
		err = db.LoadGenesis(*genesis)
		if err != nil {
			return nil, fmt.Errorf("openIndexerDb() err: %w", err)
		}
	}

	nextRound, err := db.GetNextRoundToAccount()
	if err != nil {
		return nil, fmt.Errorf("openIndexerDb() err: %w", err)
	}

	if nextRound == 0 {
		err = db.AddBlock(genesisBlock)
		if err != nil {
			return nil, fmt.Errorf("openIndexerDb() err: %w", err)
		}
	}

	return db, nil
}

func openLedger(ledgerPath string, genesis *bookkeeping.Genesis, genesisBlock *bookkeeping.Block) (*ledger.Ledger, error) {
	logger := logging.NewLogger()

	accounts := make(map[basics.Address]basics.AccountData)
	for _, alloc := range genesis.Allocation {
		address, err := basics.UnmarshalChecksumAddress(alloc.Address)
		if err != nil {
			return nil, fmt.Errorf("openLedger() decode address err: %w", err)
		}
		accounts[address] = alloc.State
	}

	initState := ledger.InitState{
		Block: *genesisBlock,
		Accounts: accounts,
		GenesisHash: genesisBlock.GenesisHash(),
	}

	ledger, err := ledger.OpenLedger(
		logger, path.Join(ledgerPath, "ledger"), false, initState, config.GetDefaultLocal())
	if err != nil {
		return nil, fmt.Errorf("openLedger() open err: %w", err)
	}

	return ledger, nil
}

func getModifiedAccounts(l *ledger.Ledger, block *bookkeeping.Block) ([]basics.Address, error) {
	eval, err := l.StartEvaluator(block.BlockHeader, len(block.Payset))
	if err != nil {
		return nil, fmt.Errorf("changedAccounts() start evaluator err: %w", err)
	}

	paysetgroups, err := block.DecodePaysetGroups()
	if err != nil {
		return nil, fmt.Errorf("changedAccounts() decode payset groups err: %w", err)
	}

	for _, group := range paysetgroups {
		err = eval.TransactionGroup(group)
		if err != nil {
			return nil, fmt.Errorf("changedAccounts() apply transaction group err: %w", err)
		}
	}

	vb, err := eval.GenerateBlock()
	if err != nil {
		return nil, fmt.Errorf("changedAccounts() generate block err: %w", err)
	}

	return vb.Delta().Accts.ModifiedAccounts(), nil
}

func checkModifiedAccounts(db *postgres.IndexerDb, l *ledger.Ledger, block *bookkeeping.Block, addresses []basics.Address) error {
	specialAddresses := transactions.SpecialAddresses{
		FeeSink: block.FeeSink,
		RewardsPool: block.RewardsPool,
	}

	var accountsIndexer map[basics.Address]basics.AccountData
	var err0 error
	var accountsAlgod map[basics.Address]basics.AccountData
	var err1 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		accountsIndexer, err0 = db.GetAccountData(
			block.GenesisHash(), specialAddresses, addresses)
		if err0 != nil {
			err0 = fmt.Errorf("checkModifiedAccounts() err0: %w", err0)
			return
		}

		// Delete special addresses. TODO: delete.
		delete(accountsIndexer, specialAddresses.FeeSink)
		delete(accountsIndexer, specialAddresses.RewardsPool)
	}()

	go func() {
		defer wg.Done()

		accountsAlgod = make(map[basics.Address]basics.AccountData, len(addresses))
		for _, address := range addresses {
			// Exclude special addresses. TODO: delete.
			if (address != specialAddresses.FeeSink) &&
				(address != specialAddresses.RewardsPool) {
				accountsAlgod[address], _, err1 = l.LookupWithoutRewards(block.Round(), address)
				if err1 != nil {
					err1 = fmt.Errorf("checkModifiedAccounts() lookup err1: %w", err1)
					return
				}
			}
		}
	}()

	wg.Wait()
	if err0 != nil {
		return err0
	}
	if err1 != nil {
		return err1
	}

	if !reflect.DeepEqual(accountsIndexer, accountsAlgod) {
		diff := util.Diff(accountsAlgod, accountsIndexer)
		return fmt.Errorf(
			"checkModifiedAccounts() accounts differ," +
			"\naccountsIndexer: %+v,\naccountsAlgod: %+v,\ndiff: %s",
			accountsIndexer, accountsAlgod, diff)
	}

	return nil
}

func catchup(db *postgres.IndexerDb, l *ledger.Ledger, bot fetcher.Fetcher, logger *logrus.Logger) error {
	nextRoundIndexer, err := db.GetNextRoundToAccount()
	if err != nil {
		return fmt.Errorf("catchup err: %w", err)
	}
	nextRoundLedger := uint64(l.Latest()) + 1

	if nextRoundLedger > nextRoundIndexer {
		return fmt.Errorf(
			"catchup() ledger is ahead of indexer nextRoundIndexer: %d nextRoundLedger: %d",
			nextRoundIndexer, nextRoundLedger)
	}

	if nextRoundIndexer > nextRoundLedger + 1 {
		return fmt.Errorf(
			"catchup() indexer is too ahead of ledger " +
			"nextRoundIndexer: %d nextRoundLedger: %d",
			nextRoundIndexer, nextRoundLedger)
	}

	blockHandlerFunc := func(block *rpcs.EncodedBlockCert) error {
		if nextRoundLedger < nextRoundIndexer {
			err := l.AddBlock(block.Block, block.Certificate)
			nextRoundLedger++
			return fmt.Errorf("catchup() err: %w", err)
		}

		var modifiedAccounts []basics.Address
		var err0 error
		var err1 error
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			modifiedAccounts, err0 = getModifiedAccounts(l, &block.Block)
			wg.Done()
		}()

		go func() {
			err1 = db.AddBlock(&block.Block)
			wg.Done()
		}()

		wg.Wait()
		if err0 != nil {
			return fmt.Errorf("catchup() err0: %w", err0)
		}
		if err1 != nil {
			return fmt.Errorf("catchup() err1: %w", err1)
		}

		err0 = l.AddBlock(block.Block, agreement.Certificate{})
		if err0 != nil {
			return fmt.Errorf("catchup() err0: %w", err0)
		}

		return checkModifiedAccounts(db, l, &block.Block, modifiedAccounts)
	}
	bot.AddBlockHandler(blockHandler{f: blockHandlerFunc})
	bot.SetNextRound(nextRoundLedger)
	bot.Run()

	return nil
}

func main() {
	var algodAddr string
	var algodToken string
	var algodLedger string
	var postgresConnStr string

	var rootCmd = &cobra.Command{
		Use:   "import-validator",
		Short: "Import validator",
		Run: func(cmd *cobra.Command, args []string) {
			logger := logrus.New()

			bot, err := fetcher.ForNetAndToken(algodAddr, algodToken, logger)
			if err != nil {
				fmt.Printf("error initializing fetcher err: %v", err)
				os.Exit(1)
			}

			genesis, err := getGenesis(bot.Algod())
			if err != nil {
				fmt.Printf("error getting genesis err: %v", err)
				os.Exit(1)
			}
			genesisBlock, err := getGenesisBlock(bot.Algod())
			if err != nil {
				fmt.Printf("error getting genesis block err: %v", err)
				os.Exit(1)
			}

			db, err := openIndexerDb(postgresConnStr, &genesis, &genesisBlock, logger)
			if err != nil {
				fmt.Printf("error opening indexer database err: %v", err)
				os.Exit(1)
			}
			l, err := openLedger(algodLedger, &genesis, &genesisBlock)
			if err != nil {
				fmt.Printf("error opening algod database err: %v", err)
				os.Exit(1)
			}

			err = catchup(db, l, bot, logger)
			if err != nil {
				fmt.Printf("error catching up err: %v", err)
				os.Exit(1)
			}
		},
	}

	rootCmd.Flags().StringVar(&algodAddr, "algod-net", "", "host:port of algod")
	rootCmd.MarkFlagRequired("algod-net")

	rootCmd.Flags().StringVar(
		&algodToken, "algod-token", "", "api access token for algod")
	rootCmd.MarkFlagRequired("algod-token")

	rootCmd.Flags().StringVar(
		&algodLedger, "algod-ledger", "", "path to algod ledger directory")
	rootCmd.MarkFlagRequired("algod-ledger")

	rootCmd.Flags().StringVar(
		&postgresConnStr, "postgres", "", "connection string for postgres database")
	rootCmd.MarkFlagRequired("postgres")

	rootCmd.Execute()
}
