package writer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/protocol"
	"github.com/jackc/pgx/v4"

	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres/internal/encoding"
)

const (
	addBlockHeaderStmtName = "add_block_header"
	setSpecialAccountsStmtName = "set_special_accounts"
	addTxnStmtName = "add_txn"
	addTxnParticipantStmtName = "add_txn_participant"
	upsertAssetStmtName = "upsert_asset"
	upsertAccountAssetStmtName = "upsert_account_asset"
	upsertAppStmtName = "upsert_app"
	upsertAccountAppStmtName = "upsert_account_app"
	deleteAccountStmtName = "delete_account"
	upsertAccountStmtName = "upsert_account"
	deleteAssetStmtName = "delete_asset"
	deleteAccountAssetStmtName = "delete_account_asset"
	deleteAppStmtName = "delete_app"
	deleteAccountAppStmtName = "delete_account_app"
	updateAccountKeyTypeStmtName = "update_account_key_type"
)

var statements = map[string]string{
	addBlockHeaderStmtName: `INSERT INTO block_header
	(round, realtime, rewardslevel, header)
	VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
	setSpecialAccountsStmtName: `INSERT INTO metastate (k, v)
	VALUES ('accounts', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`,
	addTxnStmtName: `INSERT INTO txn
	(round, intra, typeenum, asset, txid, txnbytes, txn, extra)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`,
	addTxnParticipantStmtName: `INSERT INTO txn_participation
	(addr, round, intra) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
	upsertAssetStmtName: `INSERT INTO asset
	(index, creator_addr, params, deleted, created_at)
	VALUES($1, $2, $3, FALSE, $4) ON CONFLICT (index) DO UPDATE SET
	creator_addr = EXCLUDED.creator_addr, params = EXCLUDED.params, deleted = FALSE`,
	upsertAccountAssetStmtName: `INSERT INTO account_asset
	(addr, assetid, amount, frozen, deleted, created_at)
	VALUES($1, $2, $3, $4, FALSE, $5) ON CONFLICT (addr, assetid) DO UPDATE SET
	amount = EXCLUDED.amount, frozen = EXCLUDED.frozen, deleted = FALSE`,
	upsertAppStmtName: `INSERT INTO app
	(index, creator, params, deleted, created_at)
	VALUES($1, $2, $3, FALSE, $4) ON CONFLICT (index) DO UPDATE SET
	creator = EXCLUDED.creator, params = EXCLUDED.params, deleted = FALSE`,
	upsertAccountAppStmtName: `INSERT INTO account_app
	(addr, app, localstate, deleted, created_at)
	VALUES($1, $2, $3, FALSE, $4) ON CONFLICT (addr, app) DO UPDATE SET
	localstate = EXCLUDED.localstate, deleted = FALSE`,
	deleteAccountStmtName: `INSERT INTO account
	(addr, microalgos, rewardsbase, rewards_total, deleted, created_at, closed_at)
	VALUES($1, 0, 0, 0, TRUE, $2, $2) ON CONFLICT (addr) DO UPDATE SET
	microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
	rewards_total = EXCLUDED.rewards_total, deleted = TRUE,
	closed_at = EXCLUDED.closed_at, account_data = EXCLUDED.account_data`,
	upsertAccountStmtName: `INSERT INTO account
	(addr, microalgos, rewardsbase, rewards_total, deleted, created_at, account_data)
	VALUES($1, $2, $3, $4, FALSE, $5, $6) ON CONFLICT (addr) DO UPDATE SET
	microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
	rewards_total = EXCLUDED.rewards_total, deleted = FALSE,
	account_data = EXCLUDED.account_data`,
	deleteAssetStmtName: `INSERT INTO asset
	(index, creator_addr, params, deleted, created_at, closed_at)
	VALUES($1, $2, 'null'::jsonb, TRUE, $3, $3) ON CONFLICT (index) DO UPDATE SET
	creator_addr = EXCLUDED.creator_addr, params = EXCLUDED.params, deleted = TRUE,
	closed_at = EXCLUDED.closed_at`,
	deleteAccountAssetStmtName: `INSERT INTO account_asset
	(addr, assetid, amount, frozen, deleted, created_at, closed_at)
	VALUES($1, $2, 0, false, TRUE, $3, $3) ON CONFLICT (addr, assetid) DO UPDATE SET
	amount = EXCLUDED.amount, deleted = TRUE, closed_at = EXCLUDED.closed_at`,
	deleteAppStmtName: `INSERT INTO app
	(index, creator, params, deleted, created_at, closed_at)
	VALUES($1, $2, 'null'::jsonb, TRUE, $3, $3) ON CONFLICT (index) DO UPDATE SET
	creator = EXCLUDED.creator, params = EXCLUDED.params, deleted = TRUE,
	closed_at = EXCLUDED.closed_at`,
	deleteAccountAppStmtName: `INSERT INTO account_app
	(addr, app, localstate, deleted, created_at, closed_at)
	VALUES($1, $2, 'null'::jsonb, TRUE, $3, $3) ON CONFLICT (addr, app) DO UPDATE SET
	localstate = EXCLUDED.localstate, deleted = TRUE, closed_at = EXCLUDED.closed_at`,
	updateAccountKeyTypeStmtName: `UPDATE account SET keytype = $1 WHERE addr = $2`,
}

// Writer is responsible for writing blocks and accounting state deltas to the database.
type Writer struct {
	tx pgx.Tx
}

// MakeWriter creates a Writer object.
func MakeWriter(tx pgx.Tx) (Writer, error) {
	w := Writer{
		tx: tx,
	}

	for name, query := range statements {
		_, err := tx.Prepare(context.Background(), name, query)
		if err != nil {
			return Writer{}, fmt.Errorf("MakeWriter() prepare statement err: %w", err)
		}
	}

	return w, nil
}

// Close shuts down Writer.
func (w *Writer) Close() {
	for name := range statements {
		w.tx.Conn().Deallocate(context.Background(), name)
	}
}

func addBlockHeader(batch *pgx.Batch, blockHeader *bookkeeping.BlockHeader) {
	batch.Queue(
		addBlockHeaderStmtName,
		uint64(blockHeader.Round), time.Unix(blockHeader.TimeStamp, 0).UTC(),
		blockHeader.RewardsLevel, encoding.EncodeBlockHeader(*blockHeader))
}

func setSpecialAccounts(batch *pgx.Batch, addresses transactions.SpecialAddresses) {
	j := encoding.EncodeSpecialAddresses(addresses)
	batch.Queue(setSpecialAccountsStmtName, j)
}

// Get the ID of the creatable referenced in the given transaction
// (0 if not an asset or app transaction).
func transactionAssetID(block *bookkeeping.Block, intra uint64, typeenum idb.TxnTypeEnum) uint64 {
	assetid := uint64(0)
	txn := block.Payset[intra].Txn

	switch typeenum {
	case idb.TypeEnumAssetConfig:
		assetid = uint64(txn.ConfigAsset)
		if assetid == 0 {
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case idb.TypeEnumAssetTransfer:
		assetid = uint64(txn.XferAsset)
	case idb.TypeEnumAssetFreeze:
		assetid = uint64(txn.FreezeAsset)
	case idb.TypeEnumApplication:
		assetid = uint64(txn.ApplicationID)
		if assetid == 0 {
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	}

	return assetid
}

// Add transactions from `block` to the database. `modifiedTxns` contains enhanced
// apply data generated by evaluator.
func addTransactions(batch *pgx.Batch, block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock) error {
	for i, stib := range block.Payset {
		var stxnad transactions.SignedTxnWithAD
		var err error
		// This function makes sure to set correct genesis information so we can get the
		// correct transaction hash.
		stxnad.SignedTxn, stxnad.ApplyData, err = block.BlockHeader.DecodeSignedTxn(stib)
		if err != nil {
			return fmt.Errorf("addTransactions() decode signed txn err: %w", err)
		}

		txn := &stxnad.Txn
		typeenum, ok := idb.GetTypeEnum(txn.Type)
		if !ok {
			return fmt.Errorf("addTransactions() get type enum")
		}
		assetid := transactionAssetID(block, uint64(i), typeenum)
		id := txn.ID().String()
		extra := idb.TxnExtra{
			AssetCloseAmount: modifiedTxns[i].ApplyData.AssetClosingAmount,
		}
		batch.Queue(
			addTxnStmtName,
			uint64(block.Round()), i, int(typeenum), assetid, id,
			protocol.Encode(&stxnad),
			encoding.EncodeSignedTxnWithAD(stxnad),
			encoding.EncodeJSON(extra))
	}

	return nil
}

func getTransactionParticipants(txn transactions.Transaction) []basics.Address {
	res := make([]basics.Address, 0, 7)

	add := func(address basics.Address) {
		if address.IsZero() {
			return
		}
		for _, p := range res {
			if address == p {
				return
			}
		}
		res = append(res, address)
	}

	add(txn.Sender)
	add(txn.Receiver)
	add(txn.CloseRemainderTo)
	add(txn.AssetSender)
	add(txn.AssetReceiver)
	add(txn.AssetCloseTo)
	add(txn.FreezeAccount)

	return res
}

func addTransactionParticipation(batch *pgx.Batch, block *bookkeeping.Block) error {
	for i, stxnad := range block.Payset {
		participants := getTransactionParticipants(stxnad.Txn)

		for j := range participants {
			batch.Queue(addTxnParticipantStmtName, participants[j][:], uint64(block.Round()), i)
		}
	}

	return nil
}

func writeAccountData(batch *pgx.Batch, round basics.Round, address basics.Address, accountData basics.AccountData) {
	// Update `asset` table.
	for assetid, params := range accountData.AssetParams {
		batch.Queue(
			upsertAssetStmtName,
			uint64(assetid), address[:], encoding.EncodeAssetParams(params), uint64(round))
	}

	// Update `account_asset` table.
	for assetid, holding := range accountData.Assets {
		batch.Queue(
			upsertAccountAssetStmtName,
			address[:], uint64(assetid), strconv.FormatUint(holding.Amount, 10),
			holding.Frozen, uint64(round))
	}

	// Update `app` table.
	for appid, params := range accountData.AppParams {
		batch.Queue(
			upsertAppStmtName,
			uint64(appid), address[:], encoding.EncodeAppParams(params), uint64(round))
	}

	// Update `account_app` table.
	for appid, state := range accountData.AppLocalStates {
		batch.Queue(
			upsertAccountAppStmtName,
			address[:], uint64(appid), encoding.EncodeAppLocalState(state), uint64(round))
	}

	// Update `account` table.
	if accountData.IsZero() {
		// Delete account.
		batch.Queue(deleteAccountStmtName, address[:], uint64(round))
	} else {
		// Update account.
		accountDataJSON :=
			encoding.EncodeTrimmedAccountData(encoding.TrimAccountData(accountData))
		batch.Queue(
			upsertAccountStmtName,
			address[:], accountData.MicroAlgos.Raw, accountData.RewardsBase,
			accountData.RewardedMicroAlgos.Raw, uint64(round), accountDataJSON)
	}
}

func writeAccountDeltas(batch *pgx.Batch, round basics.Round, deltas ledgercore.AccountDeltas, specialAddresses transactions.SpecialAddresses) {
	// Update `account` table.
	for i := 0; i < deltas.Len(); i++ {
		address, accountData := deltas.GetByIdx(i)

		// Indexer currently doesn't support special accounts.
		// TODO: remove this check.
		if (address != specialAddresses.FeeSink) &&
			(address != specialAddresses.RewardsPool) {
			writeAccountData(batch, round, address, accountData)
		}
	}
}

func writeDeletedCreatables(batch *pgx.Batch, round basics.Round, creatables map[basics.CreatableIndex]ledgercore.ModifiedCreatable) {
	for index, creatable := range creatables {
		// If deleted.
		if !creatable.Created {
			creator := new(basics.Address)
			*creator = creatable.Creator

			if creatable.Ctype == basics.AssetCreatable {
				batch.Queue(deleteAssetStmtName, uint64(index), creator[:], uint64(round))
			} else {
				batch.Queue(deleteAppStmtName, uint64(index), creator[:], uint64(round))
			}
		}
	}
}

func writeDeletedAssetHoldings(batch *pgx.Batch, round basics.Round, modifiedAssetHoldings map[ledgercore.AccountAsset]bool) {
	for aa, created := range modifiedAssetHoldings {
		if !created {
			address := new(basics.Address)
			*address = aa.Address

			batch.Queue(
				deleteAccountAssetStmtName, address[:], uint64(aa.Asset), uint64(round))
		}
	}
}

func writeDeletedAppLocalStates(batch *pgx.Batch, round basics.Round, modifiedAppLocalStates map[ledgercore.AccountApp]bool) {
	for aa, created := range modifiedAppLocalStates {
		if !created {
			address := new(basics.Address)
			*address = aa.Address

			batch.Queue(deleteAccountAppStmtName, address[:], uint64(aa.App), uint64(round))
		}
	}
}

func writeStateDelta(batch *pgx.Batch, round basics.Round, delta ledgercore.StateDelta, specialAddresses transactions.SpecialAddresses) {
	writeAccountDeltas(batch, round, delta.Accts, specialAddresses)
	writeDeletedCreatables(batch, round, delta.Creatables)
	writeDeletedAssetHoldings(batch, round, delta.ModifiedAssetHoldings)
	writeDeletedAppLocalStates(batch, round, delta.ModifiedAppLocalStates)
}

func updateAccountSigType(batch *pgx.Batch, payset []transactions.SignedTxnInBlock) error {
	for i := range payset {
		if payset[i].Txn.RekeyTo == (basics.Address{}) {
			sigtype, err := idb.SignatureType(&payset[i].SignedTxn)
			if err != nil {
				return fmt.Errorf("updateAccountSigType() err: %w", err)
			}
			batch.Queue(updateAccountKeyTypeStmtName, sigtype, payset[i].Txn.Sender[:])
		} else {
			batch.Queue(updateAccountKeyTypeStmtName, nil, payset[i].Txn.Sender[:])
		}
	}

	return nil
}

// AddBlock writes the block and accounting state deltas to the database.
func (w *Writer) AddBlock(block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock, delta ledgercore.StateDelta) error {
	var batch pgx.Batch

	specialAddresses := transactions.SpecialAddresses{
		FeeSink:     block.FeeSink,
		RewardsPool: block.RewardsPool,
	}

	addBlockHeader(&batch, &block.BlockHeader)
	setSpecialAccounts(&batch, specialAddresses)
	err := addTransactions(&batch, block, modifiedTxns)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}
	err = addTransactionParticipation(&batch, block)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}
	writeStateDelta(&batch, block.Round(), delta, specialAddresses)
	err = updateAccountSigType(&batch, block.Payset)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}

	results := w.tx.SendBatch(context.Background(), &batch)
	for i := 0; i < batch.Len(); i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("AddBlock() exec err: %w", err)
		}
	}
	err = results.Close()
	if err != nil {
		return fmt.Errorf("AddBlock() close results err: %w", err)
	}

	return nil
}
