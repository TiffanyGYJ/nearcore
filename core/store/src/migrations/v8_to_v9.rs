use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::ShardId;

use crate::db::refcount::encode_value_with_rc;
use crate::migrations::v6_to_v7::{account_id_to_shard_id_v6, get_num_shards};
use crate::{DBCol, Store, StoreUpdate};

fn replace_col(
    store: &Store,
    store_update: &mut StoreUpdate,
    column: DBCol,
    values: HashMap<CryptoHash, Vec<u8>>,
) {
    let mut values = values;
    for (key, value) in store.iter_without_rc_logic(column) {
        let hash = CryptoHash::try_from_slice(&key).unwrap();
        match values.remove(&hash) {
            None => {
                store_update.delete(column, &key);
            }
            Some(encoded) => {
                if &encoded[..] != &value[..] {
                    store_update.set(column, &key, &encoded);
                }
            }
        }
    }
    for (hash, encoded) in values.drain() {
        store_update.set(column, hash.as_ref(), &encoded);
    }
}

fn encode_with_rc<ValueType: BorshSerialize>(
    values: HashMap<CryptoHash, (ValueType, i64)>,
) -> HashMap<CryptoHash, Vec<u8>> {
    values
        .into_iter()
        .map(|(key, (value, rc))| (key, encode_value_with_rc(&value.try_to_vec().unwrap(), rc)))
        .collect::<HashMap<_, _>>()
}

fn replace_col_rc<ValueType: BorshSerialize>(
    store: &Store,
    store_update: &mut StoreUpdate,
    column: DBCol,
    values: HashMap<CryptoHash, (ValueType, i64)>,
) {
    replace_col(store, store_update, column, encode_with_rc(values))
}

// Make ColTransactions match transactions in ColChunks
pub(crate) fn repair_col_transactions(store: &Store, store_update: &mut StoreUpdate) {
    let mut tx_refcount: HashMap<CryptoHash, (SignedTransaction, i64)> = HashMap::new();
    for tx in store
        .iter(DBCol::ColChunks)
        .map(|(_key, value)| {
            ShardChunk::try_from_slice(&value).expect("BorshDeserialize should not fail")
        })
        .flat_map(|chunk: ShardChunk| chunk.transactions)
    {
        tx_refcount.entry(tx.get_hash()).and_modify(|(_, rc)| *rc += 1).or_insert((tx, 1));
    }

    replace_col_rc(store, store_update, DBCol::ColTransactions, tx_refcount);
}

// Make ColReceiptIdToShardId match receipts in ColOutgoingReceipts
pub(crate) fn repair_col_receipt_id_to_shard_id(store: &Store, store_update: &mut StoreUpdate) {
    let num_shards = get_num_shards(&store);
    let mut receipt_refcount: HashMap<CryptoHash, (ShardId, i64)> = HashMap::new();
    for receipt in store.iter(DBCol::ColOutgoingReceipts).flat_map(|(_key, value)| {
        <Vec<Receipt>>::try_from_slice(&value).expect("BorshDeserialize should not fail")
    }) {
        receipt_refcount
            .entry(receipt.receipt_id)
            .and_modify(|(_, rc)| *rc += 1)
            .or_insert((account_id_to_shard_id_v6(&receipt.receiver_id, num_shards), 1));
    }

    replace_col_rc(store, store_update, DBCol::ColReceiptIdToShardId, receipt_refcount);
}
