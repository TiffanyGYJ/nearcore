use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};

use near_primitives::block::Block;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::hash;
use near_primitives::types::{AccountId, NumShards, ShardId};

use crate::migrations::v8_to_v9::{repair_col_receipt_id_to_shard_id, repair_col_transactions};
use crate::{DBCol, Store, StoreUpdate};

// Refcount from i32 to i64
pub(crate) fn col_state_refcount_8byte(store: &Store, store_update: &mut StoreUpdate) {
    for (k, v) in store.iter_without_rc_logic(DBCol::ColState) {
        if v.len() < 4 {
            store_update.delete(DBCol::ColState, &k);
            continue;
        }
        let mut v = v.into_vec();
        v.extend_from_slice(&[0, 0, 0, 0]);
        store_update.set(DBCol::ColState, &k, &v);
    }
}

// Deprecate ColTransactionRefCount, move the info to ColTransactions
pub(crate) fn migrate_col_transaction_refcount(store: &Store, store_update: &mut StoreUpdate) {
    // Discard the data we had, reconstruct from ColChunks
    repair_col_transactions(store, store_update);
    for (key, _value) in store.iter(DBCol::_ColTransactionRefCount) {
        store_update.delete(DBCol::_ColTransactionRefCount, &key);
    }
}

pub(crate) fn get_num_shards(store: &Store) -> NumShards {
    store
        .iter(DBCol::ColBlock)
        .map(|(_key, value)| {
            Block::try_from_slice(value.as_ref()).expect("BorshDeserialize should not fail")
        })
        .map(|block| block.chunks().len() as u64)
        .next()
        .unwrap_or(1)
}

pub(crate) fn account_id_to_shard_id_v6(account_id: &AccountId, num_shards: NumShards) -> ShardId {
    let mut cursor = Cursor::new((hash(&account_id.clone().into_bytes()).0).0);
    cursor.read_u64::<LittleEndian>().expect("Must not happened") % (num_shards)
}

// Make ColReceiptIdToShardId refcounted
pub(crate) fn migrate_receipts_refcount(store: &Store, store_update: &mut StoreUpdate) {
    // Discard the data we had, reconstruct from ColOutgoingReceipts
    repair_col_receipt_id_to_shard_id(store, store_update);
}
