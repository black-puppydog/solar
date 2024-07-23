use std::io::Read;

use futures::SinkExt;
use kuska_ssb::feed::{Feed as MessageKvt, Message as MessageValue};
use log::{debug, log, warn};
use serde::{Deserialize, Serialize};
use sled::{Config as DbConfig, Db};

use crate::{
    broker::{BrokerEvent, BrokerMessage, ChBrokerSend, Destination},
    error::Error,
    storage::indexes::Indexes,
    Result,
};

// TODO: Consider replacing prefix-based approach with separate db trees.

/// Prefix for a key to the latest sequence number for a stored feed.
const PREFIX_LATEST_SEQ: u8 = 0u8;
/// Prefix for a key to a message KVT (Key Value Timestamp).
const PREFIX_MSG_KVT: u8 = 1u8;
/// Prefix for a key to a message value (the 'V' in KVT).
const PREFIX_MSG_VAL: u8 = 2u8;
/// Prefix for a key to a blob.
const PREFIX_BLOB: u8 = 3u8;
/// Prefix for a key to a peer.
const PREFIX_PEER: u8 = 4u8;

/// Unique key in which the latest sequence number in the global order is stored.
const GLOBAL_ORDER_KEY: &'static str = "solar:global_seq";

/// A new message has been appended to feed belonging to the given SSB ID.
#[derive(Debug, Clone)]
pub struct StoreKvEvent(pub (String, u64));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStatus {
    retrieved: bool,
    users: Vec<String>,
}

/// The public key (ID) of a peer and a message sequence number.
#[derive(Debug, Serialize, Deserialize)]
pub struct PubKeyAndSeqNum {
    pub_key: String,
    seq_num: u64,
}

// TODO: Can we remove the `Option` from all of these fields?
// Will make the rest of the code more compact (no need to match on an
// `Option` every time).
// Will probably require some changes in `solar_cli` config.
#[derive(Default)]
pub struct KvStorage {
    /// The core database which stores messages and blob references.
    db: Option<Db>,
    /// Indexes to allow for efficient database value look-ups.
    pub indexes: Option<Indexes>,
    /// A message-passing sender.
    ch_broker: Option<ChBrokerSend>,
}

fn buffer_to_u64(buffer: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(buffer);
    u64::from_be_bytes(array)
}

impl KvStorage {
    /// Open the key-value database using the given configuration, open the
    /// database index trees and populate the instance of `KvStorage`
    /// with the database, indexes and message-passing sender.
    pub async fn open(&mut self, config: DbConfig, ch_broker: ChBrokerSend) -> Result<()> {
        println!("Opening KvStorage");
        let db = config.open()?;
        let indexes = Indexes::open(&db)?;

        self.db = Some(db);
        self.indexes = Some(indexes);
        self.ch_broker = Some(ch_broker);

        // check if the global_order key exists and is equal to 1u8
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let global_order_seq = self.get_global_order_seq().await?;

        if global_order_seq == 0u64 {
            // build the global order index
            self.build_global_order_index().await?;
            // set the solar:global_order flag so we don't re-do this
            // TODO: re-enable once we have a way to reset the global order
            db.insert("solar:global_order".as_bytes(), 1u8.to_be_bytes().to_vec())?;
        } else {
            log!(
                log::Level::Info,
                "global_order_exists: {}",
                global_order_seq
            );
        }

        Ok(())
    }

    /// Generate a key for the latest sequence number of the feed authored by
    /// the given public key.
    fn key_latest_seq(user_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_LATEST_SEQ);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Generate a key for a message KVT authored by the given public key and
    /// with the given message sequence number.
    fn key_msg_kvt(user_id: &str, msg_seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_MSG_KVT);
        key.extend_from_slice(&msg_seq.to_be_bytes()[..]);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Generate a key for a message value with the given ID (reference).
    fn key_msg_val(msg_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_MSG_VAL);
        key.extend_from_slice(msg_id.as_bytes());
        key
    }

    /// Generate a key for a blob with the given ID (reference).
    fn key_blob(blob_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_BLOB);
        key.extend_from_slice(blob_id.as_bytes());
        key
    }

    /// Generate a key for a peer with the given public key.
    fn key_peer(user_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_PEER);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Get the status of a blob with the given ID.
    pub fn get_blob(&self, blob_id: &str) -> Result<Option<BlobStatus>> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        if let Some(raw) = db.get(Self::key_blob(blob_id))? {
            Ok(serde_cbor::from_slice(&raw)?)
        } else {
            Ok(None)
        }
    }

    /// Set the status of a blob with the given ID.
    pub fn set_blob(&self, blob_id: &str, blob: &BlobStatus) -> Result<()> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let raw = serde_cbor::to_vec(blob)?;
        db.insert(Self::key_blob(blob_id), raw)?;

        Ok(())
    }

    /// Get a list of IDs for all blobs which have not yet been retrieved.
    pub fn get_pending_blobs(&self) -> Result<Vec<String>> {
        let mut list = Vec::new();

        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let scan_key_start: &[u8] = &[PREFIX_BLOB];
        let scan_key_end: &[u8] = &[PREFIX_BLOB + 1];
        for item in db.range(scan_key_start..scan_key_end) {
            let (k, v) = item?;
            let blob: BlobStatus = serde_cbor::from_slice(&v)?;
            if !blob.retrieved {
                list.push(String::from_utf8_lossy(&k[1..]).to_string());
            }
        }

        Ok(list)
    }

    /// Get the sequence number of the latest message in the feed authored by
    /// the peer with the given public key.
    pub fn get_latest_seq(&self, user_id: &str) -> Result<Option<u64>> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let key = Self::key_latest_seq(user_id);
        let seq = if let Some(value) = db.get(key)? {
            Some(buffer_to_u64(&value))
        } else {
            None
        };

        Ok(seq)
    }
    /// Get the message KVT (Key Value Timestamp) for the given author and
    /// message sequence number.
    pub fn get_msg_kvt(&self, user_id: &str, msg_seq: u64) -> Result<Option<MessageKvt>> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        if let Some(raw) = db.get(Self::key_msg_kvt(user_id, msg_seq))? {
            Ok(Some(MessageKvt::from_slice(&raw)?))
        } else {
            Ok(None)
        }
    }

    /// Get the message value for the given message ID (key).
    pub fn get_msg_val(&self, msg_id: &str) -> Result<Option<MessageValue>> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;

        if let Some(raw) = db.get(Self::key_msg_val(msg_id))? {
            let msg_ref = serde_cbor::from_slice::<PubKeyAndSeqNum>(&raw)?;
            let msg = self
                .get_msg_kvt(&msg_ref.pub_key, msg_ref.seq_num)?
                .ok_or(Error::OptionIsNone)?
                .into_message()?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Get the latest message value authored by the given public key.
    pub fn get_latest_msg_val(&self, user_id: &str) -> Result<Option<MessageValue>> {
        let latest_msg = if let Some(last_id) = self.get_latest_seq(user_id)? {
            Some(
                self.get_msg_kvt(user_id, last_id)?
                    .ok_or(Error::OptionIsNone)?
                    .into_message()?,
            )
        } else {
            None
        };

        Ok(latest_msg)
    }

    /// Add the public key and latest sequence number of a peer to the list of
    /// peers.
    pub async fn set_peer(&self, user_id: &str, latest_seq: u64) -> Result<()> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        db.insert(Self::key_peer(user_id), &latest_seq.to_be_bytes()[..])?;

        // TODO: Should we be flushing here?
        // Flush may have a performance impact. It may also be unnecessary
        // depending on where / when this method is called.

        Ok(())
    }

    /// Return the public key and latest sequence number for all peers in the
    /// database.
    pub async fn get_peers(&self) -> Result<Vec<(String, u64)>> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let mut peers = Vec::new();

        // Use the generic peer prefix to return an iterator over all peers.
        let scan_peer_key_start: &[u8] = &[PREFIX_PEER];
        let scan_peer_key_end: &[u8] = &[PREFIX_PEER + 1];
        for peer in db.range(scan_peer_key_start..scan_peer_key_end) {
            let (peer_key, _) = peer?;
            // Drop the prefix byte and convert the remaining bytes to
            // a string.
            let pub_key = String::from_utf8_lossy(&peer_key[1..]).to_string();
            // Get the latest sequence number for the peer.
            // Fallback to a value of 0 if a `None` value is returned.
            let seq_num = self.get_latest_seq(&pub_key)?.unwrap_or(0);
            peers.push((pub_key, seq_num))
        }

        Ok(peers)
    }

    /// Append a message value to a feed.
    pub async fn append_feed(&self, msg_val: MessageValue) -> Result<u64> {
        debug!("Appending message to feed in database");
        let seq_num = self.get_latest_seq(msg_val.author())?.map_or(0, |num| num) + 1;

        if msg_val.sequence() != seq_num {
            return Err(Error::InvalidSequence);
        }

        let author = msg_val.author().to_owned();
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;

        let msg_ref = serde_cbor::to_vec(&PubKeyAndSeqNum {
            pub_key: author.clone(),
            seq_num,
        })?;

        db.insert(Self::key_msg_val(&msg_val.id().to_string()), msg_ref)?;

        let mut msg_kvt = MessageKvt::new(msg_val.clone());
        self.increment_global_seq(&msg_kvt.key).await?;
        msg_kvt.rts = None;
        db.insert(
            Self::key_msg_kvt(&author, seq_num),
            msg_kvt.to_string().as_bytes(),
        )?;
        db.insert(Self::key_latest_seq(&author), &seq_num.to_be_bytes()[..])?;

        // Add the public key and latest sequence number for this peer to the
        // list of peers.
        self.set_peer(&author, seq_num).await?;

        debug!("Passing message to indexer");
        // Pass the author and message value to the indexer.
        if let Some(indexes) = &self.indexes {
            indexes.index_msg(&author, msg_val)?
        }

        db.flush_async().await?;

        // Publish a notification that the feed belonging to the given public
        // key has been updated.
        let broker_msg = BrokerEvent::new(
            Destination::Broadcast,
            BrokerMessage::StoreKv(StoreKvEvent((author, seq_num))),
        );

        // Matching on the error here (instead of unwrapping) allows us to
        // write unit tests for `append_feed`; a case where we do not have
        // a broker deployed to receive the event message.
        if let Err(err) = self
            .ch_broker
            .as_ref()
            .ok_or(Error::OptionIsNone)?
            .send(broker_msg)
            .await
        {
            warn!(
                "Failed to notify broker of message appended to kv store: {}",
                err
            )
        };

        Ok(seq_num)
    }

    /// Get all messages comprising the feed authored by the given public key.
    pub fn get_feed(&self, user_id: &str) -> Result<Vec<MessageKvt>> {
        let mut feed = Vec::new();

        // Lookup the latest sequence number for the given peer.
        if let Some(latest_seq) = self.get_latest_seq(user_id)? {
            // Iterate through the messages in the feed.
            for msg_seq in 1..=latest_seq {
                // Get the message KVT for the given author and message
                // sequence number and add it to the feed vector.
                feed.push(
                    self.get_msg_kvt(user_id, msg_seq)?
                        .ok_or(Error::OptionIsNone)?,
                )
            }
        }

        Ok(feed)
    }

    /// Builds the global order index of all messages.
    /// When embedding solar, or using the RPC methods, we sometimes need
    /// to be able to iterate over all messages in the database in any
    //// (semantically useful) order.
    /// That means that this index will return messages within the same feed in-order,
    /// but the order of feeds is not guaranteed, and neither is the order of messages between feeds.
    /// I.e. this may assign sequence number N to a message A that references message B with sequence number N+M.
    async fn build_global_order_index(&self) -> Result<()> {
        // we'll simply iterate over all feeds in the database
        // and assign a global sequence number to each message
        // in the feed in order of their sequence number.
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        // first make sure we start from global order sequence number 1
        // To do so, simply delete the global_order_seq key.
        db.remove(GLOBAL_ORDER_KEY.as_bytes().to_vec())?;
        for peer in self
            .get_peers()
            .await
            .map_err(|_| Error::Indexes)?
            .into_iter()
        {
            let (pub_key, latest_seq) = peer;
            for msg_seq in 1..=latest_seq {
                // Get the message KVT for the given author and message
                // sequence number and add it to the feed vector.
                let msg = self
                    .get_msg_kvt(&pub_key, msg_seq)?
                    .ok_or(Error::OptionIsNone)?;
                self.increment_global_seq(&msg.key).await?;
            }
        }
        log!(
            log::Level::Info,
            "Built global order index with {} messages",
            self.get_global_order_seq().await?
        );
        Ok(())
    }

    /// Get the last global order sequence number for the given message key.
    /// Returns 0 if no global order sequence number is found.
    async fn get_global_order_seq(&self) -> Result<u64> {
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        let global_seq = db.get(GLOBAL_ORDER_KEY.as_bytes().to_vec())?;
        if let Some(global_seq) = global_seq {
            Ok(buffer_to_u64(&global_seq))
        } else {
            Ok(0)
        }
    }

    async fn increment_global_seq(&self, msg_key: &str) -> Result<()> {
        let new_global_seq = self.get_global_order_seq().await? + 1;
        let db = self.db.as_ref().ok_or(Error::OptionIsNone)?;
        db.insert(
            format!("global_seq:{}", new_global_seq).as_bytes().to_vec(),
            msg_key.as_bytes().to_vec(),
        )?;
        // inverted index for global sequence number.
        // we use "global_seq:{msg_ref}" as the key, and the global sequence number as the value.
        db.insert(
            format!("gloabl_seq:{}", msg_key).as_bytes().to_vec(),
            new_global_seq.to_be_bytes().to_vec(),
        )?;
        db.insert(
            GLOBAL_ORDER_KEY.as_bytes().to_vec(),
            new_global_seq.to_be_bytes().to_vec(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use kuska_ssb::{api::dto::content::TypedMessage, keystore::OwnedIdentity};
    use serde_json::json;
    use sled::Config;

    use crate::secret_config::SecretConfig;

    #[async_std::test]
    async fn open_temporary_kv() -> Result<KvStorage> {
        let mut kv = KvStorage::default();
        let (sender, _) = futures::channel::mpsc::unbounded();
        let path = tempdir::TempDir::new("solardb").unwrap();
        let config = Config::new().path(path.path());
        kv.open(config, sender).await.unwrap();

        Ok(kv)
    }

    fn initialise_keypair_and_kv() -> Result<(OwnedIdentity, KvStorage)> {
        // Create a unique keypair to sign messages.
        let keypair = SecretConfig::create().to_owned_identity()?;

        // Open a temporary key-value store.
        let kv = open_temporary_kv()?;

        Ok((keypair, kv))
    }

    #[async_std::test]
    async fn test_feed_length() -> Result<()> {
        use kuska_ssb::feed::Message;

        let (keypair, kv) = initialise_keypair_and_kv()?;

        let mut last_msg: Option<Message> = None;
        for i in 1..=4 {
            // Create a post-type message.
            let msg_content = TypedMessage::Post {
                text: format!("Important announcement #{i}"),
                mentions: None,
            };

            let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

            // Append the signed message to the feed. Returns the sequence number
            // of the appended message.
            let seq = kv.append_feed(msg).await?;
            assert_eq!(seq, i);

            last_msg = kv.get_latest_msg_val(&keypair.id)?;

            let feed = kv.get_feed(&keypair.id)?;
            assert_eq!(feed.len(), i as usize);
        }

        Ok(())
    }

    #[async_std::test]
    async fn test_single_message_content_matches() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Create a post-type message.
        let msg_content = TypedMessage::Post {
            text: "A strange rambling expiration of my own conscious".to_string(),
            mentions: None,
        };

        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

        // Append the signed message to the feed. Returns the sequence number
        // of the appended message.
        let seq = kv.append_feed(msg).await?;
        assert_eq!(seq, 1);

        let latest_seq = kv.get_latest_seq(&keypair.id)?;
        assert_eq!(latest_seq, Some(1));

        // Lookup the value of the previous message. This will be `None`
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        assert!(last_msg.is_some());
        let expected = serde_json::value::to_value(msg_content)?;
        let last_msg = last_msg.unwrap().content().clone();

        assert_eq!(last_msg, expected);

        Ok(())
    }

    #[async_std::test]
    async fn test_new_feed_is_empty() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Lookup the value of the previous message. This will be `None`
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        assert!(last_msg.is_none());

        let latest_seq = kv.get_latest_seq(&keypair.id)?;
        assert!(latest_seq.is_none());

        Ok(())
    }

    #[async_std::test]
    async fn test_peer_range_query() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;
        // Get a list of all replicated peers and their latest sequence
        // numbers. This list is expected to be empty because we never
        // added any data to the database.
        let peers = kv.get_peers().await?;
        assert_eq!(peers.len(), 0);

        // Create a post-type message.
        let msg_content = TypedMessage::Post {
            text: "A solar flare is an intense localized eruption of electromagnetic radiation."
                .to_string(),
            mentions: None,
        };

        // Lookup the value of the previous message. This will be `None`.
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;

        // Sign the message content using the temporary keypair and value of
        // the previous message.
        let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

        // Append the signed message to the feed. Returns the sequence number
        // of the appended message.
        let _ = kv.append_feed(msg).await?;

        // now that we have added a message, we should have one peer,
        // which is the keypair we used to sign the message.
        let peers = kv.get_peers().await?;
        assert_eq!(peers.len(), 1);
        assert_eq!(&peers.get(0).unwrap().0, &keypair.id);

        let db = kv.db.as_ref().ok_or(Error::OptionIsNone)?;

        // insert one key with PREFIX_PEER+1 as the first byte.
        db.insert(
            &vec![PREFIX_PEER + 1u8],
            "this should not show up in the peers list because it's after the peers range"
                .as_bytes()
                .to_vec(),
        )?;

        // this should not have changed the peers list
        let peers = kv.get_peers().await?;
        assert_eq!(peers.len(), 1);

        // do the same for PREFIX_PEER-1
        db.insert(
            &vec![PREFIX_PEER - 1u8],
            "this should not show up in the peers list because it's before the peers range"
                .as_bytes()
                .to_vec(),
        )?;

        // this should not have changed the peers list
        let peers = kv.get_peers().await?;
        assert_eq!(peers.len(), 1);

        Ok(())
    }

    // In reality this test covers more than just the append method.
    // It tests multiple methods exposed by the kv database.
    // The main reason for combining the tests is the cost of setting up
    // testable conditions (ie. creating the keypair and database and
    // it with messages). Perhaps this could be broken up in the future.
    #[async_std::test]
    async fn test_append_feed() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Create a post-type message.
        let msg_content = TypedMessage::Post {
            text: "A solar flare is an intense localized eruption of electromagnetic radiation."
                .to_string(),
            mentions: None,
        };

        // Lookup the value of the previous message. This will be `None`.
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;

        // Sign the message content using the temporary keypair and value of
        // the previous message.
        let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

        // Append the signed message to the feed. Returns the sequence number
        // of the appended message.
        let seq = kv.append_feed(msg).await?;

        // Ensure that the message is the first in the feed.
        assert_eq!(seq, 1);

        // Get the latest sequence number.
        let latest_seq = kv.get_latest_seq(&keypair.id)?;

        // Ensure the stored sequence number matches that of the appended
        // message.
        assert_eq!(latest_seq, Some(seq));

        // Get a list of all replicated peers and their latest sequence
        // numbers. This list is expected to contain an entry for the
        // local keypair.
        let peers = kv.get_peers().await?;

        // Ensure there is only one entry in the peers list.
        assert_eq!(peers.len(), 1);
        // Ensure the public key of the peer matches expectations and that
        // the sequence number is correct.
        assert_eq!(peers[0].0, keypair.id);
        assert_eq!(peers[0].1, 1);

        // Create, sign and append a second post-type message.
        let msg_content_2 = TypedMessage::Post {
            text: "When the sun shone upon her.".to_string(),
            mentions: None,
        };
        let last_msg_2 = kv.get_latest_msg_val(&keypair.id)?;
        let msg_2 = MessageValue::sign(last_msg_2.as_ref(), &keypair, json!(msg_content_2))?;
        let msg_2_clone = msg_2.clone();
        let seq_2 = kv.append_feed(msg_2).await?;

        // Ensure that the message is the second in the feed.
        assert_eq!(seq_2, 2);

        // Get the second message in the key-value store in the form of a KVT.
        let msg_kvt = kv.get_msg_kvt(&keypair.id, 2)?;
        assert!(msg_kvt.is_some());

        // Retrieve the key from the KVT.
        let msg_kvt_key = msg_kvt.unwrap().key;

        // Get the second message in the key-value store in the form of a value.
        let msg_val = kv.get_msg_val(&msg_kvt_key)?;

        // Ensure the retrieved message value matches the previously created
        // and signed message.
        assert_eq!(msg_val, Some(msg_2_clone));

        // Get all messages comprising the feed.
        let feed = kv.get_feed(&keypair.id)?;

        // Ensure that two messages are returned.
        assert_eq!(feed.len(), 2);

        Ok(())
    }

    #[async_std::test]
    async fn test_blobs_range_query_when_peers_exist() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;
        kv.set_blob(
            "b1",
            &BlobStatus {
                retrieved: false,
                users: ["u2".to_string()].to_vec(),
            },
        )?;

        assert_eq!(kv.get_peers().await?.len(), 0);

        assert_eq!(kv.get_pending_blobs().unwrap(), vec!["b1".to_string()]);

        println!("Inserting a new message and thus peer");
        let msg_content = TypedMessage::Post {
            text: "A solar flare is an intense localized eruption of electromagnetic radiation."
                .to_string(),
            mentions: None,
        };
        // Passing None as the last message since we start from an empty feed
        let msg = MessageValue::sign(None, &keypair, json!(msg_content))?;
        let _ = kv.append_feed(msg).await?;

        // now that we have added a message, we should have one peer,
        // which is the keypair we used to sign the message.
        let peers = kv.get_peers().await?;
        assert_eq!(peers.len(), 1);

        println!("Inserting a second blob");
        kv.set_blob(
            "b2",
            &BlobStatus {
                retrieved: false,
                users: ["u7".to_string()].to_vec(),
            },
        )?;

        assert_eq!(
            kv.get_pending_blobs()?,
            vec!["b1".to_string(), "b2".to_string()]
        );

        Ok(())
    }

    #[test]
    fn test_blobs() -> Result<()> {
        let kv = open_temporary_kv()?;

        assert!(kv.get_blob("1")?.is_none());

        kv.set_blob(
            "b1",
            &BlobStatus {
                retrieved: true,
                users: ["u1".to_string()].to_vec(),
            },
        )?;

        kv.set_blob(
            "b2",
            &BlobStatus {
                retrieved: false,
                users: ["u2".to_string()].to_vec(),
            },
        )?;

        if let Some(blob) = kv.get_blob("b1")? {
            assert!(blob.retrieved);
            assert_eq!(blob.users, ["u1".to_string()].to_vec());
            assert_eq!(kv.get_pending_blobs().unwrap(), ["b2".to_string()].to_vec());
        }

        kv.set_blob(
            "b1",
            &BlobStatus {
                retrieved: false,
                users: ["u7".to_string()].to_vec(),
            },
        )?;

        if let Some(blob) = kv.get_blob("b1")? {
            assert!(!blob.retrieved);
            assert_eq!(blob.users, ["u7".to_string()].to_vec());
            assert_eq!(
                kv.get_pending_blobs()?,
                ["b1".to_string(), "b2".to_string()].to_vec()
            );
        }

        Ok(())
    }
}
