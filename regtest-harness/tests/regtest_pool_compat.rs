//! Mining-pool compatibility: the exact calls stock s-nomp and Miningcore send their wallet
//! daemon, sent to zecd as that single daemon.
//!
//! Miningcore talks to one daemon for mining and payouts alike, so the node calls it needs
//! (`getblocktemplate`, `submitblock`, `getblock`, ...) are forwarded by zecd to its node, and
//! the wallet calls are zecd's own. The test walks a pool's life in that order: startup checks,
//! block templates and submission, the coinbase ladder (`immature`, then `generate`), shielding
//! the coinbase into Ironwood, the shielded balance check, a 50-recipient Miningcore payout with
//! its hard-coded 0.0001 fee, and s-nomp's refill and transparent payout.
//!
//! zebra backend only: lightwalletd has no node JSON-RPC to forward the mining calls to, so this
//! test is not in the zecd-lwd leg's list. The chain is mined to zecd from genesis with an
//! address derived offline, for the reason `regtest_coinbase.rs` explains.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zecd_regtest_harness::{
    derive_address_offline, derive_funder_transparent_address, pick_port, resolve_node_bin, Zebrad,
    Zecd, ZecdConfig, COINBASE_MATURITY,
};

/// The pool wallet, restored from the repo's committed throwaway regtest phrase.
const POOL_MNEMONIC: &str = "mechanic vehicle helmet decide plug gorilla frost dial october \
midnight culture idea mountain fame park social drip bid doctor scatter glance defy moment stage";

/// Blocks mined before zecd starts, below the wallet's birthday (see `regtest_coinbase.rs`).
const SEED_BLOCKS: u32 = 2;
const BIRTHDAY: u32 = SEED_BLOCKS + 1;
/// Blocks the pool "finds" once zecd is running.
const POOL_BLOCKS: u32 = 4;
/// Miningcore pays at most this many recipients per `z_sendmany`.
const MINER_PAYOUTS: usize = 50;
/// Miningcore's payout `minconf` and its hard-coded fee.
const PAYOUT_MINCONF: u32 = 8;
const MININGCORE_FEE: f64 = 0.0001;
/// What each miner is paid, in ZEC.
const PAYOUT_ZEC: f64 = 0.01;
/// ZIP 317's marginal fee per logical action, in zatoshis.
const ZIP317_MARGINAL_FEE: u64 = 5_000;

const SYNC_TIMEOUT: Duration = Duration::from_secs(240);
const OP_TIMEOUT: Duration = Duration::from_secs(300);

fn zats(zec: f64) -> u64 {
    (zec.abs() * 1e8).round() as u64
}

async fn node_tip(zebrad: &Zebrad) -> u64 {
    zebrad
        .rpc("getblockcount", json!([]))
        .await
        .expect("node getblockcount")
        .as_u64()
        .expect("block count")
}

/// Poll `getpeerinfo` until zecd's upstream is ready. Call only once a block at or above the
/// wallet's birthday exists.
async fn wait_ready(zecd: &Zecd) {
    let deadline = Instant::now() + SYNC_TIMEOUT;
    loop {
        let peers = zecd
            .call("getpeerinfo", json!([]))
            .await
            .expect("getpeerinfo");
        if peers
            .as_array()
            .and_then(|a| a.first())
            .is_some_and(|p| p["conn_state"] == "ready")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "zecd never reached ready: {peers}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn mine(zebrad: &Zebrad, zecd: &Zecd, blocks: u32) {
    zebrad.generate_blocks(blocks).await.expect("mine blocks");
    zecd.wait_until_synced_to_node(zebrad, SYNC_TIMEOUT)
        .await
        .expect("zecd sync to the node tip");
}

/// Wait for an async operation the way Miningcore does: poll `z_getoperationresult [[opid]]`
/// until it returns the finished operation, then take its txid.
async fn operation_txid(zecd: &Zecd, opid: &str) -> String {
    let deadline = Instant::now() + OP_TIMEOUT;
    loop {
        let results = zecd
            .call("z_getoperationresult", json!([[opid]]))
            .await
            .expect("z_getoperationresult");
        if let Some(op) = results.as_array().and_then(|a| a.first()) {
            assert_eq!(op["status"], "success", "operation {opid} failed: {op}");
            return op["result"]["txid"]
                .as_str()
                .expect("operation result txid")
                .to_string();
        }
        assert!(
            Instant::now() < deadline,
            "operation {opid} did not finish in {OP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The first transaction (the coinbase) of the block at `height`.
async fn coinbase_txid_at(zebrad: &Zebrad, height: u64) -> String {
    let hash = zebrad
        .rpc("getblockhash", json!([height]))
        .await
        .expect("node getblockhash");
    let block = zebrad
        .rpc("getblock", json!([hash, 1]))
        .await
        .expect("node getblock");
    block["tx"][0].as_str().expect("coinbase txid").to_string()
}

/// The category of the first `gettransaction.details` entry, the field both pool packages read
/// to decide whether a block still counts.
async fn coinbase_category(zecd: &Zecd, txid: &str) -> Value {
    let tx = zecd
        .call("gettransaction", json!([txid]))
        .await
        .expect("gettransaction of a coinbase");
    assert_eq!(
        tx["generated"],
        json!(true),
        "a coinbase is generated: {tx}"
    );
    tx["details"][0]["category"].clone()
}

#[tokio::test]
async fn regtest_pool_calls_work_against_zecd_as_the_only_daemon() {
    let Some(node_bin) = resolve_node_bin() else {
        eprintln!(
            "SKIP regtest_pool_calls_work_against_zecd_as_the_only_daemon: set the node binary \
             variable (ZEBRAD_BIN / ZAKURAD_BIN) to run it. The harness still compiled."
        );
        return;
    };

    // The pool's addresses: the coinbase t-address (s-nomp `address`, Miningcore `address`) and
    // the unified address the coinbase is shielded to (s-nomp `zAddress`, Miningcore
    // `z-address`). Workers are transparent addresses this wallet does not own.
    let pool_taddr = derive_address_offline(POOL_MNEMONIC, "transparent", 0)
        .expect("derive the pool t-address offline");
    let pool_ua =
        derive_address_offline(POOL_MNEMONIC, "orchard", 0).expect("derive the pool UA offline");
    let workers: Vec<String> = (10..10 + MINER_PAYOUTS as u32)
        .map(|i| derive_funder_transparent_address(i).expect("derive a worker address"))
        .collect();

    let zebrad = Zebrad::start_with_miner(&node_bin, &pool_taddr)
        .await
        .expect("start the node");
    zebrad
        .generate_blocks(SEED_BLOCKS)
        .await
        .expect("seed the chain");

    // Transparent payouts need a policy that allows them: Miningcore pays t-addresses from the
    // UA (revealed recipients) and s-nomp pays them from transparent funds (fully transparent).
    let mut cfg = ZecdConfig::new(zebrad.rpc_port, pick_port().expect("pick zecd rpc port"));
    cfg.transparent = true;
    cfg.privacy_policy = Some("AllowFullyTransparent".to_string());
    cfg.restore_mnemonic = Some(POOL_MNEMONIC.to_string());
    cfg.birthday = Some(BIRTHDAY);
    let zecd = Zecd::start(&cfg).await.expect("start zecd");

    let first_pool_block = node_tip(&zebrad).await + 1;
    zebrad
        .generate_blocks(POOL_BLOCKS)
        .await
        .expect("mine the pool's blocks");
    let last_pool_block = first_pool_block + u64::from(POOL_BLOCKS) - 1;
    wait_ready(&zecd).await;
    zecd.wait_until_synced_to_node(&zebrad, SYNC_TIMEOUT)
        .await
        .expect("zecd sync");

    // 1. Miningcore's startup batch and s-nomp's startup checks.
    let v = zecd
        .call("validateaddress", json!([pool_taddr]))
        .await
        .expect("validateaddress");
    assert_eq!(v["isvalid"], json!(true), "{v}");
    let v = zecd
        .call("z_validateaddress", json!([pool_ua]))
        .await
        .expect("z_validateaddress");
    assert_eq!(v["isvalid"], json!(true), "{v}");
    let v = zecd
        .call("getaddressinfo", json!([pool_taddr]))
        .await
        .expect("getaddressinfo");
    assert_eq!(
        v["ismine"],
        json!(true),
        "the pool address is the wallet's: {v}"
    );
    // Miningcore probes submitblock with no params and only checks the method exists.
    let e = zecd
        .call("submitblock", json!([]))
        .await
        .expect_err("submitblock with no block is refused by the node");
    assert!(
        e.code().is_some_and(|c| c != -32601),
        "the node's own refusal, not method-not-found: {e}"
    );
    zecd.call("getblockchaininfo", json!([]))
        .await
        .expect("getblockchaininfo");
    let difficulty = zecd
        .call("getdifficulty", json!([]))
        .await
        .expect("getdifficulty");
    assert_eq!(
        difficulty,
        zebrad
            .rpc("getdifficulty", json!([]))
            .await
            .expect("node getdifficulty"),
        "getdifficulty is the node's answer"
    );
    let subsidy = zecd
        .call("getblocksubsidy", json!([]))
        .await
        .expect("getblocksubsidy");
    assert_eq!(
        subsidy,
        zebrad
            .rpc("getblocksubsidy", json!([]))
            .await
            .expect("node getblocksubsidy"),
        "getblocksubsidy is the node's answer"
    );
    let mining = zecd
        .call("getmininginfo", json!([]))
        .await
        .expect("getmininginfo");
    assert!(mining.is_object(), "{mining}");
    zecd.call("getnetworkinfo", json!([]))
        .await
        .expect("getnetworkinfo");
    zecd.call("getnetworkhashps", json!([]))
        .await
        .expect("getnetworkhashps");
    zecd.call("getbalance", json!([]))
        .await
        .expect("getbalance");
    // s-nomp asks for the account of each address and carries on when the call fails.
    assert!(zecd.call("getaccount", json!([pool_taddr])).await.is_err());

    // 2. Forwarded calls give the node's answers.
    let template_request =
        json!([{ "capabilities": ["coinbasetxn", "workid", "coinbase/append"] }]);
    let via_node = zebrad
        .rpc("getblocktemplate", template_request.clone())
        .await
        .expect("node getblocktemplate");
    let via_zecd = zecd
        .call("getblocktemplate", template_request)
        .await
        .expect("getblocktemplate through zecd");
    for field in ["height", "previousblockhash"] {
        assert_eq!(
            via_zecd[field], via_node[field],
            "getblocktemplate {field} matches the node"
        );
    }
    let tip_hash = zebrad
        .rpc("getbestblockhash", json!([]))
        .await
        .expect("node getbestblockhash");
    let block = zecd
        .call("getblock", json!([tip_hash, 1]))
        .await
        .expect("getblock through zecd");
    assert_eq!(
        block,
        zebrad
            .rpc("getblock", json!([tip_hash, 1]))
            .await
            .expect("node getblock"),
        "getblock is the node's answer"
    );
    // s-nomp's form, with the default verbosity.
    zecd.call("getblock", json!([tip_hash]))
        .await
        .expect("getblock with the default verbosity");
    let raw_tip = zebrad
        .rpc("getblock", json!([tip_hash, 0]))
        .await
        .expect("node raw block");
    let submitted = zecd
        .call("submitblock", json!([raw_tip]))
        .await
        .expect("submitblock through zecd");
    assert_eq!(submitted, json!("duplicate"), "the tip is already known");

    // 3. The coinbase ladder: immature until it can be spent, then generate.
    let first_coinbase = coinbase_txid_at(&zebrad, first_pool_block).await;
    assert_eq!(
        coinbase_category(&zecd, &first_coinbase).await,
        json!("immature")
    );
    let tip = node_tip(&zebrad).await;
    // Mature every pool block: a coinbase spends at COINBASE_MATURITY confirmations.
    let target = last_pool_block + u64::from(COINBASE_MATURITY) - 1;
    mine(&zebrad, &zecd, (target - tip) as u32).await;
    assert_eq!(
        coinbase_category(&zecd, &first_coinbase).await,
        json!("generate")
    );
    // s-nomp's listing: mature coinbase at the pool address, marked generated.
    let unspent = zecd
        .call("listunspent", json!([1, 99_999_999, [pool_taddr]]))
        .await
        .expect("listunspent");
    let entries = unspent.as_array().expect("listunspent array");
    assert!(
        entries.iter().any(|e| e["txid"] == json!(first_coinbase)),
        "the mature coinbase is listed: {unspent}"
    );
    assert!(
        entries.iter().all(|e| e["generated"] == json!(true)),
        "coinbase outputs are generated: {unspent}"
    );
    // z_getbalance on the t-address is what listunspent lists there.
    let listed: u64 = entries
        .iter()
        .map(|e| zats(e["amount"].as_f64().expect("amount")))
        .sum();
    let t_balance = zecd
        .call("z_getbalance", json!([pool_taddr, 1]))
        .await
        .expect("z_getbalance t-address");
    assert_eq!(zats(t_balance.as_f64().expect("number")), listed);

    // 4. Shield the coinbase into the UA, in the two-argument form both packages send.
    let shield = zecd
        .call("z_shieldcoinbase", json!([pool_taddr, pool_ua]))
        .await
        .expect("z_shieldcoinbase");
    let opid = shield["opid"].as_str().expect("opid").to_string();
    let statuses = zecd
        .call("z_getoperationstatus", Value::Null)
        .await
        .expect("z_getoperationstatus with null params (s-nomp)");
    assert!(
        statuses
            .as_array()
            .is_some_and(|a| a.iter().any(|s| s["id"] == json!(opid))),
        "{statuses}"
    );
    let shield_txid = operation_txid(&zecd, &opid).await;
    let raw = zebrad
        .rpc("getrawtransaction", json!([shield_txid, 1]))
        .await
        .expect("node getrawtransaction");
    assert_eq!(
        raw["vout"].as_array().map(Vec::len),
        Some(0),
        "a coinbase shielding has no transparent outputs: {raw}"
    );
    mine(&zebrad, &zecd, PAYOUT_MINCONF).await;

    // 5. The shielded balance, at Miningcore's minconf, is exactly the shielded notes, which
    //    are Ironwood notes on this NU6.3-active chain.
    let unspent = zecd
        .call("listunspent", json!([PAYOUT_MINCONF]))
        .await
        .expect("listunspent");
    let shielded: Vec<&Value> = unspent
        .as_array()
        .expect("array")
        .iter()
        .filter(|e| e["txid"] == json!(shield_txid))
        .collect();
    assert!(
        !shielded.is_empty(),
        "the shielding output is listed: {unspent}"
    );
    assert!(
        shielded.iter().all(|e| e["pool"] == "ironwood"),
        "the coinbase is shielded into Ironwood: {unspent}"
    );
    let shielded_total: u64 = shielded
        .iter()
        .map(|e| zats(e["amount"].as_f64().expect("amount")))
        .sum();
    let z_balance = zecd
        .call("z_getbalance", json!([pool_ua, PAYOUT_MINCONF]))
        .await
        .expect("z_getbalance UA");
    assert_eq!(
        zats(z_balance.as_f64().expect("number")),
        shielded_total,
        "z_getbalance on the UA is the shielded coinbase"
    );

    // 6. Miningcore's payout: 50 transparent recipients, four positional args, a fixed fee.
    let recipients: Vec<Value> = workers
        .iter()
        .map(|w| json!({ "address": w, "amount": PAYOUT_ZEC }))
        .collect();
    let opid = zecd
        .call(
            "z_sendmany",
            json!([pool_ua, recipients, PAYOUT_MINCONF, MININGCORE_FEE]),
        )
        .await
        .expect("Miningcore z_sendmany");
    let payout_txid = operation_txid(&zecd, opid.as_str().expect("opid")).await;
    mine(&zebrad, &zecd, 1).await;
    let payout = zecd
        .call("gettransaction", json!([payout_txid]))
        .await
        .expect("gettransaction of the payout");
    let fee = zats(payout["fee"].as_f64().expect("payout fee"));
    assert!(
        fee > zats(MININGCORE_FEE)
            && fee >= MINER_PAYOUTS as u64 * ZIP317_MARGINAL_FEE
            && fee.is_multiple_of(ZIP317_MARGINAL_FEE),
        "the ZIP 317 fee is paid, not the requested 0.0001: {fee} zatoshis"
    );
    let paid: BTreeSet<String> = payout["details"]
        .as_array()
        .expect("details")
        .iter()
        .filter(|d| {
            d["category"] == "send" && zats(d["amount"].as_f64().unwrap_or(0.0)) == zats(PAYOUT_ZEC)
        })
        .filter_map(|d| d["address"].as_str().map(str::to_string))
        .collect();
    let expected: BTreeSet<String> = workers.iter().cloned().collect();
    assert_eq!(paid, expected, "every miner is paid once: {payout}");
    let raw = zebrad
        .rpc("getrawtransaction", json!([payout_txid, 1]))
        .await
        .expect("node getrawtransaction");
    assert_eq!(
        raw["vout"].as_array().map(Vec::len),
        Some(MINER_PAYOUTS),
        "only the miners' outputs are transparent: {raw}"
    );
    mine(&zebrad, &zecd, 10).await;

    // 7. s-nomp's refill of its transparent payout address: two-argument z_sendmany.
    let t_payout = zecd
        .call("getnewaddress", json!(["", "transparent"]))
        .await
        .expect("getnewaddress transparent")
        .as_str()
        .expect("address")
        .to_string();
    let opid = zecd
        .call(
            "z_sendmany",
            json!([pool_ua, [{ "address": t_payout, "amount": 0.5 }]]),
        )
        .await
        .expect("s-nomp z_sendmany");
    operation_txid(&zecd, opid.as_str().expect("opid")).await;
    mine(&zebrad, &zecd, 10).await;

    // 8. s-nomp's payout from transparent funds. It must never spend a coinbase output
    //    directly: consensus requires that spend to be fully shielded.
    let mut amounts = serde_json::Map::new();
    for w in workers.iter().take(3) {
        amounts.insert(w.clone(), json!(PAYOUT_ZEC));
    }
    let txid = zecd
        .call("sendmany", json!(["", amounts]))
        .await
        .expect("s-nomp sendmany");
    let txid = txid.as_str().expect("sendmany txid").to_string();
    mine(&zebrad, &zecd, 1).await;
    let raw = zebrad
        .rpc("getrawtransaction", json!([txid, 1]))
        .await
        .expect("node getrawtransaction");
    for input in raw["vin"].as_array().into_iter().flatten() {
        let Some(prev) = input["txid"].as_str() else {
            continue;
        };
        let prev_tx = zebrad
            .rpc("getrawtransaction", json!([prev, 1]))
            .await
            .expect("node getrawtransaction of an input");
        assert!(
            prev_tx["vin"][0].get("coinbase").is_none(),
            "sendmany spent a coinbase output: {raw}"
        );
    }
    let sent = zecd
        .call("gettransaction", json!([txid]))
        .await
        .expect("gettransaction of the s-nomp payout");
    let paid: BTreeSet<&str> = sent["details"]
        .as_array()
        .expect("details")
        .iter()
        .filter(|d| d["category"] == "send")
        .filter_map(|d| d["address"].as_str())
        .collect();
    for w in workers.iter().take(3) {
        assert!(paid.contains(w.as_str()), "{w} is paid: {sent}");
    }
}
