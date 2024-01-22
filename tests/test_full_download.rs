#![cfg(debug_assertions)]

use std::process::Command;

#[test]
fn test_download_torrent_from_two_seeders() {
    let seeders_bin: &'static str = env!("CARGO_BIN_EXE_seeders");
    let mtorrent_bin: &'static str = env!("CARGO_BIN_EXE_mtorrent");

    let mut seeders = Command::new(seeders_bin)
        .arg("12345")
        .arg("23456")
        .spawn()
        .expect("failed to execute 'seeders'");

    let mut mtorrent = Command::new(mtorrent_bin).spawn().expect("failed to execute 'mtorrent'");

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
    assert!(mtorrent_ecode.success());

    let seeders_ecode = seeders.wait().expect("failed to wait on 'seeders'");
    assert!(seeders_ecode.success());
}
