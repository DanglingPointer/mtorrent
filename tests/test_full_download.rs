use std::process::Command;

#[test]
fn test_download_torrent_from_two_seeders() {
    let metainfo_file = "tests/zeroed.torrent";

    let mut seeders = Command::new(env!("CARGO_BIN_EXE_seeders"))
        .arg(metainfo_file)
        .arg("12345")
        .arg("23456")
        .spawn()
        .expect("failed to execute 'seeders'");

    let mut mtorrent = Command::new(env!("CARGO_BIN_EXE_mtorrent"))
        .arg(metainfo_file)
        .spawn()
        .expect("failed to execute 'mtorrent'");

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
    assert!(mtorrent_ecode.success());

    let seeders_ecode = seeders.wait().expect("failed to wait on 'seeders'");
    assert!(seeders_ecode.success());
}

#[ignore = "takes too long"]
#[test]
fn test_download_torrent_from_one_seeder() {
    let metainfo_file = "tests/zeroed_ubuntu.torrent";

    let mut seeders = Command::new(env!("CARGO_BIN_EXE_seeders"))
        .arg(metainfo_file)
        .arg("12345")
        .spawn()
        .expect("failed to execute 'seeders'");

    let mut mtorrent = Command::new(env!("CARGO_BIN_EXE_mtorrent"))
        .arg(metainfo_file)
        .spawn()
        .expect("failed to execute 'mtorrent'");

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
    assert!(mtorrent_ecode.success());

    let seeders_ecode = seeders.wait().expect("failed to wait on 'seeders'");
    assert!(seeders_ecode.success());
}