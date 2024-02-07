use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::process::Command;

#[test]
fn test_download_multifile_torrentv2_from_50_seeders() {
    let metainfo_file = "tests/zeroed.torrent";
    let output_dir = "test_output_download_multifile_torrentv2_from_50_seeders";

    let seeder_ips = (50000u16..50050u16)
        .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
        .collect::<Vec<_>>();

    let mut seeders = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_seeders"));
        cmd.arg(metainfo_file);
        for ip in &seeder_ips {
            cmd.arg(ip.port().to_string());
        }
        cmd.spawn().expect("failed to execute 'seeders'")
    };

    let mut mtorrent = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"));
        cmd.arg(metainfo_file).arg(output_dir);
        for ip in &seeder_ips {
            cmd.arg(ip.to_string());
        }
        cmd.spawn().expect("failed to execute 'mtorrentv2'")
    };

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    let seeders_ecode = seeders.wait().expect("failed to wait on 'seeders'");
    assert!(seeders_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[test]
fn test_download_monofile_torrentv2_from_50_seeders() {
    let metainfo_file = "tests/zeroed_ubuntu.torrent";
    let output_dir = "test_output_download_download_monofile_torrentv2_from_50_seeders";

    let seeder_ips = (50050u16..50100u16)
        .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
        .collect::<Vec<_>>();

    let mut seeders = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_seeders"));
        cmd.arg(metainfo_file);
        for ip in &seeder_ips {
            cmd.arg(ip.port().to_string());
        }
        cmd.spawn().expect("failed to execute 'seeders'")
    };

    let mut mtorrent = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"));
        cmd.arg(metainfo_file).arg(output_dir);
        for ip in &seeder_ips {
            cmd.arg(ip.to_string());
        }
        cmd.spawn().expect("failed to execute 'mtorrentv2'")
    };

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    let seeders_ecode = seeders.wait().expect("failed to wait on 'seeders'");
    assert!(seeders_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}
