// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Warn)
        .with_module_level("mtorrent::app", log::LevelFilter::Info)
        .with_module_level("mtorrent_utils", log::LevelFilter::Debug)
        .init()
        .unwrap();
    mtorrent_gui_lib::run();
}
