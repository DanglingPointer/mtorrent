use futures_util::FutureExt;
use mtorrent::app;
use mtorrent::utils::listener::{StateListener, StateSnapshot};
use mtorrent_dht as dht;
use mtorrent_utils::{peer_id::PeerId, worker};
use std::ops::ControlFlow;
use std::time::Duration;
use tauri::Manager;

struct Listener {
    callback: tauri::ipc::Channel<String>,
}

impl StateListener for Listener {
    const INTERVAL: Duration = Duration::from_secs(1);

    fn on_snapshot(&mut self, snapshot: StateSnapshot<'_>) -> ControlFlow<()> {
        match self.callback.send(format!("{snapshot}")) {
            Ok(_) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }
}

#[tauri::command(async)]
fn launch_download(
    metainfo_uri: String,
    output_dir: String,
    callback: tauri::ipc::Channel<String>,
    state: tauri::State<'_, State>,
) {
    tokio::task::spawn_local(
        app::main::single_torrent(
            state.peer_id,
            metainfo_uri,
            output_dir,
            Some(state.dht_cmd_sender.clone()),
            Listener {
                callback: callback.clone(),
            },
            state.pwp_runtime_handle.clone(),
            state.storage_runtime_handle.clone(),
            false, /* use_upnp */
        )
        .map(move |ret| {
            callback
                .send(match ret {
                    Ok(()) => "Download completed successfully!".to_owned(),
                    Err(e) => format!("Download failed: {e}"),
                })
                .unwrap()
        }),
    );
}

struct State {
    peer_id: PeerId,
    pwp_runtime_handle: tokio::runtime::Handle,
    storage_runtime_handle: tokio::runtime::Handle,
    dht_cmd_sender: dht::CommandSink,
}

fn run_with_exit_code() -> i32 {
    let main_worker = worker::with_local_runtime(worker::rt::Config {
        name: "app".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    });
    tauri::async_runtime::set(main_worker.runtime_handle().clone());

    let storage_worker = worker::with_runtime(worker::rt::Config {
        name: "storage".to_owned(),
        io_enabled: false,
        time_enabled: false,
        ..Default::default()
    });

    let pwp_worker = worker::with_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    });

    let (_dht_worker, dht_cmds) = app::dht::launch_node_runtime(
        6881,
        None,
        std::env::current_dir().unwrap(),
        false, /* use_upnp */
    );

    let state = State {
        peer_id: PeerId::generate_new(),
        pwp_runtime_handle: pwp_worker.runtime_handle().clone(),
        storage_runtime_handle: storage_worker.runtime_handle().clone(),
        dht_cmd_sender: dht_cmds,
    };

    let app = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(state)
        .invoke_handler(tauri::generate_handler![launch_download])
        .build(tauri::generate_context!())
        .expect("error while running tauri application");

    app.run_return(move |app_handle, event| {
        if let tauri::RunEvent::ExitRequested { .. } = event {
            let state = app_handle.state::<State>();
            state.dht_cmd_sender.try_send(dht::Command::Shutdown).unwrap();
        }
    })
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let code = run_with_exit_code();
    std::process::exit(code)
}
