const { invoke } = window.__TAURI__.core;
const { Channel } = window.__TAURI__.core;
const { open } = window.__TAURI__.dialog;

let tabIdSeq = 0;

function ensureToastContainer() {
  let c = document.querySelector('.toast-container');
  if (!c) {
    c = document.createElement('div');
    c.className = 'toast-container';
    document.body.appendChild(c);
  }
  return c;
}

function showToast(message, kind = 'error', timeout = 3500) {
  const container = ensureToastContainer();
  const el = document.createElement('div');
  el.className = `toast toast-${kind}`;
  el.textContent = message;
  container.appendChild(el);
  // trigger animation
  requestAnimationFrame(() => el.classList.add('show'));
  setTimeout(() => {
    el.classList.remove('show');
    el.classList.add('hide');
    el.addEventListener('transitionend', () => el.remove(), { once: true });
  }, timeout);
}

function createTabElements(id) {
  const panel = document.createElement('div');
  panel.className = 'tab-panel';
  panel.dataset.tabId = id;
  panel.innerHTML = `
    <h2 class="title">Welcome to mtorrent!</h2>
    <form class="dl-form" autocomplete="off">
      <div class="uri-row">
        <input name="uri" placeholder="Enter magnet link or file path" />
        <button type="button" data-action="pick" title="Choose metainfo file">Choose...</button>
      </div>
      <div class="actions-row">
        <button class="start-btn" type="submit">Start download</button>
      </div>
    </form>
    <div class="log-wrapper"><textarea readonly></textarea></div>
  `;
  return panel;
}

function createTabButton(id) {
  const btn = document.createElement('button');
  btn.className = 'tab-btn';
  btn.dataset.tabId = id;
  btn.innerHTML = `<span class="label">Tab ${id}</span><span class="close" title="Close">×</span>`;
  return btn;
}

function setActiveTab(id) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tabId === id));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('hidden', p.dataset.tabId !== id));
}

async function startDownload(panel, tabBtn) {
  const input = panel.querySelector('input[name="uri"]');
  const textarea = panel.querySelector('textarea');
  const titleEl = panel.querySelector('.title');
  const form = panel.querySelector('.dl-form');

  const uri = input.value.trim();
  if (!uri) {
    showToast('Magnet link or .torrent file path is required');
    input.focus();
    return;
  }

  const outputDir = await open({ multiple: false, directory: true, title: 'Choose output folder' });
  if (!outputDir) return; // user cancelled

  const channel = new Channel();
  channel.onmessage = msg => {
    textarea.value = msg;
  };

  titleEl.textContent = 'Download in progress';
  tabBtn.querySelector('.label').textContent = uri.slice(0, 12) || `Tab ${panel.dataset.tabId}`;
  form.classList.add('disabled');
  form.querySelectorAll('input, button').forEach(el => el.disabled = true);

  try {
    await invoke('start_download', { metainfoUri: uri, outputDir, callback: channel });
  } catch (e) {
    channel.onmessage(`Failed to start: ${e}`);
  }
}

function addNewTab(autoActivate = true) {
  const id = (++tabIdSeq).toString();
  const tabsScroll = document.getElementById('tabs-scroll');
  const container = document.getElementById('tabs-container');

  const btn = createTabButton(id);
  const panel = createTabElements(id);
  container.appendChild(panel);
  tabsScroll.appendChild(btn);

  btn.addEventListener('click', (e) => {
    if ((e.target).classList.contains('close')) {
      // Stop download
      const uri = panel.querySelector('input[name="uri"]').value.trim();
      if (uri) {
        invoke('stop_download', { metainfoUri: uri }).catch(() => { /* ignore */ });
      }
      // Close tab
      panel.remove();
      btn.remove();
      // Activate last tab if current closed
      const last = [...document.querySelectorAll('.tab-btn')].pop();
      if (last) setActiveTab(last.dataset.tabId);
      return;
    }
    setActiveTab(id);
  });

  const form = panel.querySelector('form');
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    startDownload(panel, btn);
  });

  form.querySelector('button[data-action="pick"]').addEventListener('click', (e) => {
    e.preventDefault();

    open({ multiple: false, title: 'Select metainfo file', filters: [{ name: 'Torrent Files', extensions: ['torrent'] }] })
      .then(path => {
        if (path) form.querySelector('input[name="uri"]').value = path;
      });
  });

  if (autoActivate) setActiveTab(id);
  return { id, panel, btn };
}

window.addEventListener('DOMContentLoaded', () => {
  // document.documentElement.dataset.theme = 'dark';
  document.getElementById('add-tab').addEventListener('click', () => addNewTab(true));
  addNewTab(true); // initial tab
});
