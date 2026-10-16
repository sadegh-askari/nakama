import { gameRPC } from './api.mjs';
import { ObsoleteItemIds } from './obsolete-items.mjs';
import { setCurrencyLabel } from './currency-art.mjs';

let nextPicker = 0;
export function createResourcePicker(host, { walletOnly = false, signal } = {}) {
  const pickerId = ++nextPicker;
  host.innerHTML = '<p data-catalog-status role="status">Loading currencies from BankDef…</p><button type="button" data-retry-catalog hidden>Retry loading currencies</button><fieldset data-picker-fields disabled><div class="cardgame-resource-headers" aria-hidden="true"><span>#</span><span>Currency</span><span>Value</span><span></span></div><div data-items></div><button type="button" data-add>Add currency</button></fieldset>';
  const items = host.querySelector('[data-items]');
  const fields = host.querySelector('[data-picker-fields]');
  const add = host.querySelector('[data-add]');
  const status = host.querySelector('[data-catalog-status]');
  const retry = host.querySelector('[data-retry-catalog]');
  let catalog = null;
  let nextItem = 0;
  function changed() {
    [...items.children].forEach((row, position) => {
      const number = position + 1;
      row.querySelector('[data-position]').textContent = number;
      row.querySelector('[data-select]').setAttribute('aria-label', `Currency ${number}`);
      row.querySelector('[data-value]').setAttribute('aria-label', `Currency ${number} value`);
      row.querySelector('[data-remove]').setAttribute('aria-label', `Remove currency ${number}`);
      row.querySelector('[data-search]').setAttribute('aria-label', `Search currencies ${number}`);
      row.querySelector('[role=listbox]').setAttribute('aria-label', `Currencies ${number}`);
    });
    host.dispatchEvent(new Event('cardgame-picker-change', { bubbles: true })); }
  function addItem(values) {
    if (items.children.length >= 100) return;
    const row = document.createElement('div');
    row.className = 'cardgame-resource';
    const index = ++nextItem;
    const listId = `cardgame-currencies-${pickerId}-${index}`;
    row.innerHTML = `<span class="cardgame-resource-position" data-position aria-hidden="true"></span><div class="cardgame-currency-picker"><button type="button" data-select aria-label="Currency ${index}" aria-haspopup="listbox" aria-expanded="false" aria-controls="${listId}">Select currency</button><div data-menu hidden><input type="search" data-search aria-label="Search currencies ${index}" placeholder="Search currencies…" autocomplete="off"><div role="listbox" id="${listId}" aria-label="Currencies ${index}"></div></div></div><input data-value aria-label="Currency ${index} value" type="number" min="1" max="9007199254740991" step="1" required><button type="button" data-remove aria-label="Remove currency ${index}">Remove</button>`;
    const button = row.querySelector('[data-select]');
    const menu = row.querySelector('[data-menu]');
    const search = row.querySelector('[data-search]');
    const list = row.querySelector('[role=listbox]');
    row.dataset.id = values?.id || '';
    if (values?.id) setCurrencyLabel(button, values.id, values.id);
    if (values?.value) row.querySelector('[data-value]').value = values.value;
    function close() { menu.hidden = true; button.setAttribute('aria-expanded', 'false'); }
    function render() {
      list.replaceChildren();
      const query = search.value.trim().toLocaleLowerCase();
      for (const item of catalog?.values() || []) {
        if (!item.Name.toLocaleLowerCase().includes(query)) continue;
        const option = document.createElement('button');
        option.type = 'button'; option.role = 'option'; setCurrencyLabel(option, item.Id, item.Name);
        option.setAttribute('aria-selected', String(row.dataset.id === item.Id));
        option.addEventListener('click', () => { row.dataset.id = item.Id; setCurrencyLabel(button, item.Id, item.Name); button.removeAttribute('aria-invalid'); close(); button.focus(); changed(); });
        list.append(option);
      }
      if (!list.children.length) { const empty = document.createElement('p'); empty.textContent = 'No matching currencies'; list.append(empty); }
    }
    button.addEventListener('click', () => { if (!menu.hidden) { close(); return; } search.value = ''; render(); menu.hidden = false; button.setAttribute('aria-expanded', 'true'); search.focus(); });
    search.addEventListener('input', render);
    search.addEventListener('keydown', event => { if (event.key === 'ArrowDown') { event.preventDefault(); list.querySelector('button')?.focus(); } if (event.key === 'Enter') event.preventDefault(); });
    menu.addEventListener('keydown', event => { if (event.key === 'Escape') { event.preventDefault(); close(); button.focus(); } if (event.target.role === 'option' && ['ArrowDown', 'ArrowUp'].includes(event.key)) { event.preventDefault(); (event.key === 'ArrowDown' ? event.target.nextElementSibling : event.target.previousElementSibling)?.focus(); } });
    row.addEventListener('focusout', event => { if (!row.contains(event.relatedTarget)) close(); });
    row.querySelector('[data-value]').addEventListener('input', changed);
    row.querySelector('[data-remove]').addEventListener('click', () => { row.remove(); add.disabled = false; changed(); });
    items.append(row); add.disabled = items.children.length >= 100; changed();
  }
  async function load() {
    retry.hidden = true; fields.disabled = true;
    status.textContent = 'Loading currencies from BankDef…';
    try {
      const rows = await gameRPC('admin/bank_items', {}, { fetchFn: (url, options) => fetch(url, { ...options, signal }) });
      if (signal?.aborted || !host.isConnected) return;
      catalog = new Map(rows.filter(item => item.Kind === 'Currency' && !ObsoleteItemIds.has(item.Id) && (!walletOnly || item.CanGrant)).map(item => [item.Id, item]));
      if (!catalog.size) throw new Error('BankDef has no eligible currencies. Publish the definition first.');
      status.textContent = `${catalog.size} currencies available.`; fields.disabled = false;
      for (const row of items.children) {
        const selected = catalog.get(row.dataset.id);
        if (selected) setCurrencyLabel(row.querySelector('[data-select]'), selected.Id, selected.Name);
        else row.querySelector('[data-select]').textContent = 'Select currency';
        if (!selected && row.dataset.id) row.querySelector('[data-select]').setAttribute('aria-invalid', 'true');
      }
    } catch (error) {
      if (signal?.aborted) return;
      catalog = null; status.textContent = error.message; retry.hidden = false;
    }
    changed();
  }
  add.addEventListener('click', () => addItem()); retry.addEventListener('click', load); addItem(); load();
  return {
    valid() { return !!catalog && items.children.length > 0 && [...items.children].every(row => catalog.has(row.dataset.id) && row.querySelector('[data-value]').validity.valid); },
    read() {
      if (!catalog) throw new Error('Load the BankDef currency list before submitting.');
      return [...items.children].map(row => {
        if (!catalog.has(row.dataset.id)) { row.querySelector('[data-select]').setAttribute('aria-invalid', 'true'); throw new Error('Select a currency from BankDef.'); }
        return { id: row.dataset.id, value: row.querySelector('[data-value]').value };
      });
    },
    reset(values = []) { items.replaceChildren(); nextItem = 0; if (values.length) values.forEach(addItem); else addItem(); },
  };
}
