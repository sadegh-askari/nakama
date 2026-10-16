let nextPopup = 0;
export function mountPopupTabs(dialog, nativePanel, panel, { nativeLabel, customLabel, submit, dispose, canSubmit = () => true }) {
  const prefix = `cardgame-popup-${++nextPopup}`;
  const tabs = document.createElement('div');
  tabs.className = 'cardgame-notification-tabs';
  tabs.setAttribute('role', 'tablist');
  tabs.setAttribute('aria-label', 'Admin popup tools');
  const originalAttributes = Object.fromEntries(['id', 'role', 'aria-labelledby'].map(key => [key, nativePanel.getAttribute(key)]));
  const tabButtons = [nativeLabel, customLabel].map((label, index) => {
    const button = document.createElement('button');
    button.type = 'button'; button.textContent = label; button.id = `${prefix}-tab-${index}`;
    button.setAttribute('role', 'tab'); button.setAttribute('aria-controls', `${prefix}-panel-${index}`);
    tabs.append(button); return button;
  });
  [nativePanel, panel].forEach((element, index) => {
    element.id = `${prefix}-panel-${index}`;
    element.setAttribute('role', 'tabpanel'); element.setAttribute('aria-labelledby', tabButtons[index].id);
  });
  nativePanel.classList.add('cardgame-native-panel');
  nativePanel.before(tabs); nativePanel.after(panel);
  const customFields = panel.querySelector('fieldset');
  const nativeControls = [...nativePanel.querySelectorAll('input, textarea, select, button')].map(control => ({ control, disabled: control.disabled }));
  const rootFields = dialog.querySelector('fieldset');
  const submitButton = dialog.querySelector('button[type=submit]');
  const originalSubmitDisabled = submitButton.disabled;
  submitButton.classList.add('cardgame-popup-submit');
  function updateSubmit() { submitButton.disabled = busy || (selected === 1 ? !canSubmit() : originalSubmitDisabled); }
  panel.addEventListener('cardgame-picker-change', updateSubmit);
  panel.addEventListener('change', updateSubmit);
  let selected = 0;
  let busy = false;
  function select(index) {
    if (busy) return;
    selected = index;
    customFields.disabled = index !== 1;
    nativeControls.forEach(({ control, disabled }) => { control.disabled = index === 1 || disabled; });
    nativePanel.hidden = index !== 0; panel.hidden = index !== 1;
    tabButtons.forEach((button, i) => { button.setAttribute('aria-selected', String(i === index)); button.tabIndex = i === index ? 0 : -1; });
    updateSubmit();
  }
  tabButtons.forEach((button, index) => {
    button.addEventListener('click', () => select(index));
    button.addEventListener('keydown', event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault(); const next = event.key === 'Home' ? 0 : event.key === 'End' ? 1 : 1 - index;
      select(next); tabButtons[next].focus();
    });
  });
  async function onSubmit(event) {
    if (selected !== 1) return;
    event.preventDefault(); event.stopImmediatePropagation();
    if (busy || !canSubmit()) return;
    busy = true; rootFields.disabled = true;
    try { await submit(); } finally { busy = false; rootFields.disabled = false; customFields.disabled = selected !== 1; updateSubmit(); }
  }
  dialog.addEventListener('submit', onSubmit, true);
  select(0);
  return {
    dialog,
    dispose() {
      submitButton.disabled = originalSubmitDisabled;
      submitButton.classList.remove('cardgame-popup-submit');
      dispose?.(); dialog.removeEventListener('submit', onSubmit, true);
      nativeControls.forEach(({ control, disabled }) => { control.disabled = disabled; }); nativePanel.hidden = false;
      nativePanel.classList.remove('cardgame-native-panel');
      for (const [key, value] of Object.entries(originalAttributes)) { if (value === null) nativePanel.removeAttribute(key); else nativePanel.setAttribute(key, value); }
      tabs.remove(); panel.remove();
    },
  };
}
