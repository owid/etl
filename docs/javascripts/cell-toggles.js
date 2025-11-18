(function () {
  function attach(cell) {
    // If cell has "hidden" tag, hide it completely
    if (cell.classList.contains('celltag_hidden')) {
      cell.style.display = 'none';
      return;
    }

    // Find the rendered input area - now supporting nbconvert basic template
    const input = cell.querySelector('.jp-InputArea, .nbinput, .nbsphinx-input, .highlight-ipynb, .input_area');
    if (!input || !input.parentNode) return;

    // Remove any old toggle before adding a new one (prevents duplicates on soft reload)
    const oldBtn = input.previousElementSibling;
    if (oldBtn && oldBtn.classList?.contains('owid-toggle-btn')) oldBtn.remove();

    // Create button
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'owid-toggle-btn';

    // Initial state: collapsed if the cell has "collapsed" tag
    const isCollapsed = cell.classList.contains('celltag_collapsed');
    if (isCollapsed) input.classList.add('owid-collapsed');

    // Accessibility: link button to the code block it controls
    const uid = input.id || `owid-code-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    input.id = uid;
    btn.setAttribute('aria-controls', uid);

    // Label + state
    btn.setAttribute('aria-expanded', String(!isCollapsed));
    btn.textContent = isCollapsed ? 'Show code' : 'Hide code';

    // Insert before the code block
    input.parentNode.insertBefore(btn, input);

    // Toggle visibility + label (always in sync with DOM state)
    btn.addEventListener('click', () => {
      const nowCollapsed = input.classList.toggle('owid-collapsed'); // true if hidden now
      btn.setAttribute('aria-expanded', String(!nowCollapsed));
      btn.textContent = nowCollapsed ? 'Show code' : 'Hide code';
    });
  }

  function init() {
    document.querySelectorAll('.jp-Cell.jp-CodeCell, .cell.code_cell').forEach(attach);
    // Remove gray tag toolbar that appears when tags exist
    document.querySelectorAll('.celltoolbar').forEach(el => { el.style.display = 'none'; });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Re-run after each MkDocs instant navigation (Material for MkDocs)
  if (window.document$) window.document$.subscribe(() => init());
})();
