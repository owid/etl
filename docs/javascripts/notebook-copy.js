/**
 * Add copy-to-clipboard buttons to notebook code cells
 * Integrates with Material for MkDocs copy functionality
 */
(function () {
  function addCopyButton(inputArea) {
    // Skip if button already exists
    if (inputArea.querySelector('.md-clipboard')) return;

    // Find the code element
    const codeElement = inputArea.querySelector('pre code, pre');
    if (!codeElement) return;

    // Create copy button matching Material for MkDocs structure
    const button = document.createElement('button');
    button.className = 'md-clipboard md-icon';
    button.title = 'Copy to clipboard';
    button.setAttribute('data-clipboard-target', '#' + codeElement.id);

    // Generate unique ID if not present
    if (!codeElement.id) {
      codeElement.id = `notebook-code-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      button.setAttribute('data-clipboard-target', '#' + codeElement.id);
    }

    // Add SVG icon (Material for MkDocs copy icon)
    button.innerHTML = `
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
        <path d="M19 21H8V7h11m0-2H8a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2m-3-4H4a2 2 0 0 0-2 2v14h2V3h12V1z"/>
      </svg>
    `;

    // Position the button
    inputArea.style.position = 'relative';
    button.style.position = 'absolute';
    button.style.top = '0.5rem';
    button.style.right = '0.5rem';
    button.style.padding = '0.25rem';
    button.style.cursor = 'pointer';
    button.style.border = 'none';
    button.style.background = 'transparent';
    button.style.opacity = '0.7';
    button.style.transition = 'opacity 0.2s';

    // Hover effect
    button.addEventListener('mouseenter', () => {
      button.style.opacity = '1';
    });
    button.addEventListener('mouseleave', () => {
      button.style.opacity = '0.7';
    });

    // Copy functionality
    button.addEventListener('click', async () => {
      const code = codeElement.textContent || '';

      try {
        await navigator.clipboard.writeText(code);

        // Show success feedback
        const originalHTML = button.innerHTML;
        button.innerHTML = `
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
            <path d="M21 7L9 19l-5.5-5.5 1.41-1.41L9 16.17 19.59 5.59 21 7z"/>
          </svg>
        `;
        button.style.color = 'green';

        setTimeout(() => {
          button.innerHTML = originalHTML;
          button.style.color = '';
        }, 2000);
      } catch (err) {
        console.error('Failed to copy:', err);
      }
    });

    inputArea.appendChild(button);
  }

  function init() {
    // Find all code input areas in notebooks
    document.querySelectorAll('.input_area, .jp-InputArea').forEach(addCopyButton);
  }

  // Initialize on page load
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Re-run after MkDocs instant navigation
  if (window.document$) {
    window.document$.subscribe(() => init());
  }
})();
