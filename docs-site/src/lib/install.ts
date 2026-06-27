// Install command shown on the docs Installation page.
//
// The displayed form is the short, readable variant; the copy button writes the
// hardened form to the clipboard (protocol + TLS version pinned). Integrity does
// not depend on the fetch path — the installer verifies the SHA-256 checksum and
// cosign signature of the downloaded binary regardless of which command you use.
// Kept in sync with the landing site (website/src/lib/install.ts).

/** Short, readable command shown in the "Quick install" code block. */
export const INSTALL_DISPLAY = 'curl -LsSf https://keyholetui.com/install.sh | sh';

/**
 * The hardened command actually written to the clipboard by the copy button.
 * Same command as {@link INSTALL_DISPLAY}, with `--proto '=https'` and
 * `--tlsv1.2` pinned. The displayed/copied gap is intentional: readable on
 * screen, hardened on paste.
 */
export const INSTALL_COPY =
  "curl --proto '=https' --tlsv1.2 -LsSf https://keyholetui.com/install.sh | sh";
