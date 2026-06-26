// Install commands shown on the landing page.
//
// The displayed form is the short, readable variant; the copy button writes the
// hardened form to the clipboard (protocol + TLS version pinned). Integrity does
// not depend on the fetch path — the installer verifies the SHA-256 checksum and
// cosign signature of the downloaded binary regardless of which command you use.

/** Short, readable command shown in the hero install box. */
export const INSTALL_DISPLAY = 'curl -LsSf https://keyholetui.com/install.sh | sh';

/**
 * The hardened command actually written to the clipboard by the copy button.
 * Same command as {@link INSTALL_DISPLAY}, with `--proto '=https'` and
 * `--tlsv1.2` pinned. The displayed/copied gap is intentional: readable on
 * screen, hardened on paste.
 */
export const INSTALL_COPY =
  "curl --proto '=https' --tlsv1.2 -LsSf https://keyholetui.com/install.sh | sh";

/**
 * The fully verifiable path: pin the GitHub release URL directly, then verify
 * the cosign signature / SLSA provenance documented in the README. Rendered as
 * a shell line-continuation in the "Trustworthy by default" section.
 */
export const INSTALL_GITHUB =
  "curl --proto '=https' --tlsv1.2 -LsSf \\\n  https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh | sh";
