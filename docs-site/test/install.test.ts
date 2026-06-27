import { describe, it, expect } from 'vitest';
import { INSTALL_DISPLAY, INSTALL_COPY } from '../src/lib/install';

describe('install command', () => {
  it('displays the short, readable form', () => {
    expect(INSTALL_DISPLAY).toBe('curl -LsSf https://keyholetui.com/install.sh | sh');
  });

  // Security-relevant: what lands on the clipboard must be the hardened command,
  // not the shorter displayed string. Pin it exactly.
  it('copies the hardened command, pinning protocol + TLS version', () => {
    expect(INSTALL_COPY).toBe(
      "curl --proto '=https' --tlsv1.2 -LsSf https://keyholetui.com/install.sh | sh",
    );
  });

  it('the displayed and copied commands differ but target the same installer', () => {
    expect(INSTALL_DISPLAY).not.toBe(INSTALL_COPY);
    for (const cmd of [INSTALL_DISPLAY, INSTALL_COPY]) {
      expect(cmd).toContain('https://keyholetui.com/install.sh');
      expect(cmd.endsWith('| sh')).toBe(true);
    }
  });
});
