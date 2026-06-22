# Homebrew formula for Keyhole, published to the AlexKasapis/homebrew-tap repo:
#
#   brew install AlexKasapis/tap/keyhole
#
# Linux uses the prebuilt glibc release tarballs; macOS has no prebuilt binary
# yet, so the formula builds from the source tag there (Linuxbrew parity).
#
# The version and the three sha256 values are committed with placeholders and
# filled in at release time by scripts/gen_packaging.sh — do not hand-edit the
# lines tagged `# @sha256:...`.
class Keyhole < Formula
  desc "Terminal UI to connect to brokers (Redis, AMQP), browse data, and record streams"
  homepage "https://github.com/AlexKasapis/Keyhole"
  version "0.1.0"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    # No prebuilt macOS binary is published; build from the source tag.
    url "https://github.com/AlexKasapis/Keyhole/archive/refs/tags/v0.1.0.tar.gz"
    sha256 "0000000000000000000000000000000000000000000000000000000000000000" # @sha256:src
    depends_on "cmake" => :build
    depends_on "rust" => :build
  end

  on_linux do
    on_intel do
      url "https://github.com/AlexKasapis/Keyhole/releases/download/v0.1.0/keyhole-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # @sha256:linux-x86_64
    end
    on_arm do
      url "https://github.com/AlexKasapis/Keyhole/releases/download/v0.1.0/keyhole-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # @sha256:linux-aarch64
    end
  end

  def install
    if OS.mac?
      # Build the binary, then generate the man page + completions from it.
      system "cargo", "install", *std_cargo_args
      generate_completions_from_executable(bin/"keyhole", "gen", "completions", base_name: "keyhole")
      system bin/"keyhole", "gen", "man", "--out", buildpath
      man1.install "keyhole.1"
    else
      # Prebuilt tarball: Homebrew has already cd'd into its single top-level dir.
      bin.install "keyhole"
      man1.install "keyhole.1"
      bash_completion.install "keyhole.bash" => "keyhole"
      zsh_completion.install "_keyhole"
      fish_completion.install "keyhole.fish"
    end
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/keyhole --version")
  end
end
