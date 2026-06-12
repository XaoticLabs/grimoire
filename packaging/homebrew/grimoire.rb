# Homebrew formula scaffold. To publish: create a tap repo
# (XaoticLabs/homebrew-grimoire), copy this file in as Formula/grimoire.rb,
# and fill in the sha256 from the release's SHA256SUMS. Users then:
#   brew install xaoticlabs/grimoire/grimoire
class Grimoire < Formula
  desc "cron + systemd for AI coding agents — bring your own CLI"
  homepage "https://github.com/XaoticLabs/grimoire"
  version "0.1.0"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "https://github.com/XaoticLabs/grimoire/releases/download/v#{version}/grimoire-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_RELEASE_SHA256"
    end
    on_intel do
      url "https://github.com/XaoticLabs/grimoire/releases/download/v#{version}/grimoire-v#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_RELEASE_SHA256"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/XaoticLabs/grimoire/releases/download/v#{version}/grimoire-v#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_RELEASE_SHA256"
    end
    on_intel do
      url "https://github.com/XaoticLabs/grimoire/releases/download/v#{version}/grimoire-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_RELEASE_SHA256"
    end
  end

  def install
    bin.install "grim", "grimw"
    generate_completions_from_executable(bin/"grim", "completions")
  end

  test do
    assert_match "grim", shell_output("#{bin}/grim --help")
  end
end
