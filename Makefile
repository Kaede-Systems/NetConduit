# NetConduit — Makefile
# Convenience targets for the full build pipeline.
# Run from the repo root.

FLUTTER     := /home/kaedepc/flutter/bin/flutter
DART        := /home/kaedepc/flutter/bin/dart
FRB         := $(HOME)/.cargo/bin/flutter_rust_bridge_codegen
CARGO       := cargo
MATURIN     := maturin
PLUGIN_DIR  := flutter/netconduit
RUST_DIR    := netconduit_core

.PHONY: all codegen build-rust-flutter build-python build-flutter test-python check clean

all: build-rust-flutter

# ── Generate Flutter FFI bindings (run after ANY api/mod.rs change) ───────────
codegen:
	@echo "── Running flutter_rust_bridge_codegen ──────────────────────────────"
	$(FRB) generate \
		--config-file flutter_rust_bridge_codegen.yaml
	@echo "── Regenerating Freezed files ───────────────────────────────────────"
	cd $(PLUGIN_DIR) && $(DART) run build_runner build --delete-conflicting-outputs

# ── Full Flutter release build (runs codegen first) ───────────────────────────
build-flutter: codegen build-flutter-linux

# ── Build Rust with flutter feature only (fast check) ─────────────────────────
build-rust-flutter:
	@echo "── Building Rust (flutter feature) ─────────────────────────────────"
	cd $(RUST_DIR) && $(CARGO) build --features flutter --release

# ── Rust check only (no link, fast) ──────────────────────────────────────────
check:
	cd $(RUST_DIR) && $(CARGO) check --features flutter

# ── Build Python extension (maturin) ──────────────────────────────────────────
build-python:
	@echo "── Building Python extension ─────────────────────────────────────────"
	cd $(RUST_DIR) && PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 $(MATURIN) develop --features python

# ── Build Flutter shared library (Linux desktop) ──────────────────────────────
build-flutter-linux:
	@echo "── Building Rust for Linux desktop ──────────────────────────────────"
	cd $(RUST_DIR) && $(CARGO) build --features flutter --no-default-features --release

# ── Build Flutter shared library (Android, requires cargo-ndk + NDK) ─────────
build-flutter-android:
	@echo "── Building Rust for Android ─────────────────────────────────────────"
	cd $(RUST_DIR) && $(CARGO) ndk \
		-t arm64-v8a -t armeabi-v7a -t x86_64 \
		build --features flutter --no-default-features --release

# ── Install Flutter deps ───────────────────────────────────────────────────────
flutter-deps:
	cd $(PLUGIN_DIR) && $(FLUTTER) pub get

# ── Python tests ──────────────────────────────────────────────────────────────
test-python: build-python
	@echo "── Running Python test suite ────────────────────────────────────────"
	source .venv/bin/activate && python -m pytest tests/ -v

# ── Clean ──────────────────────────────────────────────────────────────────────
clean:
	cd $(RUST_DIR) && $(CARGO) clean
	rm -f $(RUST_DIR)/src/frb_generated.rs
	rm -f $(PLUGIN_DIR)/lib/src/frb_generated.dart
	rm -f $(PLUGIN_DIR)/ios/Classes/netconduit.h
	rm -f $(PLUGIN_DIR)/macos/Classes/netconduit.h
