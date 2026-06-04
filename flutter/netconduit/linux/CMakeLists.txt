cmake_minimum_required(VERSION 3.18)

# The Flutter tooling requires that developers have a version of CMake installed
# that is above 3.10, which is a commonly available version. CMake 3.18 is
# required for `CMAKE_OSX_ARCHITECTURES`.
cmake_minimum_required(VERSION 3.18)

project(netconduit_library VERSION 0.0.1 LANGUAGES C)

add_library(netconduit SHARED
  "netconduit.c"
)

set_target_properties(netconduit PROPERTIES
  PUBLIC_HEADER netconduit.h
  OUTPUT_NAME "netconduit"
)

target_compile_definitions(netconduit PUBLIC DART_SHARED_LIB)

# ─── Rust / Cargo Integration ─────────────────────────────────────────────────
# Build the Rust library via Cargo.  The resulting .so is what Flutter loads.

if(CMAKE_BUILD_TYPE MATCHES "Release")
  set(CARGO_BUILD_TYPE "release")
  set(CARGO_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../netconduit_core/target/release")
else()
  set(CARGO_BUILD_TYPE "debug")
  set(CARGO_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../netconduit_core/target/debug")
endif()

# Determine the host triple
execute_process(
  COMMAND rustc -vV
  OUTPUT_VARIABLE RUSTC_VERSION_OUTPUT
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REGEX MATCH "host: ([^\n]+)" _ "${RUSTC_VERSION_OUTPUT}")
set(RUST_HOST "${CMAKE_MATCH_1}")

add_custom_command(
  OUTPUT "${CARGO_TARGET_DIR}/libnetconduit_core.so"
  COMMAND cargo build
    --manifest-path "${CMAKE_CURRENT_SOURCE_DIR}/../../../netconduit_core/Cargo.toml"
    --features flutter
    --no-default-features
    $<$<CONFIG:Release>:--release>
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../../../netconduit_core"
  COMMENT "Building Rust netconduit_core (${CARGO_BUILD_TYPE})"
)

add_custom_target(netconduit_rust
  DEPENDS "${CARGO_TARGET_DIR}/libnetconduit_core.so"
)

add_dependencies(netconduit netconduit_rust)

install(FILES "${CARGO_TARGET_DIR}/libnetconduit_core.so"
  DESTINATION "${CMAKE_INSTALL_PREFIX}"
  RENAME "libnetconduit_core.so"
)
